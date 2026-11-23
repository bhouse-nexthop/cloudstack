// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.cloud.kubernetes.cluster.migrations;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterDetailsVO;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterService;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterActionWorker;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;
import com.cloud.kubernetes.cluster.utils.KubernetesClusterUtil;
import com.cloud.network.dao.NetworkDao;
import com.cloud.network.dao.NetworkVO;
import com.cloud.utils.Pair;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.dao.UserVmDao;
import com.cloud.vm.dao.VMInstanceDetailsDao;

/**
 * Starts the kubelets of a cluster created without it with --cloud-provider=external, as new clusters are when
 * cloud.kubernetes.cluster.external.cloud.provider.enabled is true, so that the CloudStack Kubernetes provider
 * initialises the nodes. Each node is cordoned and its kubelet restarted; it is not drained, and its VM keeps running.
 */
public class ExternalCloudProviderMigration extends KubernetesClusterMigration {

    public static final String ID = "external-cloud-provider";
    protected static final String KUBECTL = KubernetesClusterRollingUpdateWorker.KUBECTL;
    protected static final String CLOUD_PROVIDER_FLAG = "--cloud-provider=external";
    protected static final String UNINITIALIZED_TAINT = "node.cloudprovider.kubernetes.io/uninitialized";
    /** How the user data of a node sets the kubelet arguments, without the flag */
    protected static final String KUBELET_ARGS = "KUBELET_EXTRA_ARGS=--cgroup-driver=systemd";
    protected static final String KUBELET_ARGS_LINE = "echo \"" + KUBELET_ARGS + "\" > /etc/default/kubelet";
    /** Records a node migrated whose user data is not changed: that of a first control node given a CNI configuration, multipart and encoded */
    protected static final String MIGRATED_VM_DETAIL = "cks.external.cloud.provider";
    protected static final String KUBELET_ARGS_WITH_FLAG_LINE = "echo \"" + KUBELET_ARGS + KubernetesClusterActionWorker.KUBELET_CLOUD_PROVIDER_ARGS + "\" > /etc/default/kubelet";
    protected static final String PROVIDER_COMMAND = KUBECTL + "-n kube-system get deployment cloud-controller-manager --ignore-not-found "
            + "-o jsonpath='image={.spec.template.spec.containers[*].image} replicas={.status.availableReplicas}.tolerations={.spec.template.spec.tolerations[*].key}.'";
    /** The release of the provider, the first that initialises a node as it should being v1.2.0 */
    private static final Pattern IMAGE = Pattern.compile("image=([^=]*) replicas=");
    private static final Pattern RELEASE = Pattern.compile(":v(\\d{1,4})\\.(\\d{1,3})\\.\\d+$");
    private static final Pattern REPLICAS = Pattern.compile("replicas=(\\d*)\\.tolerations=");
    /** The taint among the keys of the tolerations, which are separated by spaces and end with a dot */
    private static final Pattern TOLERATED = Pattern.compile("^(?:\\S+ )*" + Pattern.quote(UNINITIALIZED_TAINT) + "[ .]");
    protected static final String NODE_COMMAND = KUBECTL + "get node %s -o jsonpath='ready={.status.conditions[?(@.type==\"Ready\")].status}.provider={.spec.providerID}.taint={.spec.taints[?(@.key==\""
            + UNINITIALIZED_TAINT + "\")].effect}.'";
    private static final Pattern NODE = Pattern.compile("ready=(\\w*)\\.provider=(\\S*)\\.taint=([\\w ]*)\\.");
    private static final Pattern BASE64_PART = Pattern.compile("Content-Transfer-Encoding: base64\\r?\\n(?:[^\\r\\n]+\\r?\\n)*\\r?\\n([A-Za-z0-9+/=\\r\\n]+)");
    protected static final String PROVIDER_IDS_COMMAND = KUBECTL + "get nodes -o jsonpath='{range .items[*]}node={.metadata.name}={.spec.providerID}.{\"\\n\"}{end}'";
    private static final Pattern PROVIDER_ID = Pattern.compile("node=(\\S+?)=(\\S*)\\.");
    protected static final String TAINT_COMMAND = KUBECTL + "taint node %s " + UNINITIALIZED_TAINT + "=true:NoSchedule --overwrite";
    protected static final String UNTAINT_COMMAND = KUBECTL + "taint node %s " + UNINITIALIZED_TAINT + ":NoSchedule-";
    /**
     * Sets the flag as the user data of a new node does, unless the kubelet arguments are not those CloudStack set,
     * restarts the kubelet, and checks that it keeps running, with the flag. Puts the arguments back as they were,
     * and restarts the kubelet again, if it does not.
     */
    protected static final String KUBELET_SCRIPT = "set -e\n"
            + "changed=\n"
            + "ok=\n"
            + "trap 'if [ -z \"$ok\" ] && [ -n \"$changed\" ]; then mv -f /etc/default/kubelet.cks-backup /etc/default/kubelet; timeout 15 systemctl restart kubelet; fi' EXIT\n"
            // an install from the internet, rather than the ISO of the Kubernetes version, writes none
            + "if [ ! -f /etc/default/kubelet ]; then echo \"/etc/default/kubelet is missing\"; exit 1; fi\n"
            + "if ! grep -qF -- " + CLOUD_PROVIDER_FLAG + " /etc/default/kubelet; then\n"
            + "  if [ \"$(cat /etc/default/kubelet)\" != \"" + KUBELET_ARGS + "\" ]; then echo \"/etc/default/kubelet is not as CloudStack wrote it\"; exit 1; fi\n"
            + "  cp -f /etc/default/kubelet /etc/default/kubelet.cks-backup\n"
            + "  changed=1\n"
            + "  " + KUBELET_ARGS_WITH_FLAG_LINE + "\n"
            // written by an attempt cut off before it could check the kubelet
            + "elif [ -f /etc/default/kubelet.cks-backup ]; then\n"
            + "  changed=1\n"
            + "fi\n"
            + "timeout 15 systemctl restart kubelet\n"
            + "sleep 5\n"
            // a kubelet that does not start with the flag exits, and systemd starts it again under another PID
            + "pid=$(systemctl show -p MainPID --value kubelet)\n"
            + "for i in $(seq 5); do\n"
            + "  sleep 3\n"
            + "  if [ \"$pid\" = 0 ] || [ \"$(systemctl show -p MainPID --value kubelet)\" != \"$pid\" ] || ! systemctl is-active --quiet kubelet; then\n"
            + "    echo \"the kubelet does not keep running after its restart\"; exit 1\n"
            + "  fi\n"
            + "done\n"
            + "if ! tr '\\0' ' ' < /proc/$pid/cmdline | grep -qF -- " + CLOUD_PROVIDER_FLAG + "; then\n"
            + "  echo \"the kubelet does not run with " + CLOUD_PROVIDER_FLAG + "\"; exit 1\n"
            + "fi\n"
            + "ok=1\n"
            + "rm -f /etc/default/kubelet.cks-backup\n";
    protected static final String KUBELET_COMMAND = "sudo bash -c \"$(echo " + Base64.encodeBase64String(KUBELET_SCRIPT.getBytes(StandardCharsets.UTF_8)) + " | base64 -d)\" < /dev/null";

    @Inject
    protected UserVmDao userVmDao;
    @Inject
    protected NetworkDao networkDao;
    @Inject
    protected VMInstanceDetailsDao vmInstanceDetailsDao;

    public ExternalCloudProviderMigration(KubernetesClusterManagerImpl manager) {
        super(manager);
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public String getDescription() {
        return "Restarts the kubelet of each node with --cloud-provider=external, cordoning the node, so that the CloudStack Kubernetes provider " +
                "initialises it, and new nodes get the flag too. Needs " + KubernetesClusterService.KubernetesClusterExternalCloudProviderEnabled.key() +
                " in the zone, and the provider running on the cluster";
    }

    @Override
    public Scope getScope() {
        return Scope.NODE;
    }

    @Override
    public boolean isDisruptive() {
        return true;
    }

    @Override
    public boolean appliesToRunningNode() {
        return true;
    }

    protected boolean isEnabledOnCluster(KubernetesCluster cluster) {
        KubernetesClusterDetailsVO detail = manager.kubernetesClusterDetailsDao.findDetail(cluster.getId(), KubernetesClusterActionWorker.EXTERNAL_CLOUD_PROVIDER_DETAIL);
        return detail != null && Boolean.parseBoolean(detail.getValue());
    }

    protected boolean isEnabledInZone(long zoneId) {
        return Boolean.TRUE.equals(KubernetesClusterService.KubernetesClusterExternalCloudProviderEnabled.valueIn(zoneId));
    }

    /** Whether the cluster gets the provider: not on a shared network, as the provider is not deployed there */
    protected boolean isApplicable(KubernetesCluster cluster) {
        NetworkVO network = cluster.getNetworkId() == null ? null : networkDao.findById(cluster.getNetworkId());
        return network != null && !manager.isDirectAccess(network);
    }

    @Override
    public String checkPrecondition(KubernetesCluster cluster) {
        if (verify(cluster, null)) {
            return null;
        }
        if (!isEnabledOnCluster(cluster) && !isEnabledInZone(cluster.getZoneId())) {
            return String.format("%s is false in the zone of the cluster", KubernetesClusterService.KubernetesClusterExternalCloudProviderEnabled.key());
        }
        if (cluster.getNetworkId() == null || networkDao.findById(cluster.getNetworkId()) == null) {
            return "the network of the cluster is not found";
        }
        for (KubernetesClusterVmMapVO vmMap : manager.kubernetesClusterVmMapDao.listByClusterId(cluster.getId())) {
            if (vmMap.isExternalNode()) {
                // a rollout does not touch a VM it did not create
                return "the cluster has nodes added with addNodesToKubernetesCluster, which the migration does not give the flag; remove them first";
            }
            UserVmVO vm = KubernetesClusterUtil.getNodeType(vmMap) == KubernetesClusterNodeType.ETCD ? null : userVmDao.findById(vmMap.getVmId());
            if (vm != null && (isPendingOnNode(cluster, vm) || isMissingKubeletArgs(cluster, vm)) && !StringUtils.contains(getNodeConfig(vm), KUBELET_ARGS_LINE)) {
                return String.format("the user data of node %s does not set the kubelet arguments as CloudStack does", vm.getHostName());
            }
        }
        return null;
    }

    /**
     * The user data of a node, decoded, with the parts of a multipart one decoded too: CloudStack gives the first
     * control node of a cluster with a CNI configuration its configuration as a base64 encoded part
     */
    protected static String getNodeConfig(UserVmVO vm) {
        String userData = KubernetesClusterRollingUpdateWorker.decodeUserData(vm);
        if (userData == null || !isMultipart(userData)) {
            return userData;
        }
        StringBuilder config = new StringBuilder(userData);
        Matcher part = BASE64_PART.matcher(userData);
        while (part.find()) {
            config.append('\n').append(new String(Base64.decodeBase64(part.group(1)), StandardCharsets.UTF_8));
        }
        return config.toString();
    }

    protected static boolean isMultipart(String userData) {
        return userData != null && userData.contains("Content-Type: multipart/");
    }

    protected boolean isMigrated(UserVmVO vm, String config) {
        return config.contains(CLOUD_PROVIDER_FLAG) || vmInstanceDetailsDao.findDetail(vm.getId(), MIGRATED_VM_DETAIL) != null;
    }

    /** Whether the user data of the VM starts a kubelet, as that of an etcd node does not, without the flag */
    @Override
    public boolean isPendingOnNode(KubernetesCluster cluster, UserVmVO vm) {
        String config = getNodeConfig(vm);
        return config != null && config.contains(KUBELET_ARGS) && !isMigrated(vm, config) && isApplicable(cluster);
    }

    /** Whether a node that is not an etcd node has no user data CloudStack could have set the kubelet arguments with */
    protected boolean isMissingKubeletArgs(KubernetesCluster cluster, UserVmVO vm) {
        String config = getNodeConfig(vm);
        return (config == null || !config.contains(KUBELET_ARGS) && !isMigrated(vm, config)) && isApplicable(cluster);
    }

    /** Checks the provider runs, before any node is touched, unless the cluster is migrated already */
    @Override
    public void applyToCluster(KubernetesClusterRollingUpdateWorker worker) {
        if (verify(worker.getKubernetesCluster(), worker)) {
            return;
        }
        String output = worker.runKubectl(PROVIDER_COMMAND);
        if (output == null) {
            throw new CloudRuntimeException("unable to run kubectl on the cluster to tell whether the CloudStack Kubernetes provider is running");
        }
        Matcher matcher = REPLICAS.matcher(output);
        if (!matcher.find() || matcher.group(1).isEmpty() || Integer.parseInt(matcher.group(1)) == 0) {
            throw new CloudRuntimeException("the CloudStack Kubernetes provider (deployment cloud-controller-manager in kube-system) is not running on the cluster");
        }
        // or it could not be scheduled again on nodes that join, and that it has not initialised yet
        Matcher image = IMAGE.matcher(output);
        // the provider container, among any others of the pod
        List<String> images = image.find() ? List.of(image.group(1).trim().split(" +")) : List.of();
        String name = images.size() == 1 ? images.get(0) : images.stream().filter(i -> i.contains("cloudstack-kubernetes-provider")).findFirst().orElse(String.join(" ", images));
        Matcher release = RELEASE.matcher(name);
        if (!release.find() || Integer.parseInt(release.group(1)) * 1000 + Integer.parseInt(release.group(2)) < 1002) {
            throw new CloudRuntimeException(String.format("the CloudStack Kubernetes provider on the cluster runs image %s, not a release from v1.2.0, the first that initialises nodes as it should", name));
        }
        if (!TOLERATED.matcher(output.substring(matcher.end())).find()) {
            throw new CloudRuntimeException(String.format("the CloudStack Kubernetes provider on the cluster does not tolerate the %s taint, which its manifest does from v1.1.0",
                    UNINITIALIZED_TAINT));
        }
    }

    /** The readiness, provider ID and uninitialized taint of a node, or null if they can not be read */
    protected String[] getNodeState(KubernetesClusterRollingUpdateWorker worker, String name) {
        String output = worker.runKubectl(String.format(NODE_COMMAND, name));
        Matcher matcher = output == null ? null : NODE.matcher(output);
        return matcher != null && matcher.find() ? new String[] {matcher.group(1), matcher.group(2), matcher.group(3)} : null;
    }

    protected boolean isInitialised(String[] state) {
        return state != null && "True".equals(state[0]) && !state[1].isEmpty() && state[2].isEmpty();
    }

    /**
     * Restarts the kubelet with the flag, and has the provider initialise the node, which it only does for a node with
     * the uninitialized taint, as the kubelet only gives that to a node as it registers it. If the provider does not,
     * the taint is taken off again, so that the node can be used; its kubelet keeps the flag, which changes nothing
     * for a node the provider has not initialised.
     */
    @Override
    public void applyToRunningNode(KubernetesClusterRollingUpdateWorker worker, UserVmVO vm) {
        String name = KubernetesClusterActionWorker.getNodeName(vm);
        String userData = KubernetesClusterRollingUpdateWorker.decodeUserData(vm);
        boolean plain = userData != null && userData.contains(KUBELET_ARGS_LINE);
        if (!plain && !StringUtils.contains(getNodeConfig(vm), KUBELET_ARGS_LINE)) {
            throw new CloudRuntimeException(String.format("the user data of node %s does not set the kubelet arguments as CloudStack does", name));
        }
        // so that the node is not pending any more, and a recreate of it keeps the flag
        String migratedUserData = plain ? Base64.encodeBase64String(userData.replace(KUBELET_ARGS_LINE, KUBELET_ARGS_WITH_FLAG_LINE).getBytes(StandardCharsets.UTF_8)) : null;
        if (plain) {
            KubernetesClusterVmMapVO vmMap = manager.kubernetesClusterVmMapDao.findByVmId(vm.getId());
            worker.validateNodeUserDataLength(migratedUserData, vmMap == null ? KubernetesClusterNodeType.WORKER : KubernetesClusterUtil.getNodeType(vmMap));
        }
        Pair<Boolean, String> restarted = worker.runOnNode(vm, KUBELET_COMMAND);
        if (!Boolean.TRUE.equals(restarted.first())) {
            throw new CloudRuntimeException(String.format("unable to restart the kubelet of node %s with %s: %s", name, CLOUD_PROVIDER_FLAG,
                    StringUtils.abbreviate(StringUtils.trim(restarted.second()), 256)));
        }
        long deadline = System.currentTimeMillis() + worker.getNodeTimeout() * 1000;
        String[] state = getNodeState(worker, name);
        if (state == null) {
            throw new CloudRuntimeException(String.format("unable to read the state of node %s", name));
        }
        boolean tainted = false;
        if (state[1].isEmpty()) {
            if (worker.runKubectl(String.format(TAINT_COMMAND, name)) == null) {
                throw new CloudRuntimeException(String.format("unable to taint node %s for the CloudStack Kubernetes provider to initialise it", name));
            }
            tainted = true;
        }
        // read once more at the deadline, so that a node initialised since the last poll is not taken for one that is not
        boolean initialised = worker.pollUntil(deadline, () -> isInitialised(getNodeState(worker, name))) || isInitialised(getNodeState(worker, name));
        if (!initialised && tainted && worker.runKubectl(String.format(UNTAINT_COMMAND, name)) == null) {
            // the provider may have taken the taint off itself, as it initialised the node
            String[] after = getNodeState(worker, name);
            if (after == null || !after[2].isEmpty()) {
                throw new CloudRuntimeException(String.format("the CloudStack Kubernetes provider did not initialise node %s, and its %s taint could not be removed, " +
                        "so nothing is scheduled on it until it is", name, UNINITIALIZED_TAINT));
            }
            initialised = isInitialised(after);
        }
        if (!initialised) {
            throw new CloudRuntimeException(String.format("the CloudStack Kubernetes provider did not initialise node %s", name));
        }
        // once the provider has initialised a node, the nodes added from now on get the flag too
        if (!isEnabledOnCluster(worker.getKubernetesCluster())) {
            manager.kubernetesClusterDetailsDao.addDetail(worker.getKubernetesCluster().getId(), KubernetesClusterActionWorker.EXTERNAL_CLOUD_PROVIDER_DETAIL, "true", false);
        }
        if (plain) {
            vm.setUserData(migratedUserData);
            userVmDao.update(vm.getId(), vm);
        } else {
            vmInstanceDetailsDao.addDetail(vm.getId(), MIGRATED_VM_DETAIL, "true", false);
        }
    }

    @Override
    public boolean verify(KubernetesCluster cluster, KubernetesClusterRollingUpdateWorker worker) {
        if (!isApplicable(cluster)) {
            return cluster.getNetworkId() != null && networkDao.findById(cluster.getNetworkId()) != null;
        }
        if (!isEnabledOnCluster(cluster)) {
            return false;
        }
        String providerIds = worker == null ? null : worker.runKubectl(PROVIDER_IDS_COMMAND);
        if (worker != null && providerIds == null) {
            return false;
        }
        Map<String, String> byNode = new HashMap<>();
        if (providerIds != null) {
            Matcher matcher = PROVIDER_ID.matcher(providerIds);
            while (matcher.find()) {
                byNode.put(matcher.group(1), matcher.group(2));
            }
        }
        for (KubernetesClusterVmMapVO vmMap : manager.kubernetesClusterVmMapDao.listByClusterId(cluster.getId())) {
            UserVmVO vm = vmMap.isExternalNode() ? null : userVmDao.findById(vmMap.getVmId());
            if (vm == null) {
                continue;
            }
            boolean kubelet = KubernetesClusterUtil.getNodeType(vmMap) != KubernetesClusterNodeType.ETCD;
            if (isPendingOnNode(cluster, vm) || kubelet && isMissingKubeletArgs(cluster, vm)) {
                return false;
            }
            if (worker != null && kubelet && StringUtils.isEmpty(byNode.get(KubernetesClusterActionWorker.getNodeName(vm)))) {
                return false;
            }
        }
        return true;
    }
}
