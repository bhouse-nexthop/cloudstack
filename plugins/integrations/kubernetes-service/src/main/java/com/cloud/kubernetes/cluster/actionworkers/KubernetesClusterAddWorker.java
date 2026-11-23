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
package com.cloud.kubernetes.cluster.actionworkers;

import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.WORKER;

import com.cloud.event.ActionEventUtils;
import com.cloud.event.EventVO;
import com.cloud.exception.InsufficientCapacityException;
import com.cloud.exception.ManagementServerException;
import com.cloud.exception.NetworkRuleConflictException;
import com.cloud.exception.ResourceUnavailableException;
import com.cloud.hypervisor.Hypervisor;
import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterEventTypes;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterService;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.utils.KubernetesClusterUtil;
import com.cloud.network.IpAddress;
import com.cloud.network.Network;
import com.cloud.network.dao.FirewallRulesDao;
import com.cloud.network.rules.FirewallRule;
import com.cloud.network.rules.FirewallRuleVO;
import com.cloud.network.rules.PortForwardingRuleVO;
import com.cloud.service.ServiceOfferingVO;
import com.cloud.user.Account;
import com.cloud.uservm.UserVm;
import com.cloud.utils.Pair;
import com.cloud.utils.Ternary;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.net.NetUtils;
import com.cloud.utils.ssh.SshHelper;
import com.cloud.vm.UserVmVO;
import org.apache.cloudstack.api.ApiCommandResourceType;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.command.user.vm.RebootVMCmd;
import org.apache.cloudstack.context.CallContext;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.Level;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KubernetesClusterAddWorker extends KubernetesClusterActionWorker {

    @Inject
    private FirewallRulesDao firewallRulesDao;
    private long addNodeTimeoutTime;

    List<Long> finalNodeIds = new ArrayList<>();

    public KubernetesClusterAddWorker(KubernetesCluster kubernetesCluster, KubernetesClusterManagerImpl clusterManager) {
        super(kubernetesCluster, clusterManager);
    }

    public boolean addNodesToCluster(List<Long> nodeIds, boolean mountCksIsoOnVr, boolean manualUpgrade) throws CloudRuntimeException {
        try {
            init();
            addNodeTimeoutTime = System.currentTimeMillis() + KubernetesClusterService.KubernetesClusterAddNodeTimeout.value() * 1000;
            Long networkId = kubernetesCluster.getNetworkId();
            Network network = networkDao.findById(networkId);
            if (Objects.isNull(network)) {
                throw new CloudRuntimeException(String.format("Failed to find network with id: %s", networkId));
            }
            templateDao.findById(kubernetesCluster.getTemplateId());
            IpAddress publicIp = null;
            try {
                publicIp = getPublicIp(network);
            } catch (ManagementServerException e) {
                throw new CloudRuntimeException(String.format("Failed to retrieve public IP for the network: %s ", network.getName()));
            }
            // take the cluster before attaching the ISO, so that an add which is refused leaves nothing behind
            transitToOperationState(KubernetesCluster.Event.AddNodeRequested);
            createJoinToken(KubernetesClusterService.KubernetesClusterAddNodeTimeout.value());
            // rendered once the join token exists, as it is part of the user data
            String base64UserData = getNodeUserData(getControlVmPrivateIp(), mountCksIsoOnVr);
            removeLeftoverNodeSshRules(network, publicIp, nodeIds);
            if (Objects.isNull(getFreeNodeSshPort(network, publicIp))) {
                throw new CloudRuntimeException(String.format("No free port left on public IP %s for SSH to new nodes of Kubernetes cluster %s",
                        publicIp.getAddress().addr(), kubernetesCluster.getName()));
            }
            attachCksIsoForNodesAdditionToCluster(nodeIds, kubernetesCluster.getId(), mountCksIsoOnVr);
            Ternary<Integer, Long, Long> nodesAddedAndMemory = importNodeToCluster(nodeIds, network, publicIp, base64UserData);
            int nodesAdded = nodesAddedAndMemory.first();
            updateKubernetesCluster(kubernetesCluster.getId(), nodesAddedAndMemory, manualUpgrade);
            if (nodeIds.size() != nodesAdded) {
                String msg = String.format("Not every node was added to the CKS cluster %s, nodes added: %s out of %s", kubernetesCluster.getUuid(), nodesAdded, nodeIds.size());
                logger.info(msg);
                detachCksIsoFromNodesAddedToCluster(nodeIds, kubernetesCluster.getId(), mountCksIsoOnVr);
                stateTransitTo(kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed);
                deleteUnusedJoinToken();
                ActionEventUtils.onCompletedActionEvent(CallContext.current().getCallingUserId(), CallContext.current().getCallingAccountId(),
                        EventVO.LEVEL_ERROR, KubernetesClusterEventTypes.EVENT_KUBERNETES_CLUSTER_NODES_ADD,
                        msg, kubernetesCluster.getId(), ApiCommandResourceType.KubernetesCluster.toString(), 0);
                return false;
            }
            Pair<String, Integer> publicIpSshPort = getKubernetesClusterServerIpSshPort(null);
            KubernetesClusterUtil.validateKubernetesClusterReadyNodesCount(kubernetesCluster, publicIpSshPort.first(), publicIpSshPort.second(),
                    getControlNodeLoginUser(), sshKeyFile, addNodeTimeoutTime, 15000);
            deleteJoinToken();
            detachCksIsoFromNodesAddedToCluster(nodeIds, kubernetesCluster.getId(), mountCksIsoOnVr);
            stateTransitTo(kubernetesCluster.getId(), KubernetesCluster.Event.OperationSucceeded);
            String description = String.format("Successfully added %s nodes to Kubernetes Cluster %s", nodesAdded, kubernetesCluster.getUuid());
            ActionEventUtils.onCompletedActionEvent(CallContext.current().getCallingUserId(), CallContext.current().getCallingAccountId(),
                    EventVO.LEVEL_INFO, KubernetesClusterEventTypes.EVENT_KUBERNETES_CLUSTER_NODES_ADD,
                    description, kubernetesCluster.getId(), ApiCommandResourceType.KubernetesCluster.toString(), 0);
            return true;
        } catch (Exception e) {
            // a failure before this operation took the cluster must not end the operation that holds it, and after,
            // the cluster is only moved while this operation still holds it, see stateTransitTo
            if (isOperationStarted()) {
                stateTransitTo(kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed);
            }
            deleteUnusedJoinToken();
            if (e instanceof CloudRuntimeException) {
                throw (CloudRuntimeException) e;
            }
            throw new CloudRuntimeException(e);
        }
    }

    /** A node rebooted with join userdata may still be joining; otherwise the token is no longer needed */
    private void deleteUnusedJoinToken() {
        if (finalNodeIds.isEmpty()) {
            deleteJoinToken();
        }
    }

    private void detachCksIsoFromNodesAddedToCluster(List<Long> nodeIds, long kubernetesClusterId, boolean mountCksIsoOnVr) {
        if (mountCksIsoOnVr) {
            detachIsoOnVirtualRouter(kubernetesClusterId);
        } else {
            logger.info("Detaching CKS ISO from the nodes");
            List<UserVm> vms = nodeIds.stream().map(nodeId -> userVmDao.findById(nodeId)).collect(Collectors.toList());
            detachIsoKubernetesVMs(vms);
        }
    }

    public void detachIsoOnVirtualRouter(Long kubernetesClusterId) {
        KubernetesClusterVO kubernetesCluster = kubernetesClusterDao.findById(kubernetesClusterId);
        Long virtualRouterId = getVirtualRouterNicOnKubernetesClusterNetwork(kubernetesCluster).getInstanceId();
        long isoId = kubernetesSupportedVersionDao.findById(kubernetesCluster.getKubernetesVersionId()).getIsoId();

        try {
            networkService.handleCksIsoOnNetworkVirtualRouter(virtualRouterId, false);
        } catch (ResourceUnavailableException e) {
            String err = String.format("Error trying to handle ISO %s on virtual router %s", isoId, virtualRouterId);
            logger.error(err);
            throw new CloudRuntimeException(err);
        }

        try {
            templateService.detachIso(virtualRouterId, isoId, true, true);
        } catch (CloudRuntimeException e) {
            String err = String.format("Error trying to detach ISO %s from virtual router %s", isoId, virtualRouterId);
            logger.error(err, e);
        }
    }

    public void attachCksIsoForNodesAdditionToCluster(List<Long> nodeIds, Long kubernetesClusterId, boolean mountCksIsoOnVr) {
        if (mountCksIsoOnVr) {
            attachAndServeIsoOnVirtualRouter(kubernetesClusterId);
        } else {
            logger.info("Attaching CKS ISO to the nodes");
            List<UserVm> vms = nodeIds.stream().map(nodeId -> userVmDao.findById(nodeId)).collect(Collectors.toList());
            attachIsoKubernetesVMs(vms);
        }
    }

    public void attachAndServeIsoOnVirtualRouter(Long kubernetesClusterId) {
        KubernetesClusterVO kubernetesCluster = kubernetesClusterDao.findById(kubernetesClusterId);
        Long virtualRouterId = getVirtualRouterNicOnKubernetesClusterNetwork(kubernetesCluster).getInstanceId();
        long isoId = kubernetesSupportedVersionDao.findById(kubernetesCluster.getKubernetesVersionId()).getIsoId();

        try {
            templateService.attachIso(isoId, virtualRouterId, true, true);
        } catch (CloudRuntimeException e) {
            String err = String.format("Error trying to attach ISO %s to virtual router %s", isoId, virtualRouterId);
            logger.error(err);
            throw new CloudRuntimeException(err);
        }

        try {
            networkService.handleCksIsoOnNetworkVirtualRouter(virtualRouterId, true);
        } catch (ResourceUnavailableException e) {
            String err = String.format("Error trying to handle ISO %s on virtual router %s", isoId, virtualRouterId);
            logger.error(err);
            throw new CloudRuntimeException(err);
        }
    }

    private String getNodeUserData(String controlNodeGuestIp, boolean mountCksIsoOnVr) {
        String k8sNodeConfig = null;
        try {
            k8sNodeConfig = getKubernetesNodeConfig(controlNodeGuestIp, Hypervisor.HypervisorType.VMware.equals(clusterTemplate.getHypervisorType()), mountCksIsoOnVr);
        } catch (IOException e) {
            logAndThrow(Level.ERROR, "Failed to read Kubernetes node configuration file", e);
        }
        if (Objects.isNull(k8sNodeConfig)) {
            logAndThrow(Level.ERROR, "Error generating worker node configuration");
        }
        String base64UserData = Base64.encodeBase64String(k8sNodeConfig.getBytes(com.cloud.utils.StringUtils.getPreferredCharset()));
        validateNodeUserDataLength(base64UserData, WORKER);
        return base64UserData;
    }

    private Ternary<Integer, Long, Long> importNodeToCluster(List<Long> nodeIds, Network network, IpAddress publicIp, String base64UserData) {
        int nodeIndex = 0;
        Long additionalMemory = 0L;
        Long additionalCores = 0L;
        for (Long nodeId : nodeIds) {
            UserVmVO vm = userVmDao.findById(nodeId);
            Pair<Boolean, Integer> result = validateAndSetupNode(network, publicIp, owner, nodeId,  nodeIndex, base64UserData);
            if (Boolean.TRUE.equals(result.first())) {
                ServiceOfferingVO offeringVO = serviceOfferingDao.findById(vm.getId(), vm.getServiceOfferingId());
                additionalMemory += offeringVO.getRamSize();
                additionalCores += offeringVO.getCpu();
                String msg = String.format("VM %s added as a node on the Kubernetes Cluster %s", vm.getUuid(), kubernetesCluster.getUuid());
                ActionEventUtils.onCompletedActionEvent(CallContext.current().getCallingUserId(), CallContext.current().getCallingAccountId(),
                        EventVO.LEVEL_INFO, KubernetesClusterEventTypes.EVENT_KUBERNETES_CLUSTER_NODES_ADD,
                        msg, vm.getId(), ApiCommandResourceType.VirtualMachine.toString(), 0);
            }
            if (Boolean.FALSE.equals(result.first())) {
                logger.error(String.format("Failed to add node %s [%s] to Kubernetes cluster : %s", vm.getName(), vm.getUuid(), kubernetesCluster.getName()));
            }
            if (System.currentTimeMillis() > addNodeTimeoutTime) {
                logger.error(String.format("Failed to add node %s to Kubernetes cluster : %s", nodeId, kubernetesCluster.getName()));
            }
            nodeIndex = result.second();
        }
        return new Ternary<>(nodeIndex, additionalMemory, additionalCores);
    }

    protected Pair<Boolean, Integer> validateAndSetupNode(Network network, IpAddress publicIp, Account account,
                                   Long nodeId, int nodeIndex, String base64UserData) {
        Integer sshStartPort = getFreeNodeSshPort(network, publicIp);
        if (Objects.isNull(sshStartPort)) {
            logger.error(String.format("Failed to add node %s to Kubernetes cluster %s: no free port left on public IP %s for SSH to it",
                    nodeId, kubernetesCluster.getName(), publicIp.getAddress().addr()));
            return new Pair<>(false, nodeIndex);
        }
        Set<Long> preExistingRuleIds = listNodeSshRuleIds(network, publicIp, nodeId, sshStartPort);
        try {
            if (Objects.isNull(network.getVpcId())) {
                provisionFirewallRules(publicIp, owner, sshStartPort, sshStartPort);
            }
            provisionPublicIpPortForwardingRule(publicIp, network, account, nodeId, sshStartPort, DEFAULT_SSH_PORT);
            boolean isCompatible = validateNodeCompatibility(publicIp, nodeId, sshStartPort);
            if (!isCompatible) {
                revertNetworkRules(network, publicIp, nodeId, sshStartPort, preExistingRuleIds);
                return new Pair<>(false, nodeIndex);
            }

            userVmManager.updateVirtualMachine(nodeId, null, null, null, null,
                    null, null, base64UserData, null, null, null,
                    BaseCmd.HTTPMethod.POST, null, null, null, null, null);
            resetNodeProvisioningState(userVmDao.findById(nodeId), publicIp, sshKeyFile, sshStartPort);

            RebootVMCmd rebootVMCmd = new RebootVMCmd();
            Field idField = rebootVMCmd.getClass().getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(rebootVMCmd, nodeId);
            userVmService.rebootVirtualMachine(rebootVMCmd);
            finalNodeIds.add(nodeId);
        } catch (ResourceUnavailableException | NetworkRuleConflictException | NoSuchFieldException |
                 InsufficientCapacityException | IllegalAccessException e) {
            logger.error(String.format("Failed to activate API port forwarding rules for the Kubernetes cluster : %s", kubernetesCluster.getName()));
            // remove added Firewall and PF rules
            revertNetworkRules(network, publicIp, nodeId, sshStartPort, preExistingRuleIds);
            return new Pair<>( false, nodeIndex);
        } catch (Exception e) {
            String errMsg = String.format("Unexpected exception while trying to add the external node %s to the Kubernetes cluster %s: %s",
                    nodeId, kubernetesCluster.getName(), e.getMessage());
            logger.error(errMsg, e);
            revertNetworkRules(network, publicIp, nodeId, sshStartPort, preExistingRuleIds);
            throw new CloudRuntimeException(e);
        }
        return new Pair<>(true, ++nodeIndex);
    }

    private void updateKubernetesCluster(long clusterId, Ternary<Integer, Long, Long> additionalNodesDetails, boolean manualUpgrade) {
        int additionalNodeCount = additionalNodesDetails.first();
        KubernetesClusterVO kubernetesClusterVO = kubernetesClusterDao.findById(clusterId);
        kubernetesClusterVO.setNodeCount(kubernetesClusterVO.getNodeCount() + additionalNodeCount);
        kubernetesClusterVO.setMemory(kubernetesClusterVO.getMemory() + additionalNodesDetails.second());
        kubernetesClusterVO.setCores(kubernetesClusterVO.getCores() + additionalNodesDetails.third());
        kubernetesClusterDao.update(clusterId, kubernetesClusterVO);
        kubernetesCluster = kubernetesClusterVO;

        finalNodeIds.forEach(id -> addKubernetesClusterVm(clusterId, id, false, true, false, manualUpgrade));
    }


    private boolean validateNodeCompatibility(IpAddress publicIp, long nodeId, int nodeSshPort) throws CloudRuntimeException {
        File pkFile = getManagementServerSshPublicKeyFile();
        try {
            File validateNodeScriptFile = retrieveScriptFile(validateNodeScript);
            Thread.sleep(15*1000);
            copyScriptFile(publicIp.getAddress().addr(), nodeSshPort, validateNodeScriptFile, validateNodeScript);
            String command = String.format("%s%s", scriptPath, validateNodeScript);
            Pair<Boolean, String> result = SshHelper.sshExecute(publicIp.getAddress().addr(), nodeSshPort, getControlNodeLoginUser(),
                    pkFile, null, command, 10000, 10000, 10 * 60 * 1000);
            if (Boolean.FALSE.equals(result.first())) {
                logger.error(String.format("Node with ID: %s cannot be added as a worker node as it does not have " +
                        "the following dependencies: %s ", nodeId, result.second()));
                return false;
            }
        } catch (Exception e) {
            logger.error(String.format("Failed to validate node with ID: %s", nodeId), e);
            return false;
        }
        return true;
    }

    /**
     * Clears the previous user data run, and any earlier cluster join: a VM removed from a cluster keeps
     * /home/cloud/success, which makes the join scripts exit early, and kubeadm state if it wasn't reset.
     */
    protected String getNodeProvisioningResetCommand(UserVm userVm) {
        return String.format("sudo rm -rf /var/lib/cloud/instances/%s/sem/* && sudo rm -f /home/cloud/success && " +
                "{ sudo systemctl stop deploy-kube-system 2>/dev/null; " +
                "sudo umount /mnt/k8sdisk 2>/dev/null; mountpoint -q /mnt/k8sdisk || sudo rm -rf /mnt/k8sdisk; sudo rm -rf /aux-dwnld; " +
                "sudo systemctl disable --now kubelet 2>/dev/null; " +
                "if PATH=$PATH:/opt/bin command -v kubeadm >/dev/null; then " +
                "sudo env PATH=$PATH:/opt/bin kubeadm reset -f; fi; }", userVm.getUuid());
    }

    protected void resetNodeProvisioningState(UserVm userVm, IpAddress publicIp, File pkFile, int nodeSshPort) {
        try {
            Pair<Boolean, String> result = SshHelper.sshExecute(publicIp.getAddress().addr(), nodeSshPort, getControlNodeLoginUser(),
                    pkFile, null, getNodeProvisioningResetCommand(userVm), 10000, 10000, 10 * 60 * 1000);
            if (Boolean.FALSE.equals(result.first())) {
                logger.error(String.format("Failed to reset previous provisioning on node: %s; this may prevent it from joining the cluster: %s",
                        userVm.getName(), result.second()));
            }
        } catch (Exception e) {
            logger.error(String.format("Failed to reset previous provisioning on node: %s; this may prevent it from joining the cluster", userVm.getName()), e);
        }
    }

    /**
     * Removes SSH rules left by an earlier failed add-node: a port-forwarding rule of the shape CKS creates (tcp, open
     * to all, one port from CLUSTER_NODES_DEFAULT_START_SSH_PORT up on the cluster IP, to port 22) to a VM in this
     * request that is in no cluster. On isolated networks the firewall rule on that port goes too, if it is exactly
     * tcp 0.0.0.0/0 with no destination CIDRs and no other rule on the IP forwards that port. Other rules are left
     * alone; the port picker skips their ports.
     */
    protected void removeLeftoverNodeSshRules(Network network, IpAddress publicIp, List<Long> nodeIds) {
        List<FirewallRuleVO> leftovers = new ArrayList<>();
        for (Long nodeId : nodeIds) {
            if (isInAnyCluster(nodeId)) {
                continue;
            }
            portForwardingRulesDao.listByVm(nodeId).stream()
                    .filter(pfRule -> Objects.equals(pfRule.getSourceIpAddressId(), publicIp.getId())
                            && hasNodeSshPfShape(pfRule) && pfRule.getSourcePortStart() >= CLUSTER_NODES_DEFAULT_START_SSH_PORT
                            && isOpenToAll(pfRule))
                    .forEach(leftovers::add);
        }
        if (leftovers.isEmpty()) {
            return;
        }
        List<FirewallRuleVO> leftoverFirewallRules = new ArrayList<>();
        if (Objects.isNull(network.getVpcId())) {
            Set<Long> leftoverPfIds = leftovers.stream().map(FirewallRuleVO::getId).collect(Collectors.toSet());
            Set<Integer> ports = leftovers.stream().map(FirewallRuleVO::getSourcePortStart).collect(Collectors.toSet());
            List<FirewallRuleVO> otherForwards = firewallRulesDao.listByIp(publicIp.getId()).stream()
                    .filter(rule -> rule.getPurpose() != FirewallRule.Purpose.Firewall && !leftoverPfIds.contains(rule.getId())
                            && Objects.nonNull(rule.getSourcePortStart()) && Objects.nonNull(rule.getSourcePortEnd()))
                    .collect(Collectors.toList());
            for (int port : ports) {
                boolean portShared = otherForwards.stream().anyMatch(rule -> rule.getSourcePortStart() <= port && port <= rule.getSourcePortEnd());
                if (portShared) {
                    continue;
                }
                firewallRulesDao.listByIpPurposePortsProtocolAndNotRevoked(publicIp.getId(), port, port, NetUtils.TCP_PROTO, FirewallRule.Purpose.Firewall)
                        .stream().filter(rule -> {
                            firewallRulesDao.loadSourceCidrs(rule);
                            firewallRulesDao.loadDestinationCidrs(rule);
                            return List.of(NetUtils.ALL_IP4_CIDRS).equals(rule.getSourceCidrList())
                                    && CollectionUtils.isEmpty(rule.getDestinationCidrList());
                        }).forEach(leftoverFirewallRules::add);
            }
        }
        String ip = publicIp.getAddress().addr();
        List<Long> leftoverIds = Stream.concat(leftovers.stream(), leftoverFirewallRules.stream()).map(FirewallRuleVO::getId).collect(Collectors.toList());
        logger.info(String.format("Removing network rules %s on %s left over from an earlier failed add-node to Kubernetes cluster %s",
                leftoverIds, ip, kubernetesCluster.getName()));
        List<String> failed = revokeNodeSshRules(leftoverFirewallRules, leftovers);
        if (!failed.isEmpty()) {
            throw new CloudRuntimeException(String.format("Failed to remove network rules %s on %s left over from an earlier failed " +
                    "add-node to Kubernetes cluster %s; remove them and retry", failed, ip, kubernetesCluster.getName()));
        }
    }

    private boolean isInAnyCluster(long vmId) {
        return kubernetesClusterVmMapDao.listByVmId(vmId).stream()
                .anyMatch(map -> Objects.nonNull(kubernetesClusterDao.findById(map.getClusterId())));
    }

    /**
     * Returns the lowest port after the positional ports of the control and default worker nodes (those stay
     * reserved even without a rule, as other operations reach those nodes by position) that the node's SSH rules can
     * take on the public IP, or null if there is none. A gap left by a removed node is reused.
     */
    protected Integer getFreeNodeSshPort(Network network, IpAddress publicIp) {
        boolean vpc = Objects.nonNull(network.getVpcId());
        long positionalNodes = kubernetesClusterVmMapDao.listByClusterId(kubernetesCluster.getId()).stream()
                .filter(map -> !map.isExternalNode() && !map.isEtcdNode()).count();
        List<FirewallRuleVO> rules = firewallRulesDao.listByIp(publicIp.getId()).stream()
                .filter(rule -> Objects.nonNull(rule.getSourcePortStart()) && Objects.nonNull(rule.getSourcePortEnd())
                        && rule.getSourcePortEnd() >= CLUSTER_NODES_DEFAULT_START_SSH_PORT)
                .sorted(Comparator.comparingInt(FirewallRuleVO::getSourcePortStart))
                .collect(Collectors.toList());
        int port = CLUSTER_NODES_DEFAULT_START_SSH_PORT + (int) positionalNodes;
        for (FirewallRuleVO rule : rules) {
            if (rule.getSourcePortStart() > port) {
                break;
            }
            if (rule.getSourcePortEnd() < port) {
                continue;
            }
            if (rule.getPurpose() == FirewallRule.Purpose.Firewall && (vpc || !isOpenTcpFirewallRule(rule))) {
                continue;
            }
            port = Math.max(port, rule.getSourcePortEnd() + 1);
        }
        return port > NetUtils.PORT_RANGE_MAX ? null : port;
    }

    /**
     * A firewall rule clashes with the node's tcp 0.0.0.0/0 firewall rule when it is tcp, lists 0.0.0.0/0 among its
     * source CIDRs and has no destination CIDRs, matching FirewallManagerImpl#detectRulesConflict.
     */
    private boolean isOpenTcpFirewallRule(FirewallRuleVO rule) {
        if (!NetUtils.TCP_PROTO.equalsIgnoreCase(rule.getProtocol())) {
            return false;
        }
        firewallRulesDao.loadSourceCidrs(rule);
        firewallRulesDao.loadDestinationCidrs(rule);
        return Objects.nonNull(rule.getSourceCidrList()) && rule.getSourceCidrList().contains(NetUtils.ALL_IP4_CIDRS)
                && CollectionUtils.isEmpty(rule.getDestinationCidrList());
    }

    private boolean isOpenToAll(FirewallRuleVO rule) {
        firewallRulesDao.loadSourceCidrs(rule);
        List<String> cidrs = rule.getSourceCidrList();
        return CollectionUtils.isEmpty(cidrs) || List.of(NetUtils.ALL_IP4_CIDRS).equals(cidrs);
    }

    private boolean isSinglePort(FirewallRuleVO rule) {
        return Objects.nonNull(rule.getSourcePortStart()) && Objects.equals(rule.getSourcePortStart(), rule.getSourcePortEnd());
    }

    private boolean hasNodeSshPfShape(PortForwardingRuleVO pfRule) {
        return isSinglePort(pfRule) && NetUtils.TCP_PROTO.equalsIgnoreCase(pfRule.getProtocol())
                && pfRule.getDestinationPortStart() == DEFAULT_SSH_PORT && pfRule.getDestinationPortEnd() == DEFAULT_SSH_PORT
                && pfRule.getState() != FirewallRule.State.Revoke;
    }

    private boolean isNodeSshPfRule(PortForwardingRuleVO pfRule, IpAddress publicIp, int port) {
        return Objects.equals(pfRule.getSourceIpAddressId(), publicIp.getId()) && Objects.equals(pfRule.getSourcePortStart(), port)
                && hasNodeSshPfShape(pfRule);
    }

    private List<FirewallRuleVO> listNodeSshRules(Network network, IpAddress publicIp, long vmId, int port) {
        List<FirewallRuleVO> rules = new ArrayList<>();
        if (Objects.isNull(network.getVpcId())) {
            rules.addAll(firewallRulesDao.listByIpPurposePortsProtocolAndNotRevoked(publicIp.getId(), port, port,
                    NetUtils.TCP_PROTO, FirewallRule.Purpose.Firewall));
        }
        portForwardingRulesDao.listByVm(vmId).stream().filter(pfRule -> isNodeSshPfRule(pfRule, publicIp, port)).forEach(rules::add);
        return rules;
    }

    protected Set<Long> listNodeSshRuleIds(Network network, IpAddress publicIp, long vmId, int port) {
        return listNodeSshRules(network, publicIp, vmId, port).stream().map(FirewallRuleVO::getId).collect(Collectors.toSet());
    }

    /**
     * Revokes the SSH port-forwarding and firewall rules for the node, except those in preExistingRuleIds,
     * so that a failed add only removes what it created.
     */
    protected void revertNetworkRules(Network network, IpAddress publicIp, long vmId, int port, Set<Long> preExistingRuleIds) {
        logger.debug(String.format("Reverting network rules for VM ID %s on network %s", vmId, network.getName()));
        List<FirewallRuleVO> rules = listNodeSshRules(network, publicIp, vmId, port);
        rules.removeIf(rule -> preExistingRuleIds.contains(rule.getId()));
        Map<Boolean, List<FirewallRuleVO>> byPurpose = rules.stream()
                .collect(Collectors.partitioningBy(rule -> rule.getPurpose() == FirewallRule.Purpose.PortForwarding));
        List<String> failed = revokeNodeSshRules(byPurpose.get(false), byPurpose.get(true));
        if (!failed.isEmpty()) {
            logger.warn(String.format("Failed to remove network rules %s for VM ID %s on port %s", failed, vmId, port));
        }
    }

    /**
     * Revokes the firewall rules, then the port-forwarding rules, each on its own, and returns the UUIDs of the rules
     * that could not be removed. Firewall rules go first because one opened together with a PF rule is related to it,
     * and removing the PF would cascade-delete the firewall row without taking the rule off the router. A PF rule whose
     * port's firewall rule could not be removed is kept, so the next add-node still sees the pair as a leftover.
     */
    private List<String> revokeNodeSshRules(List<FirewallRuleVO> firewallRules, List<FirewallRuleVO> pfRules) {
        List<String> failed = revokeEachRule(firewallRules);
        Set<Integer> failedPorts = firewallRules.stream().filter(rule -> failed.contains(rule.getUuid()))
                .map(FirewallRuleVO::getSourcePortStart).collect(Collectors.toSet());
        failed.addAll(revokeEachRule(pfRules.stream().filter(rule -> !failedPorts.contains(rule.getSourcePortStart()))
                .collect(Collectors.toList())));
        return failed;
    }

    private List<String> revokeEachRule(List<FirewallRuleVO> rules) {
        List<String> failed = new ArrayList<>();
        for (FirewallRuleVO rule : rules) {
            boolean revoked = false;
            try {
                if (rule.getPurpose() == FirewallRule.Purpose.PortForwarding) {
                    logger.debug(String.format("Removing port forwarding rule %s", rule.getId()));
                    revoked = rulesService.revokePortForwardingRule(rule.getId(), true);
                } else {
                    logger.debug(String.format("Removing firewall rule %s", rule.getId()));
                    revoked = firewallService.revokeIngressFirewallRule(rule.getId(), true);
                }
            } catch (Exception e) {
                logger.warn(String.format("Failed to remove network rule %s", rule.getId()), e);
            }
            if (!revoked) {
                failed.add(rule.getUuid());
            }
        }
        return failed;
    }
}
