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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType;
import com.cloud.service.ServiceOfferingVO;
import com.cloud.storage.VMTemplateVO;
import org.apache.cloudstack.api.ApiCommandResourceType;
import org.apache.cloudstack.api.InternalIdentity;
import org.apache.cloudstack.context.CallContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import com.cloud.dc.DataCenter;
import com.cloud.event.ActionEventUtils;
import com.cloud.event.EventVO;
import com.cloud.exception.InsufficientCapacityException;
import com.cloud.exception.ManagementServerException;
import com.cloud.exception.NetworkRuleConflictException;
import com.cloud.exception.ResourceUnavailableException;
import com.cloud.exception.VirtualMachineMigrationException;
import com.cloud.hypervisor.Hypervisor;
import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterEventTypes;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterService;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.utils.KubernetesClusterUtil;
import com.cloud.network.IpAddress;
import com.cloud.network.Network;
import com.cloud.network.rules.FirewallRule;
import com.cloud.network.rules.FirewallRuleVO;
import com.cloud.network.rules.PortForwardingRuleVO;
import com.cloud.offering.ServiceOffering;
import com.cloud.storage.LaunchPermissionVO;
import com.cloud.uservm.UserVm;
import com.cloud.utils.Pair;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.net.NetUtils;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.VirtualMachine;
import com.cloud.vm.dao.VMInstanceDao;
import org.apache.logging.log4j.Level;

import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.CONTROL;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.DEFAULT;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.ETCD;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.WORKER;

public class KubernetesClusterScaleWorker extends KubernetesClusterResourceModifierActionWorker {

    @Inject
    protected VMInstanceDao vmInstanceDao;

    private Map<String, ServiceOffering> serviceOfferingNodeTypeMap;
    private Long clusterSize;
    private List<Long> nodeIds;
    private KubernetesCluster.State originalState;
    private Network network;
    private Long minSize;
    private Long maxSize;
    private Boolean isAutoscalingEnabled;
    protected long scaleTimeoutTime;
    /** How many times to try to drain a node that is being removed, as scale-down did before. */
    private static final int SCALE_DOWN_DRAIN_ATTEMPTS = 3;

    protected KubernetesClusterScaleWorker(final KubernetesCluster kubernetesCluster, final KubernetesClusterManagerImpl clusterManager) {
        super(kubernetesCluster, clusterManager);
    }

    public KubernetesClusterScaleWorker(final KubernetesCluster kubernetesCluster,
                                        final Map<String, ServiceOffering> serviceOfferingNodeTypeMap,
                                        final Long clusterSize,
                                        final List<Long> nodeIds,
                                        final Boolean isAutoscalingEnabled,
                                        final Long minSize,
                                        final Long maxSize,
                                        final KubernetesClusterManagerImpl clusterManager) {
        super(kubernetesCluster, clusterManager);
        this.serviceOfferingNodeTypeMap = serviceOfferingNodeTypeMap;
        this.nodeIds = nodeIds;
        this.isAutoscalingEnabled = isAutoscalingEnabled;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.originalState = kubernetesCluster.getState();
        if (this.nodeIds != null) {
            this.clusterSize = kubernetesCluster.getNodeCount() - this.nodeIds.size();
        } else {
            this.clusterSize = clusterSize;
        }

    }

    protected void init() {
        super.init();
        this.network = networkDao.findById(kubernetesCluster.getNetworkId());
    }

    private void logTransitStateToFailedIfNeededAndThrow(final Level logLevel, final String message, final Exception e) throws CloudRuntimeException {
        // only fail the scaling this worker started: before it moved the cluster, a scaling state belongs to another one
        if (isScalingStateEntered()) {
            logTransitStateAndThrow(logLevel, message, kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed, e);
        } else {
            logAndThrow(logLevel, message, e);
        }
    }

    private void logTransitStateToFailedIfNeededAndThrow(final Level logLevel, final String message) throws CloudRuntimeException {
        logTransitStateToFailedIfNeededAndThrow(logLevel, message, null);
    }

    protected IpAddress getNodeSshIp() throws ManagementServerException {
        return getNodeSshIp(network);
    }

    /**
     * Gives SSH access to each of the given nodes that has no SSH port forwarding rule yet, such as the nodes just
     * created by the scale. The rules of the other nodes are left as they are. A node created with the cluster or by a
     * scale gets the port of its place among those nodes when that port is free, else the lowest free port from
     * {@link #CLUSTER_NODES_DEFAULT_START_SSH_PORT} up. Any other node gets the lowest free port after those places. An
     * existing node whose SSH port no firewall rule covers at all gets a firewall rule for it again.
     */
    protected void provisionMissingNodeSshRules(final List<Long> nodeVmIds) throws ManagementServerException {
        IpAddress sshIp = getNodeSshIp();
        if (sshIp == null) {
            logger.debug("Network: {} for Kubernetes cluster: {} is not an isolated network or ROUTED network, therefore, no need for network rules", network, kubernetesCluster);
            return;
        }
        boolean firewall = network.getVpcId() == null;
        List<PortForwardingRuleVO> sshRules = portForwardingRulesDao.listByIpAndNotRevoked(sshIp.getId()).stream()
                .filter(rule -> isNodeSshPortForwardingRule(rule, sshIp) && nodeVmIds.contains(rule.getVirtualMachineId()))
                .collect(Collectors.toList());
        Set<Long> nodesWithSshRule = sshRules.stream().map(PortForwardingRuleVO::getVirtualMachineId).collect(Collectors.toSet());
        List<Long> positionalNodeIds = getKubernetesClusterVMMaps().stream()
                .filter(map -> !map.isExternalNode() && !map.isEtcdNode())
                .map(KubernetesClusterVmMapVO::getVmId).collect(Collectors.toList());
        int firstNonPositionalPort = CLUSTER_NODES_DEFAULT_START_SSH_PORT + positionalNodeIds.size();
        List<FirewallRuleVO> rules = firewallRulesDao.listByIp(sshIp.getId()).stream()
                .filter(rule -> rule.getSourcePortStart() != null && rule.getSourcePortEnd() != null
                        && rule.getSourcePortEnd() >= CLUSTER_NODES_DEFAULT_START_SSH_PORT)
                .collect(Collectors.toList());
        List<FirewallRuleVO> usedPortRules = rules.stream()
                .filter(rule -> rule.getPurpose() != FirewallRule.Purpose.Firewall).collect(Collectors.toList());
        List<FirewallRuleVO> openPortRules = !firewall ? Collections.emptyList() : rules.stream()
                .filter(rule -> rule.getPurpose() == FirewallRule.Purpose.Firewall && isOpenTcpFirewallRule(rule))
                .collect(Collectors.toList());
        if (firewall) {
            List<FirewallRuleVO> firewallRules = rules.stream()
                    .filter(rule -> rule.getPurpose() == FirewallRule.Purpose.Firewall && rule.getState() != FirewallRule.State.Revoke
                            && NetUtils.TCP_PROTO.equalsIgnoreCase(rule.getProtocol()))
                    .collect(Collectors.toList());
            for (PortForwardingRuleVO sshRule : sshRules) {
                int port = sshRule.getSourcePortStart();
                if (!isPortInRules(firewallRules, port)) {
                    logger.warn("SSH port {} of node VM {} of Kubernetes cluster {} is not open on {}, adding a firewall rule for it",
                            port, sshRule.getVirtualMachineId(), kubernetesCluster.getName(), sshIp.getAddress().addr());
                    try {
                        provisionNodeSshFirewallRule(sshIp, port, port);
                    } catch (NoSuchFieldException | IllegalAccessException | ResourceUnavailableException | NetworkRuleConflictException | RuntimeException e) {
                        throw new ManagementServerException(String.format("Failed to provision SSH firewall rule for port %d of %s of Kubernetes cluster %s",
                                port, sshIp.getAddress().addr(), kubernetesCluster.getName()), e);
                    }
                }
            }
        }
        Set<Integer> takenPorts = new HashSet<>();
        for (Long vmId : nodeVmIds) {
            if (nodesWithSshRule.contains(vmId)) {
                continue;
            }
            int position = positionalNodeIds.indexOf(vmId);
            int preferredPort = position < 0 ? firstNonPositionalPort : CLUSTER_NODES_DEFAULT_START_SSH_PORT + position;
            int searchFrom = position < 0 ? firstNonPositionalPort : CLUSTER_NODES_DEFAULT_START_SSH_PORT;
            Integer port = getFreeNodeSshPort(usedPortRules, takenPorts, preferredPort, searchFrom);
            if (port == null) {
                throw new ManagementServerException(String.format("No free port left on %s for SSH to node VM %d of Kubernetes cluster %s",
                        sshIp.getAddress().addr(), vmId, kubernetesCluster.getName()));
            }
            takenPorts.add(port);
            boolean firewallRuleAdded = false;
            try {
                if (firewall && !isPortInRules(openPortRules, port)) {
                    firewallRuleAdded = true;
                    provisionNodeSshFirewallRule(sshIp, port, port);
                }
                provisionPublicIpPortForwardingRule(sshIp, network, owner, vmId, port, DEFAULT_SSH_PORT);
            } catch (NoSuchFieldException | IllegalAccessException | ResourceUnavailableException | NetworkRuleConflictException | RuntimeException e) {
                removeNodeSshRules(sshIp, listNodeSshPortForwardingRules(sshIp, vmId), false);
                if (firewallRuleAdded) {
                    removeNodeSshFirewallRules(sshIp, port, port);
                }
                throw new ManagementServerException(String.format("Failed to provision SSH access on port %d of %s to node VM %d of Kubernetes cluster %s",
                        port, sshIp.getAddress().addr(), vmId, kubernetesCluster.getName()), e);
            }
        }
    }

    /**
     * Returns preferredPort, or else the lowest port from searchFrom up, that is not in takenPorts and that no port
     * forwarding, load balancing or other non-firewall rule in usedPortRules covers, or null if there is none.
     */
    protected Integer getFreeNodeSshPort(List<FirewallRuleVO> usedPortRules, Set<Integer> takenPorts, int preferredPort, int searchFrom) {
        if (!takenPorts.contains(preferredPort) && !isPortInRules(usedPortRules, preferredPort)) {
            return preferredPort;
        }
        for (int port = searchFrom; port <= NetUtils.PORT_RANGE_MAX; port++) {
            if (!takenPorts.contains(port) && !isPortInRules(usedPortRules, port)) {
                return port;
            }
        }
        return null;
    }

    private boolean isPortInRules(List<FirewallRuleVO> rules, int port) {
        return rules.stream().anyMatch(rule -> rule.getSourcePortStart() <= port && port <= rule.getSourcePortEnd());
    }

    protected KubernetesClusterVO updateKubernetesClusterEntryForNodeType(final Long newWorkerSize, final KubernetesClusterNodeType nodeType,
                                                                          final ServiceOffering newServiceOffering,
                                                                          final boolean updateNodeOffering, boolean updateClusterOffering) throws CloudRuntimeException {
        final ServiceOffering serviceOffering = newServiceOffering == null ?
                getServiceOfferingForNodeTypeOnCluster(nodeType, kubernetesCluster) : newServiceOffering;
        final Long serviceOfferingId = newServiceOffering == null ? null : serviceOffering.getId();

        Pair<Long, Long> clusterCountAndCapacity = calculateNewClusterCountAndCapacity(newWorkerSize, nodeType, serviceOffering);
        long cores = clusterCountAndCapacity.first();
        long memory = clusterCountAndCapacity.second();

        KubernetesClusterVO kubernetesClusterVO = updateKubernetesClusterEntry(cores, memory, newWorkerSize, serviceOfferingId,
            kubernetesCluster.getAutoscalingEnabled(), kubernetesCluster.getMinSize(), kubernetesCluster.getMaxSize(), nodeType, updateNodeOffering, updateClusterOffering);
        if (kubernetesClusterVO == null) {
            logTransitStateAndThrow(Level.ERROR, String.format("Scaling Kubernetes cluster %s failed, unable to update Kubernetes cluster",
                    kubernetesCluster.getName()), kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed);
        }
        return kubernetesClusterVO;
    }

    protected Pair<Long, Long> calculateNewClusterCountAndCapacity(Long newWorkerSize, KubernetesClusterNodeType nodeType, ServiceOffering serviceOffering) {
        long cores;
        long memory;
        long totalClusterSize = newWorkerSize == null ? kubernetesCluster.getTotalNodeCount() : (newWorkerSize + kubernetesCluster.getControlNodeCount() + kubernetesCluster.getEtcdNodeCount());

        if (nodeType == DEFAULT) {
            cores = serviceOffering.getCpu() * totalClusterSize;
            memory = serviceOffering.getRamSize() * totalClusterSize;
        } else {
            // Nodes added with addNodesToKubernetesCluster keep their own offering and are never scaled,
            // so their share of the totals stays as it was counted when they were added
            long addedNodeCount = WORKER == nodeType ? getAddedNodeCount() : 0;
            long nodeCount = Math.max(0, getNodeCountForType(nodeType, kubernetesCluster) - addedNodeCount);
            // Read from the cluster, not its VMs: an offering scale has already upgraded the VMs at this point
            ServiceOffering previousOffering = nodeCount > 0 ?
                    getServiceOfferingForNodeTypeOnCluster(nodeType, kubernetesCluster) : serviceOffering;
            Pair<Long, Long> previousNodesCapacity = calculateNodesCapacity(previousOffering, nodeCount);
            if (WORKER == nodeType) {
                nodeCount = Math.max(0, (newWorkerSize == null ? kubernetesCluster.getNodeCount() : newWorkerSize) - addedNodeCount);
            }
            Pair<Long, Long> newNodesCapacity = calculateNodesCapacity(serviceOffering, nodeCount);
            Pair<Long, Long> newClusterCapacity = calculateClusterNewCapacity(kubernetesCluster, previousNodesCapacity, newNodesCapacity);
            cores = newClusterCapacity.first();
            memory = newClusterCapacity.second();
        }
        return new Pair<>(cores, memory);
    }

    private long getAddedNodeCount() {
        return kubernetesClusterVmMapDao.listByClusterIdAndVmType(kubernetesCluster.getId(), WORKER).stream()
                .filter(KubernetesClusterVmMapVO::isExternalNode).count();
    }

    private List<KubernetesClusterVmMapVO> listCksNodesOfType(KubernetesClusterNodeType nodeType, KubernetesCluster cluster) {
        return kubernetesClusterVmMapDao.listByClusterIdAndVmType(cluster.getId(), nodeType).stream()
                .filter(vm -> !vm.isExternalNode()).collect(Collectors.toList());
    }

    private long getNodeCountForType(KubernetesClusterNodeType nodeType, KubernetesCluster kubernetesCluster) {
        if (WORKER == nodeType) {
            return kubernetesCluster.getNodeCount();
        } else if (CONTROL == nodeType) {
            return kubernetesCluster.getControlNodeCount();
        } else if (ETCD == nodeType) {
            return kubernetesCluster.getEtcdNodeCount();
        }
        return kubernetesCluster.getTotalNodeCount();
    }

    protected Pair<Long, Long> calculateClusterNewCapacity(KubernetesCluster kubernetesCluster,
                                                           Pair<Long, Long> previousNodeTypeCapacity,
                                                           Pair<Long, Long> newNodeTypeCapacity) {
        long previousCores = kubernetesCluster.getCores();
        long previousMemory = kubernetesCluster.getMemory();
        long newCores = previousCores - previousNodeTypeCapacity.first() + newNodeTypeCapacity.first();
        long newMemory = previousMemory - previousNodeTypeCapacity.second() + newNodeTypeCapacity.second();
        return new Pair<>(newCores, newMemory);
    }

    protected Pair<Long, Long> calculateNodesCapacity(ServiceOffering offering, long nodeCount) {
        return new Pair<>(offering.getCpu() * nodeCount, offering.getRamSize() * nodeCount);
    }

    /**
     * Deletes the Kubernetes node object of a VM that is being removed from the cluster. The node has been drained
     * already, see {@link #drainNodesToRemove(List)}. A failed attempt is retried after waiting.
     */
    private boolean removeKubernetesClusterNode(final String ipAddress, final int port, final UserVm userVm, final int retries, final int waitDuration) {
        final String hostName = getNodeName(userVm);
        for (int attempt = 1; attempt <= retries; attempt++) {
            if (attempt > 1 && !sleepBeforeRetry(waitDuration)) {
                break;
            }
            try {
                Pair<Boolean, String> result = executeSshCommand(ipAddress, port,
                        String.format("sudo /opt/bin/kubectl delete node %s --ignore-not-found", hostName), 10000, 10000, 30000);
                if (result.first()) {
                    return true;
                }
                logger.warn("Deleting node: {} on VM: {} in Kubernetes cluster: {} unsuccessful: {}", hostName, userVm, kubernetesCluster, result.second());
            } catch (Exception e) {
                logger.warn("Failed to remove Kubernetes cluster: {} node: {} on VM: {}", kubernetesCluster, hostName, userVm, e);
            }
        }
        return false;
    }

    /**
     * Drains the nodes that are being removed, before any of them is removed. All of them are cordoned first, so
     * that the pods evicted from one are not moved to another one that is going away. If a node can't be drained,
     * all of them are uncordoned and the scale-down stops with nothing removed: the nodes are still working members
     * of the cluster, so the cluster goes back to the state it was in, rather than to Alert.
     *
     * @return the nodes that were cordoned here, as opposed to the ones that were cordoned already
     */
    protected List<UserVmVO> drainNodesToRemove(final List<UserVmVO> vms) throws CloudRuntimeException {
        List<UserVmVO> cordoned = new ArrayList<>();
        String failure = null;
        for (UserVmVO vm : vms) {
            if (isVmDown(vm.getState())) {
                // A down VM runs no pods, and draining its NotReady node would wait for pod deletions that the node
                // can never confirm. Deleting the node object is enough.
                continue;
            }
            // A node cordoned by someone else has to stay cordoned if the scale-down is rolled back
            final boolean cordonedAlready = isNodeCordoned(vm);
            String output;
            try {
                Pair<Boolean, String> result = executeSshCommand(publicIpAddress, sshPort,
                        String.format("sudo /opt/bin/kubectl cordon %s", getNodeName(vm)), 10000, 10000, 30000);
                if (result.first()) {
                    if (!cordonedAlready) {
                        cordoned.add(vm);
                    }
                    continue;
                }
                output = result.second();
            } catch (Exception e) {
                output = e.getMessage();
            }
            failure = String.format("unable to cordon Kubernetes node: %s on VM: %s: %s", getNodeName(vm), vm, output);
            break;
        }
        if (failure == null) {
            for (UserVmVO vm : vms) {
                if (isVmDown(vm.getState())) {
                    continue;
                }
                if (System.currentTimeMillis() > scaleTimeoutTime) {
                    failure = String.format("scaling action timed out before Kubernetes node: %s on VM: %s was drained", getNodeName(vm), vm);
                    break;
                }
                Pair<Boolean, String> drained = drainKubernetesClusterNode(publicIpAddress, sshPort, vm, SCALE_DOWN_DRAIN_ATTEMPTS, DRAIN_RETRY_WAIT, scaleTimeoutTime);
                if (!drained.first()) {
                    failure = String.format("unable to drain Kubernetes node: %s on VM: %s: %s", getNodeName(vm), vm, drained.second());
                    break;
                }
            }
        }
        if (failure == null) {
            return cordoned;
        }
        uncordonNodes(cordoned);
        String message = String.format("Scaling failed for Kubernetes cluster %s, %s. No node was removed", kubernetesCluster, failure);
        logger.error(message);
        // Nothing has changed, so the cluster goes back to the state it was in: Scaling goes back to Running (and
        // ScalingStoppedCluster to Stopped) on OperationSucceeded, and a cluster scaled from Alert goes back to Alert.
        // stateTransitTo only does this while the cluster is still in the scaling state this operation moved it to.
        stateTransitTo(kubernetesCluster.getId(), KubernetesCluster.State.Alert.equals(originalState) ?
                KubernetesCluster.Event.OperationFailed : KubernetesCluster.Event.OperationSucceeded);
        throw new CloudRuntimeException(message);
    }


    private void validateKubernetesClusterScaleOfferingParameters() throws CloudRuntimeException {
        if (KubernetesCluster.State.Created.equals(originalState)) {
            return;
        }
        final long originalNodeCount = kubernetesCluster.getTotalNodeCount();
        List<KubernetesClusterVmMapVO> vmList = kubernetesClusterVmMapDao.listByClusterId(kubernetesCluster.getId());
        if (vmList == null || vmList.isEmpty() || vmList.size() < originalNodeCount) {
            logTransitStateToFailedIfNeededAndThrow(Level.WARN, String.format("Scaling Kubernetes cluster : %s failed, it is in unstable state as not enough existing VM instances found!", kubernetesCluster.getName()));
        } else {
            for (KubernetesClusterVmMapVO vmMapVO : vmList) {
                VMInstanceVO vmInstance = vmInstanceDao.findById(vmMapVO.getVmId());
                if (vmInstance != null && vmInstance.getState().equals(VirtualMachine.State.Running) &&
                        vmInstance.getHypervisorType() != Hypervisor.HypervisorType.XenServer &&
                        vmInstance.getHypervisorType() != Hypervisor.HypervisorType.VMware &&
                        vmInstance.getHypervisorType() != Hypervisor.HypervisorType.Simulator) {
                    logTransitStateToFailedIfNeededAndThrow(Level.WARN, String.format("Scaling Kubernetes cluster : %s failed, scaling Kubernetes cluster with running VMs on hypervisor %s is not supported!", kubernetesCluster.getName(), vmInstance.getHypervisorType()));
                }
            }
        }
    }

    protected void validateKubernetesClusterScaleSizeParameters() throws CloudRuntimeException {
        final long originalClusterSize = kubernetesCluster.getNodeCount();
        if (network == null) {
            logTransitStateToFailedIfNeededAndThrow(Level.WARN, String.format("Scaling failed for Kubernetes cluster : %s, cluster network not found", kubernetesCluster.getName()));
        }
        // Check capacity and transition state
        final long newVmRequiredCount = clusterSize - originalClusterSize;
        final ServiceOffering workerServiceOffering = getServiceOfferingForNodeTypeOnCluster(WORKER, kubernetesCluster);
        if (workerServiceOffering == null) {
            logTransitStateToFailedIfNeededAndThrow(Level.WARN, String.format("Scaling failed for Kubernetes cluster : %s, worker node service offering not found", kubernetesCluster.getName()));
        }
        if (newVmRequiredCount > 0) {
            final DataCenter zone = dataCenterDao.findById(kubernetesCluster.getZoneId());
            VMTemplateVO clusterTemplate = templateDao.findById(kubernetesCluster.getTemplateId());
            try {
                if (KubernetesCluster.State.Running.equals(originalState) || KubernetesCluster.State.Alert.equals(originalState)) {
                    plan(newVmRequiredCount, zone, workerServiceOffering, kubernetesCluster.getDomainId(), kubernetesCluster.getAccountId(), clusterTemplate.getHypervisorType(), clusterTemplate.getArch());
                } else {
                    plan(kubernetesCluster.getTotalNodeCount() + newVmRequiredCount, zone, workerServiceOffering, kubernetesCluster.getDomainId(), kubernetesCluster.getAccountId(), clusterTemplate.getHypervisorType(), clusterTemplate.getArch());
                }
            } catch (InsufficientCapacityException e) {
                logTransitStateToFailedIfNeededAndThrow(Level.WARN, String.format("Scaling failed for Kubernetes cluster : %s in zone : %s, insufficient capacity", kubernetesCluster.getName(), zone.getName()));
            }
        }
        List<KubernetesClusterVmMapVO> vmList = kubernetesClusterVmMapDao.listByClusterId(kubernetesCluster.getId());
        if (CollectionUtils.isEmpty(vmList) || vmList.size() < kubernetesCluster.getTotalNodeCount()) {
            logTransitStateToFailedIfNeededAndThrow(Level.ERROR, String.format("Scaling failed for Kubernetes cluster : %s, it is in unstable state as not enough existing VM instances found", kubernetesCluster.getName()));
        }
    }

    protected void scaleKubernetesClusterOffering(KubernetesClusterNodeType nodeType, ServiceOffering serviceOffering,
                                                  boolean updateNodeOffering, boolean updateClusterOffering) throws CloudRuntimeException {
        validateKubernetesClusterScaleOfferingParameters();
        enterScalingState(KubernetesCluster.Event.ScaleUpRequested);
        if (KubernetesCluster.State.Created.equals(originalState)) {
            kubernetesCluster = updateKubernetesClusterEntryForNodeType(null, nodeType, serviceOffering, updateNodeOffering, updateClusterOffering);
            return;
        }
        final long size = getNodeCountForType(nodeType, kubernetesCluster);
        List<KubernetesClusterVmMapVO> vmList = listCksNodesOfType(nodeType, kubernetesCluster);
        final long tobeScaledVMCount =  Math.min(vmList.size(), size);
        for (long i = 0; i < tobeScaledVMCount; i++) {
            KubernetesClusterVmMapVO vmMapVO = vmList.get((int) i);
            UserVmVO userVM = userVmDao.findById(vmMapVO.getVmId());
            boolean result = false;
            try {
                result = userVmManager.upgradeVirtualMachine(userVM.getId(), serviceOffering.getId(), new HashMap<String, String>());
            } catch (RuntimeException | ResourceUnavailableException | ManagementServerException | VirtualMachineMigrationException e) {
                logTransitStateAndThrow(Level.ERROR, String.format("Scaling Kubernetes cluster : %s failed, unable to scale cluster VM : %s due to %s", kubernetesCluster.getName(), userVM.getDisplayName(), e.getMessage()), kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed, e);
            }
            if (!result) {
                logTransitStateAndThrow(Level.WARN, String.format("Scaling Kubernetes cluster : %s failed, unable to scale cluster VM : %s", kubernetesCluster.getName(), userVM.getDisplayName()),kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed);
            }
            if (System.currentTimeMillis() > scaleTimeoutTime) {
                logTransitStateAndThrow(Level.WARN, String.format("Scaling Kubernetes cluster : %s failed, scaling action timed out", kubernetesCluster.getName()),kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed);
            }
        }
        kubernetesCluster = updateKubernetesClusterEntryForNodeType(null, nodeType, serviceOffering, updateNodeOffering, updateClusterOffering);
    }

    protected void removeNodesFromCluster(List<KubernetesClusterVmMapVO> vmMaps) throws CloudRuntimeException {
        IpAddress sshIp = null;
        try {
            sshIp = getNodeSshIp();
        } catch (ManagementServerException e) {
            logTransitStateAndThrow(Level.ERROR, String.format("Scaling failed for Kubernetes " +
                    "cluster %s, unable to update network rules", kubernetesCluster),
                    kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed, e);
        }
        List<UserVmVO> vms = vmMaps.stream().map(vmMapVO -> userVmDao.findById(vmMapVO.getVmId())).collect(Collectors.toList());
        List<UserVmVO> cordoned = drainNodesToRemove(vms);
        // Index of the first VM whose Kubernetes node is still in the cluster
        int firstInCluster = 0;
        // VMs taken out of the cluster's VM map so far
        List<UserVmVO> removed = new ArrayList<>();
        boolean completed = false;
        try {
            for (int i = 0; i < vmMaps.size(); i++) {
                // No timeout check here: the nodes are drained, and stopping part way would leave the cluster
                // with some of them removed and the node count not updated
                UserVmVO userVM = vms.get(i);
                KubernetesClusterVmMapVO vmMapVO = vmMaps.get(i);
                // listed before the VM is expunged, which removes its port forwarding rules but not its firewall rules
                List<PortForwardingRuleVO> nodeSshRules = sshIp == null ? Collections.emptyList()
                        : listNodeSshPortForwardingRules(sshIp, vmMapVO.getVmId());
                logger.info("Removing vm {} from cluster {}", userVM, kubernetesCluster);
                if (!removeKubernetesClusterNode(publicIpAddress, sshPort, userVM, 3, 30000)) {
                    logTransitStateAndThrow(Level.ERROR, String.format("Scaling failed for Kubernetes" +
                            " cluster %s, failed to remove Kubernetes node: %s running on VM : %s",
                            kubernetesCluster, userVM.getHostName(), userVM), kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed);
                }
                firstInCluster = i + 1;
                try {
                    destroyRemovedNodeVm(userVM);
                } finally {
                    // The node has left the Kubernetes cluster, so its VM leaves the cluster's VM map, whatever
                    // happened to the VM. A map entry kept for it would never match a Kubernetes node again: the
                    // state scanner would hold the cluster in Alert, which blocks the next scale, and the node count
                    // and the next scale-down's choice of nodes would be worked out from a VM that is gone. The
                    // entry is deleted only now as the VM's expunge looks it up (to leave alone the port-forwarding
                    // rules of a CKS node in an NSX zone).
                    kubernetesClusterVmMapDao.expunge(vmMapVO.getId());
                    removed.add(userVM);
                }
                boolean sshRulesRemoved = true;
                if (sshIp != null) {
                    try {
                        sshRulesRemoved = removeNodeSshRules(sshIp, nodeSshRules, network.getVpcId() == null);
                    } catch (RuntimeException e) {
                        logger.error("Failed to remove the SSH network rules of VM {} of Kubernetes cluster {}", userVM, kubernetesCluster, e);
                        sshRulesRemoved = false;
                    }
                }
                if (!sshRulesRemoved) {
                    logTransitStateAndThrow(Level.ERROR, String.format("Scaling Kubernetes cluster %s failed, unable to remove the SSH network rules of VM %s",
                            kubernetesCluster, userVM), kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed);
                }
            }

            if (sshIp != null && network.getVpcId() == null) {
                try {
                    shrinkNodeSshFirewallRule(sshIp);
                } catch (ManagementServerException e) {
                    logTransitStateAndThrow(Level.ERROR, String.format("Scaling failed for Kubernetes " +
                            "cluster %s, unable to update the SSH firewall rule", kubernetesCluster),
                            kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed, e);
                }
            }
            completed = true;
        } finally {
            if (!completed) {
                // The nodes that are still in the cluster were drained above and would stay unschedulable
                updateKubernetesClusterEntryForRemovedNodes(removed);
                List<UserVmVO> inCluster = vms.subList(firstInCluster, vms.size());
                uncordonNodes(cordoned.stream().filter(inCluster::contains).collect(Collectors.toList()));
            }
        }
    }

    /**
     * Destroys and expunges the VM of a node that has been deleted from the Kubernetes cluster. The caller then takes
     * the VM out of the cluster's VM map, whether this succeeds or not.
     * <ul>
     * <li>If the VM can't be destroyed, the scale fails: the VM may still be running, though it is no longer a
     * Kubernetes node nor a member of the cluster, and has to be destroyed by hand.</li>
     * <li>If the destroyed VM can't be expunged, the scale goes on, with a warning event that names the VM and its
     * state. The expunge task retries VMs left Destroyed, Expunging or in Error.</li>
     * </ul>
     */
    protected void destroyRemovedNodeVm(final UserVmVO userVM) throws CloudRuntimeException {
        CallContext vmContext = CallContext.register(CallContext.current(),
                ApiCommandResourceType.VirtualMachine);
        vmContext.setEventResourceId(userVM.getId());
        // Also left behind if it can't be destroyed, or anything unexpected goes wrong
        boolean leftBehind = true;
        try {
            leftBehind = destroyAndExpungeRemovedNodeVm(userVM);
        } finally {
            CallContext.unregister();
            if (leftBehind) {
                revokeRemovedNodeVmSshRules(userVM);
            }
        }
    }

    /**
     * @return true if the VM is left behind, false if it is expunged
     * @throws CloudRuntimeException if the VM can't be destroyed, once the cluster is moved on OperationFailed
     */
    private boolean destroyAndExpungeRemovedNodeVm(final UserVmVO userVM) throws CloudRuntimeException {
        try {
            userVmService.destroyVm(userVM.getId(), true);
        } catch (ResourceUnavailableException | CloudRuntimeException e) {
            // destroyVm can fail after the VM has been destroyed, on the resource count updates that follow
            RemovedNodeVm vm = lookUpRemovedNodeVm(userVM, "destroyed");
            if (vm.expunged) {
                logger.warn("Destroying VM {} of the node removed from Kubernetes cluster {} reported an error, but the VM is expunged",
                        userVM, kubernetesCluster, e);
                return false;
            }
            if (vm.state != VirtualMachine.State.Destroyed && vm.state != VirtualMachine.State.Expunging) {
                logTransitStateAndThrow(Level.ERROR, String.format("Scaling Kubernetes cluster %s failed, unable to destroy VM %s%s." +
                        " Its node was removed from the Kubernetes cluster and the VM is no longer part of the cluster;" +
                        " it may still be running and has to be destroyed manually", kubernetesCluster, userVM,
                        vm.state == null ? "" : String.format(" (left in state %s)", vm.state)),
                        kubernetesCluster.getId(), KubernetesCluster.Event.OperationFailed, e);
            }
            logger.warn("Destroying VM {} of the node removed from Kubernetes cluster {} reported an error, but the VM is {}",
                    userVM, kubernetesCluster, vm.state, e);
        }
        Exception expungeError = null;
        try {
            if (userVmManager.expunge(userVM)) {
                return false;
            }
        } catch (RuntimeException e) {
            expungeError = e;
        }
        return warnVmNotExpunged(userVM, expungeError);
    }

    /** A removed node's VM, as looked up after destroying or expunging it reported an error */
    private static final class RemovedNodeVm {
        final boolean expunged;
        /** null if the VM is expunged or can't be looked up */
        final VirtualMachine.State state;

        RemovedNodeVm(final boolean expunged, final VirtualMachine.State state) {
            this.expunged = expunged;
            this.state = state;
        }
    }

    /** Doesn't throw: a VM that can't be looked up is taken as not expunged, in an unknown state */
    private RemovedNodeVm lookUpRemovedNodeVm(final UserVmVO userVM, final String failedAction) {
        VMInstanceVO vm;
        try {
            vm = vmInstanceDao.findByIdIncludingRemoved(userVM.getId());
        } catch (RuntimeException e) {
            logger.warn("Unable to look up VM {} of the node removed from Kubernetes cluster {}, after it failed to be {}",
                    userVM, kubernetesCluster, failedAction, e);
            return new RemovedNodeVm(false, null);
        }
        if (vm == null || vm.getRemoved() != null) {
            return new RemovedNodeVm(true, null);
        }
        return new RemovedNodeVm(false, vm.getState());
    }

    /** @return false if the VM turns out to be expunged, true if it is left behind. Doesn't throw. */
    private boolean warnVmNotExpunged(final UserVmVO userVM, final Exception expungeError) {
        RemovedNodeVm vm = lookUpRemovedNodeVm(userVM, "expunged");
        if (vm.expunged) {
            // Expunged meanwhile, by the expunge task for one
            logger.info("VM {} of the node removed from Kubernetes cluster {} has been expunged", userVM, kubernetesCluster);
            return false;
        }
        if (vm.state == null) {
            logger.warn("Unable to expunge VM {} of the node removed from Kubernetes cluster {}; it is no longer part of the cluster",
                    userVM, kubernetesCluster, expungeError);
            return true;
        }
        String message = String.format("Unable to expunge VM %s of the node removed from Kubernetes cluster %s. The VM is" +
                " no longer part of the cluster and is left in state %s, for the expunge task to retry. Until it is expunged" +
                " it keeps the cluster's network in use, and the cluster can't be deleted with its network",
                userVM, kubernetesCluster, vm.state);
        logger.warn(message, expungeError);
        try {
            // Recorded for the VM's owner, who has to see why the VM still holds the network
            ActionEventUtils.onCompletedActionEvent(CallContext.current().getCallingUserId(), userVM.getAccountId(),
                    EventVO.LEVEL_WARN, KubernetesClusterEventTypes.EVENT_KUBERNETES_CLUSTER_SCALE,
                    message, userVM.getId(), ApiCommandResourceType.VirtualMachine.toString(), 0);
        } catch (RuntimeException e) {
            logger.warn("Unable to record an event for VM {} that was not expunged", userVM, e);
        }
        return true;
    }

    /**
     * Revokes the SSH port-forwarding rule that the cluster has on a removed node's VM that is left behind. Expunging
     * the VM would revoke it; the cluster's network rule updates may not: a VPC tier's only clears the ports of the
     * nodes that remain, where the rule would conflict with the rule of a node added later, and none runs if the
     * scale-down stops. Left alone are: a rule provided by NSX (the VM's expunge leaves the port-forwarding rules of a
     * CKS node alone in an NSX zone); the rule on the port this scale reaches the cluster through, if it is the removed
     * VM's, as the nodes that remain are still to be reached through it; and the VM's other rules.
     */
    protected void revokeRemovedNodeVmSshRules(final UserVmVO userVM) {
        boolean registered = false;
        try {
            if (network == null || publicIpAddress == null || manager.isDirectAccess(network) ||
                    networkModel.isProviderSupportServiceInNetwork(network.getId(), Network.Service.PortForwarding, Network.Provider.Nsx)) {
                return;
            }
            // A context of its own rather than the VM's or the scale's
            CallContext.register(CallContext.current(), null);
            registered = true;
            for (PortForwardingRuleVO pfRule : portForwardingRulesDao.listByVm(userVM.getId())) {
                if (!FirewallRule.State.Revoke.equals(pfRule.getState()) && pfRule.getNetworkId() == network.getId() &&
                        pfRule.getDestinationPortStart() == DEFAULT_SSH_PORT && pfRule.getSourcePortStart() >= CLUSTER_NODES_DEFAULT_START_SSH_PORT &&
                        pfRule.getSourcePortStart() != sshPort && isRuleOnAddress(pfRule, publicIpAddress)) {
                    try {
                        rulesService.revokePortForwardingRule(pfRule.getId(), true);
                    } catch (Exception e) {
                        logger.warn("Unable to revoke the SSH port-forwarding rule {} of VM {} of the node removed from Kubernetes cluster {}",
                                pfRule, userVM, kubernetesCluster, e);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to revoke the SSH port-forwarding rule of VM {} of the node removed from Kubernetes cluster {}",
                    userVM, kubernetesCluster, e);
        } finally {
            if (registered) {
                CallContext.unregister();
            }
        }
    }

    /**
     * Takes the VMs that have left the cluster's VM map off the cluster's node count, cores and memory, when a
     * scale-down stops part way, so that the cluster matches its VM map. The scale-down updates them itself once every
     * node has been removed, and this takes the same view of the node count: every VM removed, including a control
     * node removed by ID, comes off the node count, so that the cluster's total node count matches its VM map. Any
     * failure here is logged, so that the scale-down's own error is the one reported.
     */
    private void updateKubernetesClusterEntryForRemovedNodes(final List<UserVmVO> removed) {
        if (removed.isEmpty()) {
            return;
        }
        try {
            long removedCores = 0;
            long removedMemory = 0;
            for (UserVmVO vm : removed) {
                ServiceOfferingVO offering = serviceOfferingDao.findByIdIncludingRemoved(vm.getId(), vm.getServiceOfferingId());
                removedCores += offering.getCpu();
                removedMemory += offering.getRamSize();
            }
            KubernetesClusterVO cluster = kubernetesClusterDao.findById(kubernetesCluster.getId());
            updateKubernetesClusterEntry(cluster.getCores() - removedCores, cluster.getMemory() - removedMemory,
                    cluster.getNodeCount() - removed.size(), null, cluster.getAutoscalingEnabled(), cluster.getMinSize(),
                    cluster.getMaxSize(), WORKER, false, false);
        } catch (RuntimeException e) {
            logger.error("Unable to take {} removed nodes off the node count, cores and memory of Kubernetes cluster {};" +
                    " they no longer match its VMs", removed.size(), kubernetesCluster, e);
        }
    }

    private void scaleDownKubernetesClusterSize() throws CloudRuntimeException {
        enterScalingState(KubernetesCluster.Event.ScaleDownRequested);
        List<KubernetesClusterVmMapVO> vmList;
        if (this.nodeIds != null) {
            vmList = getKubernetesClusterVMMapsForNodes(this.nodeIds);
        } else {
            vmList = getWorkerNodesToRemove();
            if (vmList.isEmpty()) {
                logger.info("No nodes to remove from Kubernetes cluster: {}", kubernetesCluster);
                return;
            }
        }
        Collections.reverse(vmList);
        removeNodesFromCluster(vmList);
    }

    /**
     * Whether the VM is known not to be running, so its node can't be Ready and runs no pods.
     * Other states that aren't Running, such as Migrating or Unknown (a disconnected host), are
     * not treated as down, as the VM may still be running.
     */
    public static boolean isVmDown(VirtualMachine.State state) {
        return VirtualMachine.State.Stopped.equals(state) || VirtualMachine.State.Shutdown.equals(state) ||
                VirtualMachine.State.Error.equals(state) || VirtualMachine.State.Destroyed.equals(state) ||
                VirtualMachine.State.Expunging.equals(state);
    }

    private boolean isVmDown(long vmId) {
        VMInstanceVO vm = vmInstanceDao.findById(vmId);
        return vm != null && isVmDown(vm.getState());
    }

    /**
     * The number of the cluster's Kubernetes nodes whose VMs are down, leaving out etcd nodes,
     * which aren't Kubernetes nodes, and the given VMs.
     */
    protected long getDownNodeCount(Set<Long> excludedVmIds) {
        return getKubernetesClusterVMMaps().stream()
                .filter(vm -> !vm.isEtcdNode() && !excludedVmIds.contains(vm.getVmId()) && isVmDown(vm.getVmId()))
                .count();
    }

    public List<KubernetesClusterVmMapVO> getWorkerNodesToRemove() {
        List<KubernetesClusterVmMapVO> workerVMsMap = new ArrayList<>();
        int externalNodes = 0;
        int controlNodes = 0;
        for (KubernetesClusterVmMapVO vm : getKubernetesClusterVMMaps()) {
            if (vm.isExternalNode()) {
                externalNodes++;
            } else if (vm.isControlNode()) {
                controlNodes++;
            } else if (!vm.isEtcdNode()) {
                workerVMsMap.add(vm);
            }
        }
        // Clusters created before the control node flag existed have it unset; their control VMs are the first ones created
        int unflaggedControlNodes = (int) Math.max(0, Math.min(workerVMsMap.size(), kubernetesCluster.getControlNodeCount() - controlNodes));
        List<KubernetesClusterVmMapVO> removableVMsMap = new ArrayList<>(workerVMsMap.subList(unflaggedControlNodes, workerVMsMap.size()));
        // Nodes are removed from the end of the list: put workers whose VMs are down last, so a
        // scale-down removes those first. The sort is stable, so the order is otherwise kept.
        Set<Long> downVmIds = removableVMsMap.stream().map(KubernetesClusterVmMapVO::getVmId)
                .filter(this::isVmDown).collect(Collectors.toSet());
        removableVMsMap.sort(Comparator.comparing(vm -> downVmIds.contains(vm.getVmId())));
        int desiredWorkerNodes = clusterSize == null ? (int) kubernetesCluster.getNodeCount() : clusterSize.intValue();
        int totalWorkerNodes = removableVMsMap.size();
        // The cluster size also counts nodes added with addNodesToKubernetesCluster, which scaling never removes
        int toRemoveCount = Math.min(totalWorkerNodes, Math.max(0, totalWorkerNodes + externalNodes - desiredWorkerNodes));
        return new ArrayList<>(removableVMsMap.subList(totalWorkerNodes - toRemoveCount, totalWorkerNodes));
    }

    /**
     * Checks the request against the cluster's VMs and corrects the stored node counts, cores and memory
     * if they no longer match the worker VMs, so the scale works from the real node count.
     */
    protected void validateAndSyncWorkerNodeCount() throws CloudRuntimeException {
        if (KubernetesCluster.State.Created.equals(originalState)) {
            return;
        }
        List<KubernetesClusterVmMapVO> clusterVMs = getKubernetesClusterVMMaps();
        long storedNodes = kubernetesCluster.getNodeCount();
        long controlNodes = clusterVMs.stream().filter(KubernetesClusterVmMapVO::isControlNode).count();
        long etcdNodes = clusterVMs.stream().filter(KubernetesClusterVmMapVO::isEtcdNode).count();
        long workerNodes = clusterVMs.stream().filter(vm -> !vm.isControlNode() && !vm.isEtcdNode()).count();
        // Clusters created before the control node flag existed have it unset on every VM, so their VMs can not be told apart
        boolean nodeTypesKnown = controlNodes > 0 || kubernetesCluster.getControlNodeCount() == 0;
        if (!nodeTypesKnown) {
            workerNodes = storedNodes;
        }
        long externalNodes = clusterVMs.stream().filter(KubernetesClusterVmMapVO::isExternalNode).count();
        if (nodeIds == null && clusterSize != null && clusterSize == storedNodes) {
            // The reported size was passed back unchanged (the UI always sends it), only correct the stored count
            clusterSize = workerNodes;
            if (clusterSize < 1) {
                logAndThrow(Level.WARN, String.format("Scaling failed for Kubernetes cluster : %s, it has no worker nodes", kubernetesCluster.getName()));
            }
        } else if (nodeIds == null && clusterSize != null && clusterSize != workerNodes && workerNodes != storedNodes) {
            logAndThrow(Level.WARN, String.format("Scaling failed for Kubernetes cluster : %s, it records size %d but has %d worker nodes. Scale it to size %d first to correct this, then to the size wanted",
                    kubernetesCluster.getName(), storedNodes, workerNodes, workerNodes));
        }
        if (nodeIds != null) {
            List<KubernetesClusterVmMapVO> nodesToRemove = clusterVMs.stream().filter(vm -> nodeIds.contains(vm.getVmId())).collect(Collectors.toList());
            if (nodesToRemove.size() != nodeIds.size() || nodesToRemove.stream().anyMatch(vm -> vm.isExternalNode() || vm.isControlNode() || vm.isEtcdNode())) {
                logAndThrow(Level.WARN, String.format("Scaling failed for Kubernetes cluster : %s, only CKS-created worker nodes of the cluster can be removed by scaling", kubernetesCluster.getName()));
            }
            clusterSize = workerNodes - nodeIds.size();
            if (clusterSize < 1) {
                logAndThrow(Level.WARN, String.format("Scaling failed for Kubernetes cluster : %s, can not remove all nodes from a cluster", kubernetesCluster.getName()));
            }
        } else if (clusterSize != null && clusterSize < externalNodes) {
            logAndThrow(Level.WARN, String.format("Scaling failed for Kubernetes cluster : %s, size %d is below the %d nodes added with addNodesToKubernetesCluster",
                    kubernetesCluster.getName(), clusterSize, externalNodes));
        }
        if (nodeTypesKnown) {
            syncNodeCounts(clusterVMs, controlNodes, etcdNodes, workerNodes);
        }
    }

    private void syncNodeCounts(List<KubernetesClusterVmMapVO> clusterVMs, long controlNodes, long etcdNodes, long workerNodes) {
        if (controlNodes == kubernetesCluster.getControlNodeCount() && etcdNodes == kubernetesCluster.getEtcdNodeCount()
                && workerNodes == kubernetesCluster.getNodeCount()) {
            return;
        }
        logger.warn("Kubernetes cluster {} records {} control, {} etcd and {} worker nodes but has {}, {} and {}, correcting it", kubernetesCluster,
                kubernetesCluster.getControlNodeCount(), kubernetesCluster.getEtcdNodeCount(), kubernetesCluster.getNodeCount(), controlNodes, etcdNodes, workerNodes);
        KubernetesClusterVO kubernetesClusterVO = kubernetesClusterDao.findById(kubernetesCluster.getId());
        Pair<Long, Long> capacity = getClusterVMsCapacity(clusterVMs);
        if (capacity != null) {
            kubernetesClusterVO.setCores(capacity.first());
            kubernetesClusterVO.setMemory(capacity.second());
        }
        kubernetesClusterVO.setControlNodeCount(controlNodes);
        kubernetesClusterVO.setEtcdNodeCount(etcdNodes);
        kubernetesClusterVO.setNodeCount(workerNodes);
        kubernetesClusterDao.update(kubernetesClusterVO.getId(), kubernetesClusterVO);
        kubernetesCluster = kubernetesClusterVO;
    }

    /**
     * The cores and memory of the cluster's VMs, each by its own offering, or null if that can not be read for any of them
     */
    private Pair<Long, Long> getClusterVMsCapacity(List<KubernetesClusterVmMapVO> clusterVMs) {
        long cores = 0;
        long memory = 0;
        for (KubernetesClusterVmMapVO vmMapVO : clusterVMs) {
            ServiceOffering offering = getVmServiceOffering(vmMapVO.getVmId());
            if (offering == null) {
                logger.warn("Kubernetes cluster {} cores and memory are left unchanged", kubernetesCluster);
                return null;
            }
            cores += offering.getCpu();
            memory += offering.getRamSize();
        }
        return new Pair<>(cores, memory);
    }

    private ServiceOffering getVmServiceOffering(long vmId) {
        UserVmVO userVm = userVmDao.findById(vmId);
        if (userVm == null) {
            logger.warn("Unable to find VM {} of Kubernetes cluster {}", vmId, kubernetesCluster);
            return null;
        }
        ServiceOfferingVO offering = serviceOfferingDao.findByIdIncludingRemoved(userVm.getServiceOfferingId());
        if (offering == null) {
            logger.warn("Unable to find the service offering of VM {} of Kubernetes cluster {}", userVm, kubernetesCluster);
            return null;
        }
        if (offering.isDynamic()) {
            try {
                offering = serviceOfferingDao.findByIdIncludingRemoved(userVm.getId(), userVm.getServiceOfferingId());
            } catch (NumberFormatException e) {
                logger.warn("Unable to read the CPU and memory details of VM {} of Kubernetes cluster {}", userVm, kubernetesCluster, e);
                return null;
            }
        }
        if (offering.getCpu() == null || offering.getRamSize() == null) {
            logger.warn("The service offering of VM {} of Kubernetes cluster {} has no CPU and memory set", userVm, kubernetesCluster);
            return null;
        }
        return offering;
    }

    /**
     * Waits, until the scale timeout, for the cluster's nodes to be Ready after a scale-up, and for
     * each new node to be Ready by name. Existing nodes whose VMs are down (the usual reason a
     * cluster is in Alert) can't become Ready, so they aren't counted. The new nodes are always
     * expected to become Ready, even if a new VM goes down.
     * The count alone isn't enough: Kubernetes keeps reporting the node of a VM that was just
     * stopped as Ready until its node-monitor-grace-period runs out, so that node can make up the
     * count before a new node has joined. The binaries ISO is detached after this returns, and a
     * new node that hasn't joined by then never can.
     */
    protected boolean waitForScaledUpNodesReady(final KubernetesClusterVO cluster, final List<UserVm> newVms) {
        final Set<Long> newVmIds = newVms.stream().map(InternalIdentity::getId).collect(Collectors.toSet());
        final String loginUser = getControlNodeLoginUser();
        if (!KubernetesClusterUtil.validateKubernetesClusterReadyNodesCount(cluster, publicIpAddress, sshPort,
                loginUser, sshKeyFile, () -> cluster.getTotalNodeCount() - getDownNodeCount(newVmIds), scaleTimeoutTime, 15000)) {
            return false;
        }
        for (UserVm vm : newVms) {
            String hostName = vm.getHostName();
            if (StringUtils.isNotEmpty(hostName)) {
                hostName = hostName.toLowerCase();
            }
            if (!KubernetesClusterUtil.isKubernetesClusterNodeReady(cluster, publicIpAddress, sshPort,
                    loginUser, sshKeyFile, hostName, scaleTimeoutTime, 15000)) {
                logger.warn("Node: {} on VM: {} in Kubernetes cluster: {} isn't Ready", hostName, vm, cluster);
                return false;
            }
        }
        return true;
    }

    private void scaleUpKubernetesClusterSize(final long newVmCount) throws CloudRuntimeException {
        enterScalingState(KubernetesCluster.Event.ScaleUpRequested);
        List<UserVm> clusterVMs = new ArrayList<>();
        if (isDefaultTemplateUsed()) {
            LaunchPermissionVO launchPermission = new LaunchPermissionVO(clusterTemplate.getId(), owner.getId());
            launchPermissionDao.persist(launchPermission);
        }
        try {
            createJoinToken(KubernetesClusterService.KubernetesClusterScaleTimeout.value());
        } catch (CloudRuntimeException e) {
            logTransitStateToFailedIfNeededAndThrow(Level.ERROR, String.format("Scaling failed for Kubernetes cluster : %s, unable to create a join token", kubernetesCluster.getName()), e);
        }
        try {
            clusterVMs = provisionKubernetesClusterNodeVms((int)(newVmCount + kubernetesCluster.getNodeCount()), (int)kubernetesCluster.getNodeCount(), publicIpAddress, kubernetesCluster.getDomainId(), kubernetesCluster.getAccountId());
            updateLoginUserDetails(clusterVMs.stream().map(InternalIdentity::getId).collect(Collectors.toList()));
        } catch (CloudRuntimeException | ManagementServerException | ResourceUnavailableException | InsufficientCapacityException e) {
            logTransitStateToFailedIfNeededAndThrow(Level.ERROR, String.format("Scaling failed for Kubernetes cluster : %s, unable to provision node VM in the cluster", kubernetesCluster.getName()), e);
        }
        try {
            List<Long> clusterVMIds = getKubernetesClusterVMMaps().stream().filter(vm -> !vm.isEtcdNode()).map(KubernetesClusterVmMapVO::getVmId).collect(Collectors.toList());
            provisionMissingNodeSshRules(clusterVMIds);
        } catch (ManagementServerException e) {
            logTransitStateToFailedIfNeededAndThrow(Level.ERROR, String.format("Scaling failed for Kubernetes cluster : %s, unable to update network rules", kubernetesCluster.getName()), e);
        }
        attachIsoKubernetesVMs(clusterVMs);
        KubernetesClusterVO kubernetesClusterVO = kubernetesClusterDao.findById(kubernetesCluster.getId());
        kubernetesClusterVO.setNodeCount(clusterSize);
        boolean readyNodesCountValid = waitForScaledUpNodesReady(kubernetesClusterVO, clusterVMs);
        detachIsoKubernetesVMs(clusterVMs);
        deleteJoinToken();
        deleteTemplateLaunchPermission();
        if (!readyNodesCountValid) { // Scaling failed
            logTransitStateToFailedIfNeededAndThrow(Level.ERROR, String.format("Scaling unsuccessful for Kubernetes cluster : %s as it does not have desired number of nodes in ready state", kubernetesCluster.getName()));
        }
    }

    protected void scaleKubernetesClusterSize(KubernetesClusterNodeType nodeType) throws CloudRuntimeException {
        validateKubernetesClusterScaleSizeParameters();
        final long originalClusterSize = kubernetesCluster.getNodeCount();
        final long newVmRequiredCount = clusterSize - originalClusterSize;
        if (KubernetesCluster.State.Created.equals(originalState)) {
            enterScalingState(newVmRequiredCount > 0 ? KubernetesCluster.Event.ScaleUpRequested : KubernetesCluster.Event.ScaleDownRequested);
            kubernetesCluster = updateKubernetesClusterEntryForNodeType(null, nodeType, serviceOfferingNodeTypeMap.get(nodeType.name()), false, false);
            return;
        }
        Pair<String, Integer> publicIpSshPort = getKubernetesClusterServerIpSshPort(null);
        publicIpAddress = publicIpSshPort.first();
        sshPort = publicIpSshPort.second();
        if (StringUtils.isEmpty(publicIpAddress)) {
            logTransitStateToFailedIfNeededAndThrow(Level.ERROR, String.format("Scaling failed for Kubernetes cluster : %s, unable to retrieve associated public IP", kubernetesCluster.getName()));
        }
        if (newVmRequiredCount < 0) { // downscale
            scaleDownKubernetesClusterSize();
        } else { // upscale, same node count handled above
            scaleUpKubernetesClusterSize(newVmRequiredCount);
        }
        boolean updateNodeOffering = serviceOfferingNodeTypeMap.containsKey(nodeType.name());
        ServiceOffering nodeOffering = serviceOfferingNodeTypeMap.getOrDefault(nodeType.name(), null);
        kubernetesCluster = updateKubernetesClusterEntryForNodeType(clusterSize, nodeType, nodeOffering, updateNodeOffering, false);
    }

    private boolean isAutoscalingChanged() {
        if (this.isAutoscalingEnabled == null) {
            return false;
        }
        if (this.isAutoscalingEnabled != kubernetesCluster.getAutoscalingEnabled()) {
            return true;
        }
        if (minSize != null && (!minSize.equals(kubernetesCluster.getMinSize()))) {
            return true;
        }
        return maxSize != null && (!maxSize.equals(kubernetesCluster.getMaxSize()));
    }

    public boolean scaleCluster() throws CloudRuntimeException {
        init();
        if (logger.isInfoEnabled()) {
            logger.info("Scaling Kubernetes cluster {}", kubernetesCluster);
        }
        scaleTimeoutTime = System.currentTimeMillis() + KubernetesClusterService.KubernetesClusterScaleTimeout.value() * 1000;
        // The request was validated for the state it was accepted in, for example what Alert allows,
        // and another operation may have started since
        KubernetesClusterVO currentCluster = kubernetesClusterDao.findById(kubernetesCluster.getId());
        if (currentCluster != null && !currentCluster.getState().equals(originalState)) {
            logAndThrow(Level.WARN, String.format("Scaling Kubernetes cluster : %s failed, it went from %s to %s state after the request was accepted, please retry",
                    kubernetesCluster.getName(), originalState, currentCluster.getState()));
        }

        // Only once the cluster is known to still be in the state the request was accepted in, as this may correct its stored counts
        validateAndSyncWorkerNodeCount();
        final long originalClusterSize = kubernetesCluster.getNodeCount();

        // DEFAULT node type means only the global service offering has been set for the Kubernetes cluster
        boolean scaleClusterDefaultOffering = serviceOfferingNodeTypeMap.containsKey(DEFAULT.name());
        if (scaleClusterDefaultOffering) {
            final ServiceOffering existingServiceOffering = serviceOfferingDao.findById(kubernetesCluster.getServiceOfferingId());
            final ServiceOffering existingControlOffering = serviceOfferingDao.findById(kubernetesCluster.getControlNodeServiceOfferingId());
            final ServiceOffering existingWorkerOffering = serviceOfferingDao.findById(kubernetesCluster.getWorkerNodeServiceOfferingId());
            if (existingServiceOffering == null && ObjectUtils.anyNull(existingControlOffering, existingWorkerOffering)) {
                logAndThrow(Level.ERROR, String.format("Scaling Kubernetes cluster : %s failed, service offering for the Kubernetes cluster not found!", kubernetesCluster.getName()));
            }
        }

        final boolean autoscalingChanged = isAutoscalingChanged();
        ServiceOffering defaultServiceOffering = serviceOfferingNodeTypeMap.getOrDefault(DEFAULT.name(), null);

        for (KubernetesClusterNodeType nodeType : Arrays.asList(CONTROL, ETCD, WORKER)) {
            boolean isWorkerNode = WORKER == nodeType;
            final long newVMRequired = (!isWorkerNode || clusterSize == null) ? 0 : clusterSize - originalClusterSize;
            if (!scaleClusterDefaultOffering && !serviceOfferingNodeTypeMap.containsKey(nodeType.name()) && newVMRequired == 0
                    && !(isWorkerNode && autoscalingChanged)) {
                continue;
            }

            ServiceOffering existingServiceOffering = getExistingServiceOfferingForNodeType(nodeType, kubernetesCluster);
            ServiceOffering scalingServiceOffering = serviceOfferingNodeTypeMap.getOrDefault(nodeType.name(), defaultServiceOffering);
            boolean isNodeOfferingScalingNeeded = isServiceOfferingScalingNeededForNodeType(existingServiceOffering, scalingServiceOffering);
            boolean updateNodeOffering = serviceOfferingNodeTypeMap.containsKey(nodeType.name()) || isNodeOfferingScalingNeeded;

            boolean updateClusterOffering = isWorkerNode && scaleClusterDefaultOffering;
            if (isWorkerNode && autoscalingChanged) {
                boolean autoScaled = autoscaleCluster(this.isAutoscalingEnabled, minSize, maxSize);
                if (autoScaled && isNodeOfferingScalingNeeded) {
                    scaleKubernetesClusterOffering(nodeType, scalingServiceOffering, updateNodeOffering, updateClusterOffering);
                }
                completeScaling();
                return autoScaled;
            }
            final boolean clusterSizeScalingNeeded = isWorkerNode && clusterSize != null && clusterSize != originalClusterSize;
            if (isNodeOfferingScalingNeeded && clusterSizeScalingNeeded) {
                if (newVMRequired > 0) {
                    scaleKubernetesClusterOffering(nodeType, scalingServiceOffering, updateNodeOffering, updateClusterOffering);
                    scaleKubernetesClusterSize(nodeType);
                } else {
                    scaleKubernetesClusterSize(nodeType);
                    scaleKubernetesClusterOffering(nodeType, scalingServiceOffering, updateNodeOffering, updateClusterOffering);
                }
            } else if (isNodeOfferingScalingNeeded) {
                scaleKubernetesClusterOffering(nodeType, scalingServiceOffering, updateNodeOffering, updateClusterOffering);
            } else if (clusterSizeScalingNeeded) {
                scaleKubernetesClusterSize(nodeType);
            }
        }

        completeScaling();
        return true;
    }

    private void completeScaling() {
        // a cluster this worker did not move, such as one never started, may now be held by another operation
        if (isScalingStateEntered()) {
            stateTransitTo(kubernetesCluster.getId(), KubernetesCluster.Event.OperationSucceeded);
        }
    }

    private ServiceOffering getExistingServiceOfferingForNodeType(KubernetesClusterNodeType nodeType, KubernetesCluster kubernetesCluster) {
        Long existingOfferingId = getExistingOfferingIdForNodeType(nodeType, kubernetesCluster);
        if (existingOfferingId == null) {
            logAndThrow(Level.ERROR, String.format("The Kubernetes cluster %s does not have a service offering set for node type %s",
                    kubernetesCluster.getName(), nodeType.name()));
        }
        ServiceOffering existingOffering = serviceOfferingDao.findById(existingOfferingId);
        if (existingOffering == null) {
            logAndThrow(Level.ERROR, String.format("Cannot find service offering with ID %s set on the Kubernetes cluster %s node type %s",
                    existingOfferingId, kubernetesCluster.getName(), nodeType.name()));
        }
        return existingOffering;
    }

    protected void compareExistingToScalingServiceOfferingForNodeType(Long existingOfferingId, Long scalingOfferingId,
                                                                      KubernetesClusterNodeType nodeType) {
        if (existingOfferingId.equals(scalingOfferingId)) {
            String err = String.format("Cannot scale the nodes of type %s as the provided offering %s " +
                    "is the same as the existing offering", nodeType.name(), scalingOfferingId);
            logger.error(err);
            throw new CloudRuntimeException(err);
        }
    }

    protected boolean isServiceOfferingScalingNeededForNodeType(ServiceOffering existingServiceOffering,
                                                                ServiceOffering scalingServiceOffering) {
        return scalingServiceOffering != null && existingServiceOffering != null &&
                scalingServiceOffering.getId() != existingServiceOffering.getId();
    }

    protected Long getExistingOfferingIdForNodeType(KubernetesClusterNodeType nodeType, KubernetesCluster kubernetesCluster) {
        List<KubernetesClusterVmMapVO> clusterVms = listCksNodesOfType(nodeType, kubernetesCluster);
        if (CollectionUtils.isEmpty(clusterVms)) {
            if (WORKER == nodeType && kubernetesCluster.getWorkerNodeServiceOfferingId() != null) {
                return kubernetesCluster.getWorkerNodeServiceOfferingId();
            }
            return kubernetesCluster.getServiceOfferingId();
        }
        KubernetesClusterVmMapVO clusterVm = clusterVms.get(0);
        UserVmVO clusterUserVm = userVmDao.findById(clusterVm.getVmId());
        if (clusterUserVm == null) {
            return kubernetesCluster.getServiceOfferingId();
        }
        return clusterUserVm.getServiceOfferingId();
    }
}
