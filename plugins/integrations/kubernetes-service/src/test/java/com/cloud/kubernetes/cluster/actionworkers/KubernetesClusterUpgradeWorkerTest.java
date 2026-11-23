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

import com.cloud.hypervisor.Hypervisor;
import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDetailsDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.kubernetes.version.KubernetesSupportedVersion;
import com.cloud.kubernetes.cluster.utils.KubernetesClusterUtil;
import com.cloud.network.dao.IPAddressDao;
import com.cloud.network.dao.IPAddressVO;
import com.cloud.network.dao.NetworkDao;
import com.cloud.network.dao.NetworkVO;
import com.cloud.network.rules.FirewallRule;
import com.cloud.network.rules.PortForwardingRuleVO;
import com.cloud.network.rules.dao.PortForwardingRulesDao;
import com.cloud.uservm.UserVm;
import com.cloud.utils.Pair;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.net.Ip;
import com.cloud.utils.ssh.SshHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClusterUpgradeWorkerTest {

    @Mock
    private KubernetesCluster kubernetesCluster;
    @Mock
    private KubernetesSupportedVersion kubernetesSupportedVersion;
    @Mock
    private KubernetesClusterManagerImpl clusterManager;
    @Mock
    private KubernetesClusterVmMapDao kubernetesClusterVmMapDao;
    @Mock
    private KubernetesClusterDao kubernetesClusterDao;
    @Mock
    private NetworkDao networkDao;

    @Mock
    private KubernetesClusterDetailsDao kubernetesClusterDetailsDao;
    @Mock
    private IPAddressDao ipAddressDao;
    @Mock
    private PortForwardingRulesDao portForwardingRulesDao;
    private KubernetesClusterUpgradeWorker worker;

    @Before
    public void setUp() {
        String[] keys = {};
        worker = new KubernetesClusterUpgradeWorker(kubernetesCluster, kubernetesSupportedVersion, clusterManager, keys);
        worker.kubernetesClusterVmMapDao = kubernetesClusterVmMapDao;
    }

    @Test
    public void testFilterOutManualUpgradeNodesFromClusterUpgrade() {
        long controlNodeId = 1L;
        long workerNode1Id = 2L;
        long workerNode2Id = 3L;
        UserVm controlNode = Mockito.mock(UserVm.class);
        Mockito.when(controlNode.getId()).thenReturn(controlNodeId);
        UserVm workerNode1 = Mockito.mock(UserVm.class);
        Mockito.when(workerNode1.getId()).thenReturn(workerNode1Id);
        UserVm workerNode2 = Mockito.mock(UserVm.class);
        Mockito.when(workerNode2.getId()).thenReturn(workerNode2Id);
        KubernetesClusterVmMapVO controlNodeMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        KubernetesClusterVmMapVO workerNode1Map = Mockito.mock(KubernetesClusterVmMapVO.class);
        KubernetesClusterVmMapVO workerNode2Map = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(workerNode2Map.isManualUpgrade()).thenReturn(true);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(controlNodeId)).thenReturn(controlNodeMap);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(workerNode1Id)).thenReturn(workerNode1Map);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(workerNode2Id)).thenReturn(workerNode2Map);
        worker.clusterVMs = Arrays.asList(controlNode, workerNode1, workerNode2);
        worker.filterOutManualUpgradeNodesFromClusterUpgrade();
        Assert.assertEquals(2, worker.clusterVMs.size());
        List<Long> ids = worker.clusterVMs.stream().map(UserVm::getId).collect(Collectors.toList());
        Assert.assertTrue(ids.contains(controlNodeId) && ids.contains(workerNode1Id));
        Assert.assertFalse(ids.contains(workerNode2Id));
    }

    private static final String VERSION_COMMAND = String.format(KubernetesClusterUtil.CLUSTER_NODE_VERSION_COMMAND, "node-1");
    private static final String READY_COMMAND = String.format(KubernetesClusterUpgradeWorker.NODE_STATUS_COMMAND, "node-1");

    private KubernetesClusterUpgradeWorker mockNodeAtVersion(String storedVersion) {
        Mockito.when(kubernetesSupportedVersion.getSemanticVersion()).thenReturn("1.29.2");
        KubernetesClusterVmMapVO vmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(vmMap.getNodeVersion()).thenReturn(storedVersion);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(5L)).thenReturn(vmMap);
        worker.publicIpAddress = "10.0.0.1";
        worker.sshPort = 2222;
        return Mockito.spy(worker);
    }

    private UserVm mockVm() {
        UserVm vm = Mockito.mock(UserVm.class);
        Mockito.when(vm.getId()).thenReturn(5L);
        return vm;
    }

    @Test
    public void testIsNodeUpgradedWhenRecordedAndReported() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.2");
        Mockito.doReturn(new Pair<>(true, "v1.29.2\n")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Mockito.doReturn(new Pair<>(true, "Ready\n")).when(spy).executeSshCommand("10.0.0.1", 2222, READY_COMMAND, 10000, 10000, 20000);
        Assert.assertTrue(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedWhenCordonedAfterUpgrade() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.2");
        Mockito.doReturn(new Pair<>(true, "v1.29.2\n")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Mockito.doReturn(new Pair<>(true, "Ready,SchedulingDisabled\n")).when(spy).executeSshCommand("10.0.0.1", 2222, READY_COMMAND, 10000, 10000, 20000);
        Assert.assertTrue(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedNotWhenNodeNotReady() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.2");
        Mockito.doReturn(new Pair<>(true, "v1.29.2\n")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Mockito.doReturn(new Pair<>(true, "NotReady")).when(spy).executeSshCommand("10.0.0.1", 2222, READY_COMMAND, 10000, 10000, 20000);
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedNotTrustingRecordedVersionAlone() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.2");
        Mockito.doReturn(new Pair<>(true, "v1.28.9")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedWhenNodeUnreachable() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("1.29.2");
        Mockito.doThrow(new RuntimeException("connection refused")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedWhenRecordedAtOldVersion() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.28.9");
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
        Mockito.verify(spy, Mockito.never()).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
    }

    @Test
    public void testIsNodeUpgradedComparesWholeVersion() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.20");
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    private MockedStatic<SshHelper> mockAutoscalerSsh(KubernetesClusterUpgradeWorker spyWorker, Pair<Boolean, String> result) {
        Mockito.doReturn("cloud").when(spyWorker).getControlNodeLoginUser();
        MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class);
        sshHelper.when(() -> SshHelper.sshExecute(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyString(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenReturn(result);
        return sshHelper;
    }

    @Test
    public void testUpdateAutoscalerSkippedWhenAutoscalingDisabled() {
        Mockito.when(kubernetesCluster.getAutoscalingEnabled()).thenReturn(false);
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            worker.updateAutoscaler();
            sshHelper.verifyNoInteractions();
        }
    }

    @Test
    public void testUpdateAutoscalerSkippedWithoutMinMaxSize() {
        Mockito.when(kubernetesCluster.getAutoscalingEnabled()).thenReturn(true);
        Mockito.when(kubernetesCluster.getMinSize()).thenReturn(null);
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            worker.updateAutoscaler();
            sshHelper.verifyNoInteractions();
        }
    }

    @Test
    public void testUpdateAutoscalerRedeploysWithClusterSettings() {
        Mockito.when(kubernetesCluster.getAutoscalingEnabled()).thenReturn(true);
        Mockito.when(kubernetesCluster.getUuid()).thenReturn("cluster-uuid");
        Mockito.when(kubernetesCluster.getMinSize()).thenReturn(1L);
        Mockito.when(kubernetesCluster.getMaxSize()).thenReturn(5L);
        KubernetesClusterUpgradeWorker spyWorker = Mockito.spy(worker);
        try (MockedStatic<SshHelper> sshHelper = mockAutoscalerSsh(spyWorker, new Pair<>(true, ""))) {
            spyWorker.updateAutoscaler();
            sshHelper.verify(() -> SshHelper.sshExecute(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.eq("cloud"),
                    ArgumentMatchers.any(), ArgumentMatchers.any(),
                    ArgumentMatchers.eq("sudo /opt/bin/autoscale-kube-cluster -i cluster-uuid -e -M 5 -m 1"),
                    ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()));
        }
    }

    @Test
    public void testUpdateAutoscalerDoesNotThrowOnFailure() {
        Mockito.when(kubernetesCluster.getAutoscalingEnabled()).thenReturn(true);
        Mockito.when(kubernetesCluster.getUuid()).thenReturn("cluster-uuid");
        Mockito.when(kubernetesCluster.getMinSize()).thenReturn(1L);
        Mockito.when(kubernetesCluster.getMaxSize()).thenReturn(5L);
        KubernetesClusterUpgradeWorker spyWorker = Mockito.spy(worker);
        try (MockedStatic<SshHelper> sshHelper = mockAutoscalerSsh(spyWorker, new Pair<>(false, "autoscale-kube-cluster: not found"))) {
            spyWorker.updateAutoscaler();
            sshHelper.when(() -> SshHelper.sshExecute(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyString(),
                    ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(),
                    ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenThrow(new RuntimeException("ssh failed"));
            spyWorker.updateAutoscaler();
            sshHelper.verify(() -> SshHelper.sshExecute(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyString(),
                    ArgumentMatchers.any(), ArgumentMatchers.any(),
                    ArgumentMatchers.eq("sudo /opt/bin/autoscale-kube-cluster -i cluster-uuid -e -M 5 -m 1"),
                    ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()), Mockito.times(2));
        }
    }

    private KubernetesClusterUpgradeWorker prepareUpgrade(boolean clusterUpdated) {
        long clusterId = 5L;
        Mockito.when(kubernetesCluster.getId()).thenReturn(clusterId);
        Mockito.lenient().when(kubernetesCluster.getNetworkId()).thenReturn(9L);
        Mockito.lenient().when(networkDao.findById(9L)).thenReturn(Mockito.mock(NetworkVO.class));
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(kubernetesClusterDao.findById(clusterId)).thenReturn(clusterVO);
        Mockito.when(kubernetesClusterDao.update(clusterId, clusterVO)).thenReturn(clusterUpdated);
        UserVm controlNode = Mockito.mock(UserVm.class);
        worker.kubernetesClusterDao = kubernetesClusterDao;
        worker.networkDao = networkDao;
        worker.kubernetesClusterDetailsDao = Mockito.mock(KubernetesClusterDetailsDao.class);
        KubernetesClusterUpgradeWorker spy = Mockito.spy(worker);
        Mockito.doNothing().when(spy).init();
        Mockito.doReturn(new Pair<>("10.1.1.10", 2222)).when(spy).getKubernetesClusterServerIpSshPort(null);
        Mockito.doReturn(Collections.singletonList(controlNode)).when(spy).getKubernetesClusterVMs();
        Mockito.doNothing().when(spy).filterOutManualUpgradeNodesFromClusterUpgrade();
        Mockito.doNothing().when(spy).retrieveScriptFiles();
        Mockito.doReturn(true).when(spy).stateTransitTo(ArgumentMatchers.anyLong(), ArgumentMatchers.any(KubernetesCluster.Event.class));
        Mockito.doReturn(true).when(spy).transitToOperationState(ArgumentMatchers.any(KubernetesCluster.Event.class));
        Mockito.doNothing().when(spy).attachIsoKubernetesVMs(ArgumentMatchers.anyList(), ArgumentMatchers.any());
        Mockito.doReturn(Collections.emptyMap()).when(spy).resolveNodeSshIpPorts();
        Mockito.doNothing().when(spy).upgradeKubernetesClusterNodes(ArgumentMatchers.anyMap());
        Mockito.doNothing().when(spy).detachIsoKubernetesVMs(ArgumentMatchers.anyList());
        return spy;
    }

    @Test
    public void testUpgradeRefreshesTheStoredKubeConfigBeforeCompleting() {
        KubernetesClusterUpgradeWorker spy = prepareUpgrade(true);
        Mockito.doReturn(true).when(spy).refreshKubeConfigData(ArgumentMatchers.anyLong());

        Assert.assertTrue(spy.upgradeCluster());

        InOrder inOrder = Mockito.inOrder(spy);
        inOrder.verify(spy).upgradeKubernetesClusterNodes(ArgumentMatchers.anyMap());
        inOrder.verify(spy).refreshKubeConfigData(ArgumentMatchers.anyLong());
        inOrder.verify(spy).stateTransitTo(5L, KubernetesCluster.Event.OperationSucceeded);
        // the kubeconfig must point at the cluster's public endpoint
        Assert.assertEquals("10.1.1.10", spy.apiPublicIpAddress);
    }

    @Test
    public void testUpgradeSucceedsWhenTheKubeConfigRefreshFails() {
        KubernetesClusterUpgradeWorker spy = prepareUpgrade(true);
        Mockito.doReturn(false).when(spy).refreshKubeConfigData(ArgumentMatchers.anyLong());

        Assert.assertTrue(spy.upgradeCluster());

        Mockito.verify(spy).stateTransitTo(5L, KubernetesCluster.Event.OperationSucceeded);
        Mockito.verify(spy, Mockito.never()).stateTransitTo(5L, KubernetesCluster.Event.OperationFailed);
    }

    @Test
    public void testFailedUpgradeDoesNotRefreshTheKubeConfig() {
        KubernetesClusterUpgradeWorker spy = prepareUpgrade(false);

        Assert.assertFalse(spy.upgradeCluster());

        Mockito.verify(spy, Mockito.never()).refreshKubeConfigData(ArgumentMatchers.anyLong());
        Mockito.verify(spy).stateTransitTo(5L, KubernetesCluster.Event.OperationFailed);
    }

    private static final String SSH_IP = "203.0.113.10";
    private static final long SSH_IP_ID = 5L;

    private UserVm mockNode(long id, String hostName) {
        UserVm vm = Mockito.mock(UserVm.class);
        Mockito.when(vm.getId()).thenReturn(id);
        Mockito.when(vm.getHostName()).thenReturn(hostName);
        return vm;
    }

    private PortForwardingRuleVO sshRule(long vmId, int publicPort) {
        PortForwardingRuleVO rule = new PortForwardingRuleVO(null, SSH_IP_ID, publicPort, new Ip("10.1.1.10"), 22, "tcp", 1L, 1L, 1L, vmId);
        rule.setState(FirewallRule.State.Active);
        return rule;
    }

    private KubernetesClusterUpgradeWorker prepareUpgradeWorker(List<UserVm> vms, List<PortForwardingRuleVO> rules) throws Exception {
        worker.kubernetesClusterDetailsDao = kubernetesClusterDetailsDao;
        worker.ipAddressDao = ipAddressDao;
        worker.portForwardingRulesDao = portForwardingRulesDao;
        worker.publicIpAddress = SSH_IP;
        worker.sshPort = KubernetesClusterActionWorker.CLUSTER_NODES_DEFAULT_START_SSH_PORT;
        worker.clusterVMs = vms;
        Mockito.when(kubernetesCluster.getZoneId()).thenReturn(1L);
        IPAddressVO address = Mockito.mock(IPAddressVO.class);
        Mockito.when(address.getId()).thenReturn(SSH_IP_ID);
        Mockito.when(ipAddressDao.findByIpAndDcId(1L, SSH_IP)).thenReturn(address);
        Mockito.when(portForwardingRulesDao.listByIpAndNotRevoked(SSH_IP_ID)).thenReturn(rules);
        java.lang.reflect.Field timeout = KubernetesClusterUpgradeWorker.class.getDeclaredField("upgradeTimeoutTime");
        timeout.setAccessible(true);
        timeout.setLong(worker, System.currentTimeMillis() + 3600 * 1000L);
        java.lang.reflect.Field script = KubernetesClusterUpgradeWorker.class.getDeclaredField("upgradeScriptFile");
        script.setAccessible(true);
        script.set(worker, new File("upgrade-k8s.sh"));
        KubernetesClusterUpgradeWorker spy = Mockito.spy(worker);
        Mockito.doReturn("cloud").when(spy).getControlNodeLoginUser();
        return spy;
    }

    @Test
    public void testUpgradeReachesEachNodeOnItsPortForwardingRule() throws Exception {
        UserVm control = mockNode(10L, "cks-control");
        UserVm worker1 = mockNode(11L, "cks-node-1");
        UserVm external = mockNode(13L, "cks-external");
        Mockito.when(control.getHypervisorType()).thenReturn(Hypervisor.HypervisorType.KVM);
        Mockito.when(worker1.getHypervisorType()).thenReturn(Hypervisor.HypervisorType.KVM);
        Mockito.when(external.getHypervisorType()).thenReturn(Hypervisor.HypervisorType.KVM);
        Mockito.when(kubernetesSupportedVersion.getSemanticVersion()).thenReturn("1.30.2");
        KubernetesClusterUpgradeWorker spy = prepareUpgradeWorker(Arrays.asList(control, worker1, external),
                Arrays.asList(sshRule(10L, 2222), sshRule(11L, 2223), sshRule(13L, 2225)));
        Mockito.doReturn(true).when(spy).deployProvider();
        Mockito.doReturn(null).when(spy).getManagementServerSshPublicKeyFile();
        try (MockedStatic<SshHelper> ssh = Mockito.mockStatic(SshHelper.class);
             MockedStatic<KubernetesClusterUtil> util = Mockito.mockStatic(KubernetesClusterUtil.class)) {
            ssh.when(() -> SshHelper.sshExecute(ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                    ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()))
                    .thenReturn(new Pair<>(true, ""));
            util.when(() -> KubernetesClusterUtil.uncordonKubernetesClusterNode(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyInt(),
                    ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt())).thenReturn(true);
            util.when(() -> KubernetesClusterUtil.isKubernetesClusterNodeReady(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyInt(),
                    ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyInt())).thenReturn(true);
            util.when(() -> KubernetesClusterUtil.clusterNodeVersionMatches(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyInt(),
                    ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(),
                    ArgumentMatchers.anyLong(), ArgumentMatchers.any())).thenReturn(true);

            spy.upgradeKubernetesClusterNodes(spy.resolveNodeSshIpPorts());

            for (int port : new int[]{2222, 2223, 2225}) {
                ssh.verify(() -> SshHelper.scpTo(ArgumentMatchers.eq(SSH_IP), ArgumentMatchers.eq(port), ArgumentMatchers.any(), ArgumentMatchers.any(),
                        ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()));
                ssh.verify(() -> SshHelper.sshExecute(ArgumentMatchers.eq(SSH_IP), ArgumentMatchers.eq(port), ArgumentMatchers.any(), ArgumentMatchers.any(),
                        ArgumentMatchers.any(), ArgumentMatchers.startsWith("sudo ./upgrade-k8s.sh"), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()));
            }
            ssh.verify(() -> SshHelper.scpTo(ArgumentMatchers.anyString(), ArgumentMatchers.eq(2224), ArgumentMatchers.any(), ArgumentMatchers.any(),
                    ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()), Mockito.never());
        }
    }

    @Test
    public void testUpgradeFailsBeforeAnyChangeWhenANodeHasNoSshRule() throws Exception {
        UserVm control = mockNode(10L, "cks-control");
        UserVm external = mockNode(13L, "cks-external");
        List<UserVm> vms = Arrays.asList(control, external);
        KubernetesClusterUpgradeWorker spy = prepareUpgradeWorker(vms, List.of(sshRule(10L, 2222)));
        Mockito.doNothing().when(spy).init();
        Mockito.doReturn(new Pair<>(SSH_IP, KubernetesClusterActionWorker.CLUSTER_NODES_DEFAULT_START_SSH_PORT)).when(spy).getKubernetesClusterServerIpSshPort(null);
        Mockito.doReturn(vms).when(spy).getKubernetesClusterVMs();
        KubernetesClusterVmMapVO nodeMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(ArgumentMatchers.anyLong())).thenReturn(nodeMap);
        try (MockedStatic<SshHelper> ssh = Mockito.mockStatic(SshHelper.class)) {
            try {
                spy.upgradeCluster();
                Assert.fail("Expected a CloudRuntimeException");
            } catch (CloudRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("cks-external"));
            }
            ssh.verifyNoInteractions();
            Mockito.verify(spy, Mockito.never()).stateTransitTo(ArgumentMatchers.anyLong(), ArgumentMatchers.any());
            Mockito.verify(spy, Mockito.never()).transitToOperationState(ArgumentMatchers.any());
            Mockito.verify(spy, Mockito.never()).attachIsoKubernetesVMs(ArgumentMatchers.any(), ArgumentMatchers.any());
        }
    }
}
