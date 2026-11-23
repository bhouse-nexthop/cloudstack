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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.framework.config.dao.ConfigurationDao;
import org.apache.commons.io.IOUtils;
import org.apache.cloudstack.userdata.UserDataManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.cloud.exception.InvalidParameterValueException;
import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterDetailsVO;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDetailsDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.kubernetes.version.KubernetesSupportedVersionVO;
import com.cloud.kubernetes.version.dao.KubernetesSupportedVersionDao;
import com.cloud.network.IpAddress;
import com.cloud.network.Network;
import com.cloud.network.dao.IPAddressDao;
import com.cloud.utils.ssh.SshHelper;
import com.cloud.network.dao.IPAddressVO;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.network.rules.PortForwardingRuleVO;
import com.cloud.network.rules.dao.PortForwardingRulesDao;
import com.cloud.uservm.UserVm;
import com.cloud.utils.Pair;
import com.cloud.utils.net.Ip;
import com.cloud.vm.UserVmVO;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClusterActionWorkerTest {

    @Mock
    KubernetesClusterDao kubernetesClusterDao;

    @Mock
    KubernetesClusterVmMapDao kubernetesClusterVmMapDao;

    @Mock
    KubernetesClusterDetailsDao kubernetesClusterDetailsDao;

    @Mock
    KubernetesSupportedVersionDao kubernetesSupportedVersionDao;

    @Mock
    KubernetesClusterManagerImpl kubernetesClusterManager;

    @Mock
    IPAddressDao ipAddressDao;

    @Mock
    PortForwardingRulesDao portForwardingRulesDao;

    KubernetesCluster kubernetesCluster;

    @Mock
    ConfigurationDao configurationDao;

    KubernetesClusterActionWorker actionWorker = null;

    final static Long DEFAULT_ID = 1L;

    @Before
    public void setUp() throws Exception {
        kubernetesClusterManager.kubernetesClusterDao = kubernetesClusterDao;
        kubernetesClusterManager.kubernetesSupportedVersionDao = kubernetesSupportedVersionDao;
        kubernetesClusterManager.kubernetesClusterDetailsDao = kubernetesClusterDetailsDao;
        kubernetesClusterManager.kubernetesClusterVmMapDao = kubernetesClusterVmMapDao;
        kubernetesCluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(kubernetesCluster.getId()).thenReturn(DEFAULT_ID);
        actionWorker = new KubernetesClusterActionWorker(kubernetesCluster, kubernetesClusterManager);
        actionWorker.ipAddressDao = ipAddressDao;
        actionWorker.configurationDao = configurationDao;
        actionWorker.portForwardingRulesDao = portForwardingRulesDao;
    }

    @Test
    public void testDeleteJoinTokenOnlyOnce() {
        KubernetesClusterActionWorker worker = Mockito.spy(actionWorker);
        Mockito.doReturn(new Pair<>("10.1.1.1", 2222)).when(worker).getKubernetesClusterServerIpSshPort(Mockito.nullable(UserVm.class));
        Mockito.doReturn("cloud").when(worker).getControlNodeLoginUser();
        Mockito.doReturn(new File("id_rsa")).when(worker).getManagementServerSshPublicKeyFile();
        worker.joinToken = "abcdef.0123456789abcdef";
        worker.joinCaCertHash = "sha256:9826a516d7c2f8974dd0a09a64048c5cba6ff67780b01568b13cd3acf92a409f";
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            sshHelper.when(() -> SshHelper.sshExecute(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.any(),
                    Mockito.any(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt()))
                    .thenReturn(new Pair<>(true, ""));
            worker.deleteJoinToken();
            worker.deleteJoinToken();
            sshHelper.verify(() -> SshHelper.sshExecute(Mockito.eq("10.1.1.1"), Mockito.eq(2222), Mockito.eq("cloud"), Mockito.any(),
                    Mockito.any(), Mockito.eq("sudo /opt/bin/kubeadm token delete abcdef"), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt()));
            sshHelper.verifyNoMoreInteractions();
        }
        Assert.assertNull(worker.joinToken);
        Assert.assertNull(worker.joinCaCertHash);
    }

    @Test
    public void testDeleteJoinTokenControlNodeUnreachable() {
        KubernetesClusterActionWorker worker = Mockito.spy(actionWorker);
        Mockito.doThrow(new CloudRuntimeException("no control node")).when(worker).getKubernetesClusterServerIpSshPort(Mockito.nullable(UserVm.class));
        worker.joinToken = "abcdef.0123456789abcdef";
        worker.deleteJoinToken();
        Assert.assertNull(worker.joinToken);
    }

    @Test
    public void testGetVpcTierKubernetesPublicIpNullDetail() {
        IpAddress result = actionWorker.getVpcTierKubernetesPublicIp(Mockito.mock(Network.class));
        Assert.assertNull(result);
    }

    private String mockClusterPublicIpDetail(boolean isNull) {
        String uuid = isNull ? null : UUID.randomUUID().toString();
        KubernetesClusterDetailsVO detailsVO = new KubernetesClusterDetailsVO(DEFAULT_ID, ApiConstants.PUBLIC_IP_ID, uuid, false);
        Mockito.when(kubernetesClusterDetailsDao.findDetail(DEFAULT_ID, ApiConstants.PUBLIC_IP_ID)).thenReturn(detailsVO);
        return uuid;
    }

    @Test
    public void testGetVpcTierKubernetesPublicIpNullDetailValue() {
        mockClusterPublicIpDetail(true);
        IpAddress result = actionWorker.getVpcTierKubernetesPublicIp(Mockito.mock(Network.class));
        Assert.assertNull(result);
    }

    private Network mockNetworkForGetVpcTierKubernetesPublicIpTest() {
        Network network = Mockito.mock(Network.class);
        Mockito.when(network.getVpcId()).thenReturn(DEFAULT_ID);
        return network;
    }

    @Test
    public void testGetVpcTierKubernetesPublicIpNullVpc() {
        String uuid = mockClusterPublicIpDetail(false);
        IPAddressVO address = Mockito.mock(IPAddressVO.class);
        Mockito.when(ipAddressDao.findByUuid(uuid)).thenReturn(address);
        IpAddress result = actionWorker.getVpcTierKubernetesPublicIp(mockNetworkForGetVpcTierKubernetesPublicIpTest());
        Assert.assertNull(result);
    }

    @Test
    public void testGetVpcTierKubernetesPublicIpDifferentVpc() {
        String uuid = mockClusterPublicIpDetail(false);
        IPAddressVO address = Mockito.mock(IPAddressVO.class);
        Mockito.when(address.getVpcId()).thenReturn(2L);
        Mockito.when(ipAddressDao.findByUuid(uuid)).thenReturn(address);
        IpAddress result = actionWorker.getVpcTierKubernetesPublicIp(mockNetworkForGetVpcTierKubernetesPublicIpTest());
        Assert.assertNull(result);
    }

    @Test
    public void testGetVpcTierKubernetesPublicIpValid() {
        String uuid = mockClusterPublicIpDetail(false);
        IPAddressVO address = Mockito.mock(IPAddressVO.class);
        Mockito.when(address.getVpcId()).thenReturn(DEFAULT_ID);
        Mockito.when(ipAddressDao.findByUuid(uuid)).thenReturn(address);
        IpAddress result = actionWorker.getVpcTierKubernetesPublicIp(mockNetworkForGetVpcTierKubernetesPublicIpTest());
        Assert.assertNotNull(result);
    }

    private KubernetesClusterVO mockClusterInState(KubernetesCluster.State state) {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(cluster.getState()).thenReturn(state);
        Mockito.when(kubernetesClusterDao.findById(DEFAULT_ID)).thenReturn(cluster);
        return cluster;
    }

    @Test
    public void testTransitToOperationStateMovesCluster() {
        KubernetesClusterVO cluster = mockClusterInState(KubernetesCluster.State.Running);
        Mockito.when(kubernetesClusterDao.updateState(KubernetesCluster.State.Running, KubernetesCluster.Event.UpgradeRequested,
                KubernetesCluster.State.Upgrading, cluster, null)).thenReturn(true);
        Assert.assertTrue(actionWorker.transitToOperationState(KubernetesCluster.Event.UpgradeRequested));
    }

    @Test(expected = CloudRuntimeException.class)
    public void testTransitToOperationStateFailsWhenAnotherOperationMovedClusterFirst() {
        KubernetesClusterVO cluster = mockClusterInState(KubernetesCluster.State.Running);
        Mockito.when(kubernetesClusterDao.updateState(KubernetesCluster.State.Running, KubernetesCluster.Event.StopRequested,
                KubernetesCluster.State.Stopping, cluster, null)).thenReturn(false);
        // the compare-and-set misses even where the operation could go on without a transition: the other operation owns the state now
        actionWorker.transitToOperationState(KubernetesCluster.Event.StopRequested, KubernetesCluster.State.Running);
    }

    @Test(expected = CloudRuntimeException.class)
    public void testTransitToOperationStateFailsWithoutTransitionInOtherStates() {
        mockClusterInState(KubernetesCluster.State.Scaling);
        actionWorker.transitToOperationState(KubernetesCluster.Event.DestroyRequested, KubernetesCluster.State.Created, KubernetesCluster.State.Destroying);
    }

    @Test
    public void testTransitToOperationStateGoesOnWithoutTransitionInGivenStates() {
        mockClusterInState(KubernetesCluster.State.Destroying);
        Assert.assertFalse(actionWorker.transitToOperationState(KubernetesCluster.Event.DestroyRequested, KubernetesCluster.State.Created, KubernetesCluster.State.Destroying));
        Mockito.verify(kubernetesClusterDao, Mockito.never()).updateState(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testStateTransitToFollowsTheStatesTheOperationMovesTheClusterTo() {
        KubernetesClusterVO cluster = mockClusterInState(KubernetesCluster.State.Running);
        Mockito.when(kubernetesClusterDao.updateState(Mockito.eq(KubernetesCluster.State.Running), Mockito.eq(KubernetesCluster.Event.UpgradeRequested),
                Mockito.eq(KubernetesCluster.State.Upgrading), Mockito.eq(cluster), Mockito.isNull())).thenAnswer(invocation -> {
                    Mockito.when(cluster.getState()).thenReturn(KubernetesCluster.State.Upgrading);
                    return true;
                });
        Mockito.when(kubernetesClusterDao.updateState(Mockito.eq(KubernetesCluster.State.Upgrading), Mockito.eq(KubernetesCluster.Event.OperationFailed),
                Mockito.eq(KubernetesCluster.State.Alert), Mockito.eq(cluster), Mockito.isNull())).thenAnswer(invocation -> {
                    Mockito.when(cluster.getState()).thenReturn(KubernetesCluster.State.Alert);
                    return true;
                });
        Mockito.when(kubernetesClusterDao.updateState(KubernetesCluster.State.Alert, KubernetesCluster.Event.OperationSucceeded,
                KubernetesCluster.State.Running, cluster, null)).thenReturn(true);

        actionWorker.transitToOperationState(KubernetesCluster.Event.UpgradeRequested);
        Assert.assertTrue(actionWorker.stateTransitTo(DEFAULT_ID, KubernetesCluster.Event.OperationFailed));
        // a step that failed and was retried successfully still completes the operation
        Assert.assertTrue(actionWorker.stateTransitTo(DEFAULT_ID, KubernetesCluster.Event.OperationSucceeded));
    }

    @Test
    public void testStateTransitToLeavesClusterAnotherOperationMovedOn() {
        KubernetesClusterVO cluster = mockClusterInState(KubernetesCluster.State.Running);
        Mockito.when(kubernetesClusterDao.updateState(Mockito.eq(KubernetesCluster.State.Running), Mockito.eq(KubernetesCluster.Event.UpgradeRequested),
                Mockito.eq(KubernetesCluster.State.Upgrading), Mockito.eq(cluster), Mockito.isNull())).thenAnswer(invocation -> {
                    Mockito.when(cluster.getState()).thenReturn(KubernetesCluster.State.Upgrading);
                    return true;
                });
        actionWorker.transitToOperationState(KubernetesCluster.Event.UpgradeRequested);
        Mockito.when(cluster.getState()).thenReturn(KubernetesCluster.State.Scaling);

        Assert.assertFalse(actionWorker.stateTransitTo(DEFAULT_ID, KubernetesCluster.Event.OperationFailed));
    }

    @Test
    public void testStateTransitToLeavesClusterTheOperationWentOnWithoutMoving() {
        mockClusterInState(KubernetesCluster.State.Error);
        Assert.assertFalse(actionWorker.transitToOperationState(KubernetesCluster.Event.StopRequested, KubernetesCluster.State.Error));
        Assert.assertFalse(actionWorker.stateTransitTo(DEFAULT_ID, KubernetesCluster.Event.OperationSucceeded));
        Mockito.verify(kubernetesClusterDao, Mockito.never()).updateState(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testStartTakesClusterInAlert() {
        KubernetesClusterVO cluster = mockClusterInState(KubernetesCluster.State.Alert);
        Mockito.when(kubernetesClusterDao.updateState(KubernetesCluster.State.Alert, KubernetesCluster.Event.StartRequested,
                KubernetesCluster.State.Starting, cluster, null)).thenReturn(true);
        Assert.assertTrue(actionWorker.transitToOperationState(KubernetesCluster.Event.StartRequested));
    }

    @Test
    public void testStateTransitToKeepsTheStateAfterAnEventWithoutTransition() {
        KubernetesClusterVO cluster = mockClusterInState(KubernetesCluster.State.Running);
        Mockito.when(kubernetesClusterDao.updateState(Mockito.eq(KubernetesCluster.State.Running), Mockito.eq(KubernetesCluster.Event.ScaleUpRequested),
                Mockito.eq(KubernetesCluster.State.Scaling), Mockito.eq(cluster), Mockito.isNull())).thenAnswer(invocation -> {
                    Mockito.when(cluster.getState()).thenReturn(KubernetesCluster.State.Scaling);
                    return true;
                });
        Mockito.when(kubernetesClusterDao.updateState(KubernetesCluster.State.Scaling, KubernetesCluster.Event.OperationFailed,
                KubernetesCluster.State.Alert, cluster, null)).thenReturn(true);

        actionWorker.transitToOperationState(KubernetesCluster.Event.ScaleUpRequested);
        Assert.assertFalse(actionWorker.stateTransitTo(DEFAULT_ID, KubernetesCluster.Event.CreateFailed));
        Assert.assertTrue(actionWorker.stateTransitTo(DEFAULT_ID, KubernetesCluster.Event.OperationFailed));
    }

    private UserVm mockNodeVm() {
        UserVm vm = Mockito.mock(UserVm.class);
        Mockito.when(vm.getHostName()).thenReturn("Node-1");
        return vm;
    }

    private static final String DRAIN_COMMAND = "sudo /opt/bin/kubectl drain node-1 --ignore-daemonsets --delete-emptydir-data --timeout=300s";

    @Test
    public void testDrainKubernetesClusterNodeRetriesUntilSuccess() throws Exception {
        KubernetesClusterActionWorker worker = Mockito.spy(actionWorker);
        UserVm vm = mockNodeVm();
        Mockito.doReturn(new Pair<>(false, "Cannot evict pod as it would violate the pod's disruption budget"), new Pair<>(true, "node/node-1 drained"))
                .when(worker).executeSshCommand("10.0.0.1", 2222, DRAIN_COMMAND, 10000, 10000, 360000);
        Mockito.doReturn(true).when(worker).sleepBeforeRetry(Mockito.anyLong());

        Pair<Boolean, String> result = worker.drainKubernetesClusterNode("10.0.0.1", 2222, vm, 3, 5, Long.MAX_VALUE);

        Assert.assertTrue(result.first());
        Assert.assertEquals("node/node-1 drained", result.second());
        Mockito.verify(worker, Mockito.times(2)).executeSshCommand("10.0.0.1", 2222, DRAIN_COMMAND, 10000, 10000, 360000);
        Mockito.verify(worker, Mockito.times(1)).sleepBeforeRetry(5);
    }

    @Test
    public void testDrainKubernetesClusterNodeUsesEveryAttempt() throws Exception {
        KubernetesClusterActionWorker worker = Mockito.spy(actionWorker);
        UserVm vm = mockNodeVm();
        Mockito.doThrow(new RuntimeException("connection refused")).doReturn(new Pair<>(false, "pdb"))
                .when(worker).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(true).when(worker).sleepBeforeRetry(Mockito.anyLong());

        Pair<Boolean, String> result = worker.drainKubernetesClusterNode("10.0.0.1", 2222, vm, 3, 5, Long.MAX_VALUE);

        Assert.assertFalse(result.first());
        Assert.assertEquals("pdb", result.second());
        Mockito.verify(worker, Mockito.times(3)).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.verify(worker, Mockito.times(2)).sleepBeforeRetry(5);
        // The caller uncordons the node, as only it knows whether it cordoned it
        Mockito.verify(worker, Mockito.never()).uncordonKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void testDrainKubernetesClusterNodeStopsRetryingAtDeadline() throws Exception {
        KubernetesClusterActionWorker worker = Mockito.spy(actionWorker);
        UserVm vm = mockNodeVm();
        Mockito.doReturn(new Pair<>(false, "timed out"))
                .when(worker).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());

        // 90 seconds left: one attempt, with kubectl's timeout cut to what is left after the SSH margin
        Pair<Boolean, String> result = worker.drainKubernetesClusterNode("10.0.0.1", 2222, vm, 3, 60000, System.currentTimeMillis() + 90500);

        Assert.assertFalse(result.first());
        Mockito.verify(worker, Mockito.times(1)).executeSshCommand(Mockito.eq("10.0.0.1"), Mockito.eq(2222),
                Mockito.matches(".*--timeout=(29|30)s"), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.verify(worker, Mockito.never()).sleepBeforeRetry(Mockito.anyLong());
    }

    @Test
    public void testDrainKubernetesClusterNodeWithNoTimeLeft() throws Exception {
        KubernetesClusterActionWorker worker = Mockito.spy(actionWorker);
        UserVm vm = mockNodeVm();

        Pair<Boolean, String> result = worker.drainKubernetesClusterNode("10.0.0.1", 2222, vm, 3, 60000, System.currentTimeMillis() + 30000);

        Assert.assertFalse(result.first());
        Assert.assertTrue(result.second().contains("timed out"));
        Mockito.verify(worker, Mockito.never()).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
    }

    @Test
    public void testDrainKubernetesClusterNodeStopsRetryingWhenInterrupted() throws Exception {
        KubernetesClusterActionWorker worker = Mockito.spy(actionWorker);
        UserVm vm = mockNodeVm();
        Mockito.doReturn(new Pair<>(false, "pdb"))
                .when(worker).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(false).when(worker).sleepBeforeRetry(Mockito.anyLong());

        Assert.assertFalse(worker.drainKubernetesClusterNode("10.0.0.1", 2222, vm, 3, 5, Long.MAX_VALUE).first());
        Mockito.verify(worker, Mockito.times(1)).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
    }

    @Test
    public void testGetNodeSshAddressIgnoresRevokedRule() {
        actionWorker.publicIpAddress = "10.0.0.1";
        actionWorker.sshPort = 2222;
        UserVm vm = Mockito.mock(UserVm.class);
        Mockito.when(vm.getId()).thenReturn(7L);
        mockIpAddress(1L, "10.0.0.1");
        PortForwardingRuleVO revoked = Mockito.mock(PortForwardingRuleVO.class);
        Mockito.when(revoked.getState()).thenReturn(com.cloud.network.rules.FirewallRule.State.Revoke);
        PortForwardingRuleVO active = mockSshRule(1L, 2226);
        Mockito.when(portForwardingRulesDao.listByVm(7L)).thenReturn(List.of(revoked, active));

        Assert.assertEquals(Integer.valueOf(2226), actionWorker.getNodeSshAddress(vm, 1).second());
    }

    private PortForwardingRuleVO mockSshRule(long ipId, int sourcePort) {
        PortForwardingRuleVO rule = Mockito.mock(PortForwardingRuleVO.class);
        Mockito.when(rule.getDestinationPortStart()).thenReturn(22);
        Mockito.when(rule.getSourcePortStart()).thenReturn(sourcePort);
        Mockito.when(rule.getSourceIpAddressId()).thenReturn(ipId);
        return rule;
    }

    private void mockIpAddress(long id, String address) {
        IPAddressVO ip = Mockito.mock(IPAddressVO.class);
        Mockito.when(ip.getAddress()).thenReturn(new Ip(address));
        Mockito.when(ipAddressDao.findById(id)).thenReturn(ip);
    }

    @Test
    public void testGetNodeSshAddressUsesPortForwardingRule() {
        actionWorker.publicIpAddress = "10.0.0.1";
        actionWorker.sshPort = 2222;
        UserVm vm = Mockito.mock(UserVm.class);
        Mockito.when(vm.getId()).thenReturn(7L);
        mockIpAddress(1L, "10.0.0.1");
        mockIpAddress(2L, "10.0.0.2");
        List<PortForwardingRuleVO> rules = List.of(mockSshRule(2L, 2230), mockSshRule(1L, 2225));
        Mockito.when(portForwardingRulesDao.listByVm(7L)).thenReturn(rules);

        Pair<String, Integer> address = actionWorker.getNodeSshAddress(vm, 1);

        Assert.assertEquals("10.0.0.1", address.first());
        Assert.assertEquals(Integer.valueOf(2225), address.second());
    }

    @Test
    public void testGetNodeSshAddressWithoutRuleFallsBackToIndex() {
        actionWorker.publicIpAddress = "10.0.0.1";
        actionWorker.sshPort = 2222;
        UserVm vm = Mockito.mock(UserVm.class);
        Mockito.when(vm.getId()).thenReturn(7L);
        Mockito.when(portForwardingRulesDao.listByVm(7L)).thenReturn(Collections.emptyList());

        Pair<String, Integer> address = actionWorker.getNodeSshAddress(vm, 2);

        Assert.assertEquals("10.0.0.1", address.first());
        Assert.assertEquals(Integer.valueOf(2224), address.second());
    }

    @Test
    public void testGetNodeSshAddressOnSharedNetwork() {
        actionWorker.publicIpAddress = "10.0.0.1";
        actionWorker.sshPort = 22;
        UserVm vm = Mockito.mock(UserVm.class);
        Mockito.when(vm.getPrivateIpAddress()).thenReturn("192.168.0.5");

        Assert.assertEquals(new Pair<>("10.0.0.1", 22), actionWorker.getNodeSshAddress(vm, 0));
        Assert.assertEquals(new Pair<>("192.168.0.5", 22), actionWorker.getNodeSshAddress(vm, 1));
        Mockito.verifyNoInteractions(portForwardingRulesDao);
    }

    private void mockExternalCloudProviderDetail(String value) {
        KubernetesClusterDetailsVO detail = value == null ? null :
                new KubernetesClusterDetailsVO(DEFAULT_ID, KubernetesClusterActionWorker.EXTERNAL_CLOUD_PROVIDER_DETAIL, value, false);
        Mockito.when(kubernetesClusterDetailsDao.findDetail(DEFAULT_ID, KubernetesClusterActionWorker.EXTERNAL_CLOUD_PROVIDER_DETAIL)).thenReturn(detail);
    }

    @Test
    public void testReplaceKubeletCloudProviderArgEnabled() {
        mockExternalCloudProviderDetail("true");
        String config = "KUBELET_EXTRA_ARGS=--cgroup-driver=systemd" + KubernetesClusterActionWorker.KUBELET_CLOUD_PROVIDER_ARG_KEY;
        String result = actionWorker.replaceKubeletCloudProviderArg(config);
        Assert.assertTrue(result.startsWith("KUBELET_EXTRA_ARGS=--cgroup-driver=systemd --cloud-provider=external"));
        Assert.assertFalse(result.contains("{{"));
    }

    @Test
    public void testReplaceKubeletCloudProviderArgClusterWithoutDetail() {
        mockExternalCloudProviderDetail(null);
        String config = "KUBELET_EXTRA_ARGS=--cgroup-driver=systemd" + KubernetesClusterActionWorker.KUBELET_CLOUD_PROVIDER_ARG_KEY;
        Assert.assertEquals("KUBELET_EXTRA_ARGS=--cgroup-driver=systemd", actionWorker.replaceKubeletCloudProviderArg(config));
    }

    @Test
    public void testIsExternalCloudProviderEnabledFalseValue() {
        mockExternalCloudProviderDetail("false");
        Assert.assertFalse(actionWorker.isExternalCloudProviderEnabled());
    }

    @Test
    public void testValidateNodeUserDataLengthUnderLimit() {
        actionWorker.validateNodeUserDataLength(StringUtils.repeat('a', UserDataManager.VM_USERDATA_MAX_LENGTH.value() - 1), KubernetesClusterNodeType.CONTROL);
    }

    @Test
    public void testValidateNodeUserDataLengthAtLimit() {
        actionWorker.validateNodeUserDataLength(StringUtils.repeat('a', UserDataManager.VM_USERDATA_MAX_LENGTH.value()), KubernetesClusterNodeType.WORKER);
    }

    @Test
    public void testValidateNodeUserDataLengthOverLimit() {
        Mockito.when(kubernetesCluster.getName()).thenReturn("test-cluster");
        int limit = UserDataManager.VM_USERDATA_MAX_LENGTH.value();
        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class, () ->
                actionWorker.validateNodeUserDataLength(StringUtils.repeat('a', limit + 1), KubernetesClusterNodeType.ETCD));
        Assert.assertEquals(String.format("User data for etcd node of Kubernetes cluster test-cluster is %d bytes after base64 encoding, over vm.userdata.max.length (%d). Raise vm.userdata.max.length.",
                limit + 1, limit), e.getMessage());
    }

    @Test
    public void testLogTransitStateAndThrowKeepsInvalidParameterMessage() {
        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () ->
                actionWorker.logTransitStateAndThrow(Level.ERROR, "Provisioning failed", null, null, new InvalidParameterValueException("User data is too big")));
        Assert.assertEquals("Provisioning failed: User data is too big", e.getMessage());
    }

    @Test
    public void testLogTransitStateAndThrowDropsOtherCauseMessage() {
        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () ->
                actionWorker.logTransitStateAndThrow(Level.ERROR, "Provisioning failed", null, null, new CloudRuntimeException("internal detail")));
        Assert.assertEquals("Provisioning failed", e.getMessage());
    }

    @Test
    public void testLogTransitStateAndThrowDoesNotRepeatCauseMessage() {
        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () ->
                actionWorker.logTransitStateAndThrow(Level.ERROR, "Scaling failed due to bad offering", null, null, new InvalidParameterValueException("bad offering")));
        Assert.assertEquals("Scaling failed due to bad offering", e.getMessage());
    }

    @Test
    public void testGetKubernetesNodeConfigSetsKubernetesVersion() throws IOException {
        KubernetesSupportedVersionVO version = Mockito.mock(KubernetesSupportedVersionVO.class);
        Mockito.when(version.getSemanticVersion()).thenReturn("1.33.1");
        Mockito.when(actionWorker.kubernetesCluster.getKubernetesVersionId()).thenReturn(DEFAULT_ID);
        Mockito.when(kubernetesSupportedVersionDao.findById(DEFAULT_ID)).thenReturn(version);
        // the join token and CA hash are filled in with the version
        actionWorker.joinToken = "abcdef.0123456789abcdef";
        actionWorker.joinCaCertHash = "sha256:" + "ab".repeat(32);
        KubernetesClusterActionWorker worker = Mockito.spy(actionWorker);
        String template = IOUtils.toString(getClass().getResourceAsStream("/conf/k8s-node.yml"), StandardCharsets.UTF_8);
        Mockito.doReturn(template).when(worker).readK8sConfigFile("/conf/k8s-node.yml");
        String config = worker.getKubernetesNodeConfig("10.1.1.1", false, false);
        Assert.assertTrue(config.contains("K8S_VERSION=\"1.33.1\""));
        Assert.assertFalse(config.contains("{{ k8s.version }}"));
    }

    @Test
    public void testGetKubernetesNodeNameLowercasesHostName() {
        UserVmVO vm = Mockito.mock(UserVmVO.class);
        Mockito.when(vm.getHostName()).thenReturn("External-Node-1");
        Assert.assertEquals("external-node-1", actionWorker.getKubernetesNodeName(vm));
        Mockito.verify(vm, Mockito.never()).getDisplayName();
    }

    @Test
    public void testGetKubernetesNodeNameBlankHostName() {
        for (String hostName : new String[] {null, "", " "}) {
            UserVmVO vm = Mockito.mock(UserVmVO.class);
            Mockito.when(vm.getHostName()).thenReturn(hostName);
            Assert.assertNull("hostName=" + hostName, actionWorker.getKubernetesNodeName(vm));
        }
    }

    @Test
    public void testGetKubernetesNodeNameNullVm() {
        Assert.assertNull(actionWorker.getKubernetesNodeName(null));
    }
}
