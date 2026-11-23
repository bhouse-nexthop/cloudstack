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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
import org.springframework.test.util.ReflectionTestUtils;

import com.cloud.exception.NetworkRuleConflictException;
import com.cloud.exception.ResourceUnavailableException;
import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.network.IpAddress;
import com.cloud.network.Network;
import com.cloud.network.dao.FirewallRulesDao;
import com.cloud.network.firewall.FirewallService;
import com.cloud.network.rules.FirewallRule;
import com.cloud.network.rules.FirewallRuleVO;
import com.cloud.network.rules.PortForwardingRuleVO;
import com.cloud.network.rules.RulesService;
import com.cloud.network.rules.dao.PortForwardingRulesDao;
import com.cloud.uservm.UserVm;
import com.cloud.utils.Pair;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.utils.net.Ip;
import com.cloud.utils.ssh.SshHelper;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClusterAddWorkerTest {

    private static final long IP_ID = 10L;
    private static final long VM_ID = 20L;
    private static final int START = KubernetesClusterActionWorker.CLUSTER_NODES_DEFAULT_START_SSH_PORT;
    private static final int SSH_PORT = 2224;

    @Mock
    private KubernetesClusterManagerImpl kubernetesClusterManager;
    @Mock
    private KubernetesCluster kubernetesCluster;
    @Mock
    private FirewallRulesDao firewallRulesDao;
    @Mock
    private PortForwardingRulesDao portForwardingRulesDao;
    @Mock
    private FirewallService firewallService;
    @Mock
    private RulesService rulesService;
    @Mock
    private Network network;
    @Mock
    private IpAddress publicIp;
    @Mock
    private KubernetesClusterVmMapDao kubernetesClusterVmMapDao;
    @Mock
    private KubernetesClusterDao kubernetesClusterDao;
    @Mock
    private UserVm userVm;

    private final File pkFile = new File("/dev/null");

    private KubernetesClusterAddWorker worker;

    @Before
    public void setUp() {
        worker = new KubernetesClusterAddWorker(kubernetesCluster, kubernetesClusterManager);
        ReflectionTestUtils.setField(worker, "firewallRulesDao", firewallRulesDao);
        worker.portForwardingRulesDao = portForwardingRulesDao;
        worker.firewallService = firewallService;
        worker.rulesService = rulesService;
        worker.kubernetesClusterVmMapDao = kubernetesClusterVmMapDao;
        worker.kubernetesClusterDao = kubernetesClusterDao;
        Mockito.lenient().when(publicIp.getAddress()).thenReturn(new Ip("192.0.2.10"));
        Mockito.lenient().when(publicIp.getId()).thenReturn(IP_ID);
        Mockito.lenient().when(network.getVpcId()).thenReturn(null);
    }

    private PortForwardingRuleVO pfRule(long id, long ipId, int sourcePort, int destPort) {
        PortForwardingRuleVO rule = Mockito.mock(PortForwardingRuleVO.class);
        Mockito.lenient().when(rule.getId()).thenReturn(id);
        Mockito.lenient().when(rule.getUuid()).thenReturn("uuid-" + id);
        Mockito.lenient().when(rule.getPurpose()).thenReturn(FirewallRule.Purpose.PortForwarding);
        Mockito.lenient().when(rule.getSourceIpAddressId()).thenReturn(ipId);
        Mockito.lenient().when(rule.getSourcePortStart()).thenReturn(sourcePort);
        Mockito.lenient().when(rule.getDestinationPortStart()).thenReturn(destPort);
        Mockito.lenient().when(rule.getSourcePortEnd()).thenReturn(sourcePort);
        Mockito.lenient().when(rule.getDestinationPortEnd()).thenReturn(destPort);
        Mockito.lenient().when(rule.getProtocol()).thenReturn("tcp");
        Mockito.lenient().when(rule.getVirtualMachineId()).thenReturn(VM_ID);
        return rule;
    }

    private FirewallRuleVO firewallRule(long id) {
        FirewallRuleVO rule = Mockito.mock(FirewallRuleVO.class);
        Mockito.when(rule.getId()).thenReturn(id);
        Mockito.lenient().when(rule.getUuid()).thenReturn("uuid-" + id);
        Mockito.lenient().when(rule.getSourcePortStart()).thenReturn(SSH_PORT);
        Mockito.lenient().when(rule.getPurpose()).thenReturn(FirewallRule.Purpose.Firewall);
        return rule;
    }

    private void mockFirewallRules(FirewallRuleVO... rules) {
        Mockito.when(firewallRulesDao.listByIpPurposePortsProtocolAndNotRevoked(IP_ID, SSH_PORT, SSH_PORT, "tcp",
                FirewallRule.Purpose.Firewall)).thenReturn(List.of(rules));
    }

    @Test
    public void revertNetworkRulesRemovesFirewallThenPortForwardingRule() {
        PortForwardingRuleVO pf = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(pf));
        mockFirewallRules(firewallRule(2L));
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenReturn(true);

        worker.revertNetworkRules(network, publicIp, VM_ID, SSH_PORT, Set.of());

        InOrder inOrder = Mockito.inOrder(rulesService, firewallService);
        inOrder.verify(firewallService).revokeIngressFirewallRule(2L, true);
        inOrder.verify(rulesService).revokePortForwardingRule(1L, true);
    }

    @Test
    public void revertNetworkRulesKeepsRulesThatExistedBefore() {
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of());
        mockFirewallRules(firewallRule(2L));

        worker.revertNetworkRules(network, publicIp, VM_ID, SSH_PORT, Set.of(2L));

        Mockito.verifyNoInteractions(firewallService, rulesService);
    }

    @Test
    public void revertNetworkRulesLeavesOtherPortForwardingRules() {
        PortForwardingRuleVO otherPort = pfRule(3L, IP_ID, 8080, 22);
        PortForwardingRuleVO otherIp = pfRule(4L, 99L, SSH_PORT, 22);
        PortForwardingRuleVO otherDest = pfRule(5L, IP_ID, SSH_PORT, 80);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(otherPort, otherIp, otherDest));
        mockFirewallRules();

        worker.revertNetworkRules(network, publicIp, VM_ID, SSH_PORT, Set.of());

        Mockito.verifyNoInteractions(rulesService, firewallService);
    }

    @Test
    public void revertNetworkRulesKeepsPortForwardingRuleWhenFirewallRevokeFails() {
        PortForwardingRuleVO pf = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(pf));
        mockFirewallRules(firewallRule(2L));
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenThrow(new CloudRuntimeException("apply failed"));

        worker.revertNetworkRules(network, publicIp, VM_ID, SSH_PORT, Set.of());

        Mockito.verifyNoInteractions(rulesService);
    }

    private KubernetesClusterAddWorker spyForSetup() {
        FirewallRuleVO occupied = portRule(FirewallRule.Purpose.PortForwarding, START, SSH_PORT - 1);
        Mockito.when(firewallRulesDao.listByIp(IP_ID)).thenReturn(List.of(occupied));
        return Mockito.spy(worker);
    }

    @Test
    public void validateAndSetupNodeFirewallConflictKeepsExistingRule() throws Exception {
        KubernetesClusterAddWorker spy = spyForSetup();
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of());
        mockFirewallRules(firewallRule(7L));
        Mockito.doThrow(new NetworkRuleConflictException("conflict")).when(spy)
                .provisionFirewallRules(publicIp, null, SSH_PORT, SSH_PORT);

        Assert.assertFalse(spy.validateAndSetupNode(network, publicIp, null, VM_ID, 0, "").first());

        Mockito.verifyNoInteractions(firewallService, rulesService);
    }

    @Test
    public void validateAndSetupNodeRemovesRulesCreatedBeforeFailure() throws Exception {
        KubernetesClusterAddWorker spy = spyForSetup();
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of());
        FirewallRuleVO existing = firewallRule(7L);
        mockFirewallRules(existing);
        Mockito.doAnswer(invocation -> {
            mockFirewallRules(existing, firewallRule(2L));
            return null;
        }).when(spy).provisionFirewallRules(publicIp, null, SSH_PORT, SSH_PORT);
        Mockito.doThrow(new NetworkRuleConflictException("conflict")).when(spy)
                .provisionPublicIpPortForwardingRule(publicIp, network, null, VM_ID, SSH_PORT, 22);
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenReturn(true);

        Assert.assertFalse(spy.validateAndSetupNode(network, publicIp, null, VM_ID, 0, "").first());

        Mockito.verify(firewallService).revokeIngressFirewallRule(2L, true);
        Mockito.verify(firewallService, Mockito.never()).revokeIngressFirewallRule(7L, true);
    }

    @Test
    public void revertNetworkRulesInVpcSkipsFirewallRules() {
        Mockito.when(network.getVpcId()).thenReturn(5L);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of());

        worker.revertNetworkRules(network, publicIp, VM_ID, SSH_PORT, Set.of());

        Mockito.verifyNoInteractions(firewallRulesDao, firewallService);
    }

    @Test
    public void listNodeSshRuleIdsReturnsPortForwardingAndFirewallRuleIds() {
        PortForwardingRuleVO pf = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(pf));
        mockFirewallRules(firewallRule(2L));

        Assert.assertEquals(Set.of(1L, 2L), worker.listNodeSshRuleIds(network, publicIp, VM_ID, SSH_PORT));
    }

    @Test
    public void validateAndSetupNodeRemovesNewPortForwardingRuleWhenApplyFails() throws Exception {
        KubernetesClusterAddWorker spy = spyForSetup();
        PortForwardingRuleVO created = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of());
        mockFirewallRules();
        Mockito.doNothing().when(spy).provisionFirewallRules(publicIp, null, SSH_PORT, SSH_PORT);
        Mockito.doAnswer(invocation -> {
            Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(created));
            throw new ResourceUnavailableException("apply failed", Network.class, 1L);
        }).when(spy).provisionPublicIpPortForwardingRule(publicIp, network, null, VM_ID, SSH_PORT, 22);
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);

        Assert.assertFalse(spy.validateAndSetupNode(network, publicIp, null, VM_ID, 0, "").first());

        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
    }

    @Test
    public void validateAndSetupNodeFailsWithoutRulesWhenNoPortIsFree() throws Exception {
        FirewallRuleVO occupied = portRule(FirewallRule.Purpose.PortForwarding, START, 65535);
        Mockito.when(firewallRulesDao.listByIp(IP_ID)).thenReturn(List.of(occupied));
        KubernetesClusterAddWorker spy = Mockito.spy(worker);

        Assert.assertFalse(spy.validateAndSetupNode(network, publicIp, null, VM_ID, 0, "").first());

        Mockito.verify(spy, Mockito.never()).provisionFirewallRules(Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.verify(spy, Mockito.never()).provisionPublicIpPortForwardingRule(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyLong(), Mockito.anyInt(), Mockito.anyInt());
    }

    private FirewallRuleVO leftoverFirewallRule(long id, int port, String... cidrs) {
        FirewallRuleVO rule = firewallRule(id);
        Mockito.lenient().when(rule.getSourcePortStart()).thenReturn(port);
        Mockito.lenient().when(rule.getSourceCidrList()).thenReturn(List.of(cidrs));
        return rule;
    }

    private void mockPortFirewallRules(int port, FirewallRuleVO... rules) {
        Mockito.when(firewallRulesDao.listByIpPurposePortsProtocolAndNotRevoked(IP_ID, port, port, "tcp",
                FirewallRule.Purpose.Firewall)).thenReturn(List.of(rules));
    }

    @Test
    public void removeLeftoverNodeSshRulesRemovesOwnLeftover() {
        PortForwardingRuleVO leftover = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(leftover));
        mockPortFirewallRules(SSH_PORT, leftoverFirewallRule(2L, SSH_PORT, "0.0.0.0/0"));
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenReturn(true);

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        InOrder inOrder = Mockito.inOrder(rulesService, firewallService);
        inOrder.verify(firewallService).revokeIngressFirewallRule(2L, true);
        inOrder.verify(rulesService).revokePortForwardingRule(1L, true);
    }

    @Test
    public void removeLeftoverNodeSshRulesFindsLeftoverOnAnySshPort() {
        PortForwardingRuleVO leftover = pfRule(1L, IP_ID, 2300, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(leftover));
        mockPortFirewallRules(2300, leftoverFirewallRule(2L, 2300, "0.0.0.0/0"));
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenReturn(true);

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        Mockito.verify(firewallService).revokeIngressFirewallRule(2L, true);
        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
    }

    @Test
    public void removeLeftoverNodeSshRulesKeepsRulesOfClusterNode() {
        KubernetesClusterVmMapVO map = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(map.getClusterId()).thenReturn(40L);
        Mockito.when(kubernetesClusterVmMapDao.listByVmId(VM_ID)).thenReturn(List.of(map));
        Mockito.when(kubernetesClusterDao.findById(40L)).thenReturn(Mockito.mock(KubernetesClusterVO.class));

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        Mockito.verifyNoInteractions(portForwardingRulesDao, rulesService, firewallService);
    }

    @Test
    public void removeLeftoverNodeSshRulesRemovesLeftoverOfVmMappedToGoneCluster() {
        KubernetesClusterVmMapVO map = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(map.getClusterId()).thenReturn(40L);
        Mockito.when(kubernetesClusterVmMapDao.listByVmId(VM_ID)).thenReturn(List.of(map));
        PortForwardingRuleVO leftover = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(leftover));
        mockPortFirewallRules(SSH_PORT);
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
    }

    @Test
    public void removeLeftoverNodeSshRulesKeepsFirewallRuleOfPortInAnotherForwardRange() {
        PortForwardingRuleVO leftover = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(leftover));
        PortForwardingRuleVO other = pfRule(3L, IP_ID, SSH_PORT - 2, 22);
        Mockito.when(other.getSourcePortEnd()).thenReturn(SSH_PORT + 6);
        Mockito.when(firewallRulesDao.listByIp(IP_ID)).thenReturn(List.of(leftover, other));
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
        Mockito.verify(rulesService, Mockito.never()).revokePortForwardingRule(3L, true);
        Mockito.verifyNoInteractions(firewallService);
    }

    @Test
    public void removeLeftoverNodeSshRulesRemovesFirewallRuleOnceForLeftoversSharingPort() {
        PortForwardingRuleVO first = pfRule(1L, IP_ID, SSH_PORT, 22);
        PortForwardingRuleVO second = pfRule(3L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(first));
        Mockito.when(portForwardingRulesDao.listByVm(21L)).thenReturn(List.of(second));
        Mockito.when(firewallRulesDao.listByIp(IP_ID)).thenReturn(List.of(first, second));
        mockPortFirewallRules(SSH_PORT, leftoverFirewallRule(2L, SSH_PORT, "0.0.0.0/0"));
        Mockito.when(rulesService.revokePortForwardingRule(Mockito.anyLong(), Mockito.eq(true))).thenReturn(true);
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenReturn(true);

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID, 21L));

        Mockito.verify(firewallService, Mockito.times(1)).revokeIngressFirewallRule(2L, true);
        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
        Mockito.verify(rulesService).revokePortForwardingRule(3L, true);
    }

    @Test
    public void removeLeftoverNodeSshRulesKeepsFirewallRuleWithDestinationCidrs() {
        PortForwardingRuleVO leftover = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(leftover));
        FirewallRuleVO withDest = leftoverFirewallRule(2L, SSH_PORT, "0.0.0.0/0");
        Mockito.when(withDest.getDestinationCidrList()).thenReturn(List.of("10.1.1.5/32"));
        mockPortFirewallRules(SSH_PORT, withDest);
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
        Mockito.verifyNoInteractions(firewallService);
    }

    @Test
    public void removeLeftoverNodeSshRulesKeepsUserFirewallRuleNextToLeftover() {
        PortForwardingRuleVO leftover = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(leftover));
        mockPortFirewallRules(SSH_PORT, leftoverFirewallRule(2L, SSH_PORT, "203.0.113.0/24"));
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
        Mockito.verifyNoInteractions(firewallService);
    }

    @Test
    public void removeLeftoverNodeSshRulesIgnoresRulesNotShapedLikeNodeSsh() {
        PortForwardingRuleVO otherIp = pfRule(3L, 99L, SSH_PORT, 22);
        PortForwardingRuleVO belowStart = pfRule(4L, IP_ID, 22, 22);
        PortForwardingRuleVO otherDest = pfRule(5L, IP_ID, SSH_PORT, 80);
        PortForwardingRuleVO range = pfRule(6L, IP_ID, SSH_PORT, 22);
        Mockito.when(range.getSourcePortEnd()).thenReturn(SSH_PORT + 5);
        PortForwardingRuleVO udp = pfRule(7L, IP_ID, SSH_PORT, 22);
        Mockito.when(udp.getProtocol()).thenReturn("udp");
        PortForwardingRuleVO restricted = pfRule(8L, IP_ID, SSH_PORT, 22);
        Mockito.when(restricted.getSourceCidrList()).thenReturn(List.of("203.0.113.0/24"));
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(otherIp, belowStart, otherDest, range, udp, restricted));

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        Mockito.verifyNoInteractions(rulesService, firewallService);
        Mockito.verify(firewallRulesDao, Mockito.never()).listByIpPurposePortsProtocolAndNotRevoked(Mockito.anyLong(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void removeLeftoverNodeSshRulesKeepsPortForwardingWhenFirewallRevokeFails() {
        PortForwardingRuleVO leftover = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(leftover));
        mockPortFirewallRules(SSH_PORT, leftoverFirewallRule(2L, SSH_PORT, "0.0.0.0/0"));
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenThrow(new CloudRuntimeException("apply failed"));

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class,
                () -> worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID)));

        Assert.assertTrue(e.getMessage().contains("uuid-2"));
        Mockito.verifyNoInteractions(rulesService);
    }

    @Test
    public void removeLeftoverNodeSshRulesInVpcRemovesOnlyPortForwarding() {
        Mockito.when(network.getVpcId()).thenReturn(5L);
        PortForwardingRuleVO leftover = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(leftover));
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);

        worker.removeLeftoverNodeSshRules(network, publicIp, List.of(VM_ID));

        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
        Mockito.verifyNoInteractions(firewallService);
        Mockito.verify(firewallRulesDao, Mockito.never()).listByIpPurposePortsProtocolAndNotRevoked(Mockito.anyLong(), Mockito.any(),
                Mockito.any(), Mockito.any(), Mockito.any());
    }

    private FirewallRuleVO portRule(FirewallRule.Purpose purpose, Integer start, Integer end) {
        FirewallRuleVO rule = Mockito.mock(FirewallRuleVO.class);
        Mockito.lenient().when(rule.getPurpose()).thenReturn(purpose);
        Mockito.lenient().when(rule.getSourcePortStart()).thenReturn(start);
        Mockito.lenient().when(rule.getSourcePortEnd()).thenReturn(end);
        Mockito.lenient().when(rule.getProtocol()).thenReturn("tcp");
        return rule;
    }

    private FirewallRuleVO pf(int port) {
        return portRule(FirewallRule.Purpose.PortForwarding, port, port);
    }

    private FirewallRuleVO firewall(Integer start, Integer end) {
        return firewall(start, end, List.of("0.0.0.0/0"), Collections.emptyList());
    }

    private FirewallRuleVO firewall(Integer start, Integer end, List<String> sourceCidrs, List<String> destCidrs) {
        FirewallRuleVO rule = portRule(FirewallRule.Purpose.Firewall, start, end);
        Mockito.lenient().when(rule.getSourceCidrList()).thenReturn(sourceCidrs);
        Mockito.lenient().when(rule.getDestinationCidrList()).thenReturn(destCidrs);
        return rule;
    }

    private void mockRules(FirewallRuleVO... rules) {
        Mockito.when(firewallRulesDao.listByIp(IP_ID)).thenReturn(Arrays.asList(rules));
    }

    private Integer freePort() {
        return worker.getFreeNodeSshPort(network, publicIp);
    }

    @Test
    public void freePortIsStartWhenNoRules() {
        mockRules();
        Assert.assertEquals(Integer.valueOf(START), freePort());
    }

    @Test
    public void freePortIsAfterContiguousNodes() {
        FirewallRuleVO nodeRange = firewall(START, START + 2);
        mockRules(nodeRange, pf(START), pf(START + 1), pf(START + 2), pf(START + 3), firewall(START + 3, START + 3));
        Assert.assertEquals(Integer.valueOf(START + 4), freePort());
        Mockito.verify(firewallRulesDao).loadSourceCidrs(nodeRange);
        Mockito.verify(firewallRulesDao).loadDestinationCidrs(nodeRange);
    }

    @Test
    public void freePortFillsGapLeftByRemovedNode() {
        // control + 2 workers on START..START+2, added nodes on START+3 and START+5, START+4 was removed
        mockRules(pf(START + 5), pf(START + 3), pf(START + 1), pf(START), pf(START + 2),
                firewall(START + 5, START + 5), firewall(START + 3, START + 3), firewall(START, START + 2));
        Assert.assertEquals(Integer.valueOf(START + 4), freePort());
    }

    @Test
    public void freePortSkipsNonFirewallRulesOfAnyKind() {
        FirewallRuleVO udp = pf(START + 12);
        Mockito.lenient().when(udp.getProtocol()).thenReturn("udp");
        mockRules(portRule(FirewallRule.Purpose.LoadBalancing, START, START), pf(START + 1),
                portRule(FirewallRule.Purpose.PortForwarding, START + 2, START + 10), portRule(FirewallRule.Purpose.Vpn, START + 11, START + 11), udp);
        Assert.assertEquals(Integer.valueOf(START + 13), freePort());
    }

    @Test
    public void freePortIgnoresRulesWithoutPortsAndBelowStart() {
        mockRules(firewall(null, null), portRule(FirewallRule.Purpose.LoadBalancing, 6443, 6443), pf(22), firewall(1, 1024), firewall(22, 22));
        Assert.assertEquals(Integer.valueOf(START), freePort());
    }

    @Test
    public void freePortSkipsRangeStartingBelowStart() {
        mockRules(firewall(2000, START + 1), pf(START + 3));
        Assert.assertEquals(Integer.valueOf(START + 2), freePort());
    }

    @Test
    public void freePortIgnoresFirewallRangesThatDoNotClash() {
        FirewallRuleVO udp = firewall(START, START + 10);
        Mockito.lenient().when(udp.getProtocol()).thenReturn("udp");
        mockRules(udp, firewall(1024, 65535, List.of("198.51.100.0/24"), Collections.emptyList()),
                firewall(START, START + 10, List.of("0.0.0.0/0"), List.of("10.1.1.5/32")),
                firewall(START, START, List.of("198.51.100.0/24"), Collections.emptyList()));
        Assert.assertEquals(Integer.valueOf(START), freePort());
    }

    @Test
    public void freePortIgnoresFirewallRulesInVpc() {
        Mockito.when(network.getVpcId()).thenReturn(5L);
        mockRules(firewall(START, START), pf(START + 1));
        Assert.assertEquals(Integer.valueOf(START), freePort());
    }

    private KubernetesClusterVmMapVO vmMap(boolean external, boolean etcd) {
        KubernetesClusterVmMapVO map = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.lenient().when(map.isExternalNode()).thenReturn(external);
        Mockito.lenient().when(map.isEtcdNode()).thenReturn(etcd);
        return map;
    }

    @Test
    public void freePortKeepsPositionalPortsOfDefaultNodesReserved() {
        List<KubernetesClusterVmMapVO> maps = List.of(vmMap(false, false), vmMap(false, false), vmMap(false, false),
                vmMap(false, true), vmMap(true, false));
        Mockito.when(kubernetesClusterVmMapDao.listByClusterId(Mockito.anyLong())).thenReturn(maps);
        mockRules(pf(START + 4));
        Assert.assertEquals(Integer.valueOf(START + 3), freePort());
    }

    @Test
    public void freePortIsNullWhenNoneLeft() {
        mockRules(portRule(FirewallRule.Purpose.PortForwarding, START, 65535));
        Assert.assertNull(freePort());
    }

    @Test
    public void getNodeProvisioningResetCommandClearsMarkerAndKubeadmState() {
        Mockito.when(userVm.getUuid()).thenReturn("vm-uuid");
        Assert.assertEquals("sudo rm -rf /var/lib/cloud/instances/vm-uuid/sem/* && sudo rm -f /home/cloud/success && " +
                "{ sudo systemctl stop deploy-kube-system 2>/dev/null; " +
                "sudo umount /mnt/k8sdisk 2>/dev/null; mountpoint -q /mnt/k8sdisk || sudo rm -rf /mnt/k8sdisk; sudo rm -rf /aux-dwnld; " +
                "sudo systemctl disable --now kubelet 2>/dev/null; " +
                "if PATH=$PATH:/opt/bin command -v kubeadm >/dev/null; then " +
                "sudo env PATH=$PATH:/opt/bin kubeadm reset -f; fi; }",
                worker.getNodeProvisioningResetCommand(userVm));
    }

    private KubernetesClusterAddWorker resetWorker() {
        Mockito.when(userVm.getUuid()).thenReturn("vm-uuid");
        KubernetesClusterAddWorker spy = Mockito.spy(worker);
        Mockito.doReturn("cloud").when(spy).getControlNodeLoginUser();
        return spy;
    }

    private void verifyResetCommandRun(KubernetesClusterAddWorker spy, MockedStatic<SshHelper> sshHelper) {
        String command = spy.getNodeProvisioningResetCommand(userVm);
        sshHelper.verify(() -> SshHelper.sshExecute(ArgumentMatchers.eq("192.0.2.10"), ArgumentMatchers.eq(2222),
                ArgumentMatchers.eq("cloud"), ArgumentMatchers.eq(pkFile), ArgumentMatchers.isNull(),
                ArgumentMatchers.eq(command),
                ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()));
    }

    private void stubSsh(MockedStatic<SshHelper> sshHelper, Object result) {
        org.mockito.stubbing.OngoingStubbing<Pair<Boolean, String>> stub = sshHelper.when(() -> SshHelper.sshExecute(ArgumentMatchers.anyString(),
                ArgumentMatchers.anyInt(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()));
        if (result instanceof RuntimeException) {
            stub.thenThrow((RuntimeException) result);
        } else {
            stub.thenReturn((Pair<Boolean, String>) result);
        }
    }

    @Test
    public void resetNodeProvisioningStateRunsResetCommand() {
        KubernetesClusterAddWorker spy = resetWorker();
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            stubSsh(sshHelper, new Pair<>(true, ""));
            spy.resetNodeProvisioningState(userVm, publicIp, pkFile, 2222);
            verifyResetCommandRun(spy, sshHelper);
        }
    }

    @Test
    public void resetNodeProvisioningStateToleratesUnreachableNode() {
        KubernetesClusterAddWorker spy = resetWorker();
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            stubSsh(sshHelper, new RuntimeException("unreachable"));
            spy.resetNodeProvisioningState(userVm, publicIp, pkFile, 2222);
            verifyResetCommandRun(spy, sshHelper);
        }
    }

    @Test
    public void resetNodeProvisioningStateToleratesCommandFailure() {
        KubernetesClusterAddWorker spy = resetWorker();
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            stubSsh(sshHelper, new Pair<>(false, "kubeadm failed"));
            spy.resetNodeProvisioningState(userVm, publicIp, pkFile, 2222);
            verifyResetCommandRun(spy, sshHelper);
        }
    }
}
