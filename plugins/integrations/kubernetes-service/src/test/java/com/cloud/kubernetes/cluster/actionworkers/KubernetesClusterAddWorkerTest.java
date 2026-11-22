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

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.cloud.exception.NetworkRuleConflictException;
import com.cloud.exception.ResourceUnavailableException;
import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.network.IpAddress;
import com.cloud.network.Network;
import com.cloud.network.dao.FirewallRulesDao;
import com.cloud.network.firewall.FirewallService;
import com.cloud.network.rules.FirewallRule;
import com.cloud.network.rules.FirewallRuleVO;
import com.cloud.network.rules.PortForwardingRuleVO;
import com.cloud.network.rules.RulesService;
import com.cloud.network.rules.dao.PortForwardingRulesDao;
import com.cloud.utils.exception.CloudRuntimeException;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClusterAddWorkerTest {

    private static final long IP_ID = 10L;
    private static final long VM_ID = 20L;
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

    private KubernetesClusterAddWorker worker;

    @Before
    public void setUp() {
        worker = new KubernetesClusterAddWorker(kubernetesCluster, kubernetesClusterManager);
        ReflectionTestUtils.setField(worker, "firewallRulesDao", firewallRulesDao);
        worker.portForwardingRulesDao = portForwardingRulesDao;
        worker.firewallService = firewallService;
        worker.rulesService = rulesService;
        Mockito.when(publicIp.getId()).thenReturn(IP_ID);
        Mockito.when(network.getVpcId()).thenReturn(null);
    }

    private PortForwardingRuleVO pfRule(long id, long ipId, int sourcePort, int destPort) {
        PortForwardingRuleVO rule = Mockito.mock(PortForwardingRuleVO.class);
        Mockito.lenient().when(rule.getId()).thenReturn(id);
        Mockito.lenient().when(rule.getPurpose()).thenReturn(FirewallRule.Purpose.PortForwarding);
        Mockito.lenient().when(rule.getSourceIpAddressId()).thenReturn(ipId);
        Mockito.lenient().when(rule.getSourcePortStart()).thenReturn(sourcePort);
        Mockito.lenient().when(rule.getDestinationPortStart()).thenReturn(destPort);
        return rule;
    }

    private FirewallRuleVO firewallRule(long id) {
        FirewallRuleVO rule = Mockito.mock(FirewallRuleVO.class);
        Mockito.when(rule.getId()).thenReturn(id);
        Mockito.lenient().when(rule.getPurpose()).thenReturn(FirewallRule.Purpose.Firewall);
        return rule;
    }

    private void mockFirewallRules(FirewallRuleVO... rules) {
        Mockito.when(firewallRulesDao.listByIpPurposePortsProtocolAndNotRevoked(IP_ID, SSH_PORT, SSH_PORT, "tcp",
                FirewallRule.Purpose.Firewall)).thenReturn(List.of(rules));
    }

    @Test
    public void revertNetworkRulesRemovesPortForwardingThenFirewallRule() {
        PortForwardingRuleVO pf = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(pf));
        mockFirewallRules(firewallRule(2L));
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenReturn(true);

        worker.revertNetworkRules(network, publicIp, VM_ID, SSH_PORT, Set.of());

        InOrder inOrder = Mockito.inOrder(rulesService, firewallService);
        inOrder.verify(rulesService).revokePortForwardingRule(1L, true);
        inOrder.verify(firewallService).revokeIngressFirewallRule(2L, true);
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
    public void revertNetworkRulesRemovesFirewallRuleWhenPortForwardingRevokeFails() {
        PortForwardingRuleVO pf = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(pf));
        mockFirewallRules(firewallRule(2L));
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenThrow(new CloudRuntimeException("apply failed"));
        Mockito.when(firewallService.revokeIngressFirewallRule(2L, true)).thenReturn(true);

        worker.revertNetworkRules(network, publicIp, VM_ID, SSH_PORT, Set.of());

        Mockito.verify(firewallService).revokeIngressFirewallRule(2L, true);
    }

    private KubernetesClusterAddWorker spyForSetup() {
        Mockito.when(kubernetesCluster.getTotalNodeCount()).thenReturn(2L);
        Mockito.when(kubernetesCluster.getEtcdNodeCount()).thenReturn(0L);
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
        PortForwardingRuleVO existing = pfRule(6L, IP_ID, SSH_PORT, 22);
        PortForwardingRuleVO created = pfRule(1L, IP_ID, SSH_PORT, 22);
        Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(existing));
        mockFirewallRules();
        Mockito.doNothing().when(spy).provisionFirewallRules(publicIp, null, SSH_PORT, SSH_PORT);
        Mockito.doAnswer(invocation -> {
            Mockito.when(portForwardingRulesDao.listByVm(VM_ID)).thenReturn(List.of(existing, created));
            throw new ResourceUnavailableException("apply failed", Network.class, 1L);
        }).when(spy).provisionPublicIpPortForwardingRule(publicIp, network, null, VM_ID, SSH_PORT, 22);
        Mockito.when(rulesService.revokePortForwardingRule(1L, true)).thenReturn(true);

        Assert.assertFalse(spy.validateAndSetupNode(network, publicIp, null, VM_ID, 0, "").first());

        Mockito.verify(rulesService).revokePortForwardingRule(1L, true);
        Mockito.verify(rulesService, Mockito.never()).revokePortForwardingRule(6L, true);
    }
}
