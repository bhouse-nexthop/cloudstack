/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cloud.kubernetes.cluster;

import com.cloud.api.query.dao.TemplateJoinDao;
import com.cloud.api.query.vo.TemplateJoinVO;
import com.cloud.cpu.CPU;
import com.cloud.dc.DataCenter;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.exception.PermissionDeniedException;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterActionWorker;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterAffinityGroupMapDao;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterUpgradeWorker;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDetailsDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.kubernetes.version.KubernetesSupportedVersion;
import com.cloud.kubernetes.version.KubernetesSupportedVersionVO;
import com.cloud.kubernetes.version.dao.KubernetesSupportedVersionDao;
import com.cloud.network.Network;
import com.cloud.network.dao.FirewallRulesDao;
import com.cloud.network.dao.IPAddressDao;
import com.cloud.network.dao.IPAddressVO;
import com.cloud.network.rules.FirewallRule;
import com.cloud.network.rules.FirewallRuleVO;
import com.cloud.network.rules.PortForwardingRuleVO;
import com.cloud.network.rules.dao.PortForwardingRulesDao;
import com.cloud.network.vpc.NetworkACL;
import com.cloud.offering.ServiceOffering;
import com.cloud.service.ServiceOfferingVO;
import com.cloud.service.dao.ServiceOfferingDao;
import com.cloud.storage.VMTemplateVO;
import com.cloud.storage.dao.VMTemplateDao;
import com.cloud.user.Account;
import com.cloud.user.AccountManager;
import org.apache.cloudstack.acl.Role;
import org.apache.cloudstack.acl.RolePermission;
import org.apache.cloudstack.acl.RolePermissionEntity;
import org.apache.cloudstack.acl.RoleService;
import org.apache.cloudstack.acl.Rule;
import com.cloud.user.User;
import com.cloud.user.UserDataVO;
import com.cloud.user.dao.UserDataDao;
import com.cloud.utils.Pair;
import com.cloud.utils.net.Ip;
import com.cloud.utils.net.NetUtils;
import com.cloud.vm.NicVO;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.VirtualMachine;
import com.cloud.vm.dao.NicDao;
import com.cloud.vm.dao.VMInstanceDao;
import org.apache.cloudstack.api.response.KubernetesClusterResponse;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import org.apache.cloudstack.affinity.AffinityGroupService;
import org.apache.cloudstack.affinity.AffinityGroupVO;
import org.apache.cloudstack.affinity.AffinityProcessorBase;
import org.apache.cloudstack.affinity.dao.AffinityGroupDao;
import org.apache.cloudstack.affinity.dao.AffinityGroupVMMapDao;
import org.apache.cloudstack.api.ApiCommandResourceType;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.command.user.kubernetes.cluster.AddVirtualMachinesToKubernetesClusterCmd;
import org.apache.cloudstack.api.command.user.kubernetes.cluster.RemoveNodesFromKubernetesClusterCmd;
import org.apache.cloudstack.api.command.user.kubernetes.cluster.RemoveVirtualMachinesFromKubernetesClusterCmd;
import org.apache.cloudstack.context.CallContext;
import org.apache.cloudstack.framework.config.ConfigKey;
import org.apache.cloudstack.framework.jobs.dao.AsyncJobDao;
import org.apache.cloudstack.framework.jobs.impl.AsyncJobVO;
import org.apache.commons.collections.MapUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.CONTROL;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.DEFAULT;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.ETCD;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.WORKER;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClusterManagerImplTest {

    @Mock
    FirewallRulesDao firewallRulesDao;

    @Mock
    VMTemplateDao templateDao;

    @Mock
    TemplateJoinDao templateJoinDao;

    @Mock
    KubernetesClusterDao kubernetesClusterDao;

    @Mock
    KubernetesClusterVmMapDao kubernetesClusterVmMapDao;

    @Mock
    KubernetesClusterDetailsDao kubernetesClusterDetailsDao;

    @Mock
    KubernetesSupportedVersionDao kubernetesSupportedVersionDao;

    @Mock
    VMInstanceDao vmInstanceDao;

    @Mock
    private AccountManager accountManager;

    @Mock
    private ServiceOfferingDao serviceOfferingDao;

    @Mock
    private KubernetesClusterAffinityGroupMapDao kubernetesClusterAffinityGroupMapDao;

    @Mock
    private AffinityGroupDao affinityGroupDao;

    @Mock
    private HostDao hostDao;

    @Mock
    private AsyncJobDao asyncJobDao;

    @Mock
    private RoleService roleService;

    @Mock
    private AffinityGroupService affinityGroupService;

    @Mock
    private AffinityGroupVMMapDao affinityGroupVMMapDao;

    @Mock
    private NicDao nicDao;

    @Mock
    private UserDataDao userDataDao;

    @Mock
    private PortForwardingRulesDao pfRuleDao;

    @Mock
    private IPAddressDao ipAddressDao;

    @Spy
    @InjectMocks
    KubernetesClusterManagerImpl kubernetesClusterManager;

    @Test
    public void testValidateVpcTierAllocated() {
        Network network = Mockito.mock(Network.class);
        Mockito.when(network.getState()).thenReturn(Network.State.Allocated);
        kubernetesClusterManager.validateVpcTier(network);
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateVpcTierDefaultDenyRule() {
        Network network = Mockito.mock(Network.class);
        Mockito.when(network.getState()).thenReturn(Network.State.Implemented);
        Mockito.when(network.getNetworkACLId()).thenReturn(NetworkACL.DEFAULT_DENY);
        kubernetesClusterManager.validateVpcTier(network);
    }

    @Test
    public void testValidateVpcTierValid() {
        Network network = Mockito.mock(Network.class);
        Mockito.when(network.getState()).thenReturn(Network.State.Implemented);
        Mockito.when(network.getNetworkACLId()).thenReturn(NetworkACL.DEFAULT_ALLOW);
        kubernetesClusterManager.validateVpcTier(network);
    }

    @Test
    public void validateIsolatedNetworkIpRulesNoRules() {
        long ipId = 1L;
        FirewallRule.Purpose purpose = FirewallRule.Purpose.Firewall;
        Network network = Mockito.mock(Network.class);
        Mockito.when(firewallRulesDao.listByIpPurposeProtocolAndNotRevoked(ipId, purpose, NetUtils.TCP_PROTO)).thenReturn(new ArrayList<>());
        kubernetesClusterManager.validateIsolatedNetworkIpRules(ipId, FirewallRule.Purpose.Firewall, network, 3);
    }

    private FirewallRuleVO createRule(int startPort, int endPort) {
        FirewallRuleVO rule = new FirewallRuleVO(null, null, startPort, endPort, "tcp", 1L, 1, 1, FirewallRule.Purpose.Firewall, List.of("0.0.0.0/0"), null, null, null, FirewallRule.TrafficType.Ingress);
        return rule;
    }

    @Test
    public void validateIsolatedNetworkIpRulesNoConflictingRules() {
        long ipId = 1L;
        FirewallRule.Purpose purpose = FirewallRule.Purpose.Firewall;
        Network network = Mockito.mock(Network.class);
        Mockito.when(firewallRulesDao.listByIpPurposeProtocolAndNotRevoked(ipId, purpose, NetUtils.TCP_PROTO)).thenReturn(List.of(createRule(80, 80), createRule(443, 443)));
        kubernetesClusterManager.validateIsolatedNetworkIpRules(ipId, FirewallRule.Purpose.Firewall, network, 3);
    }

    @Test(expected = InvalidParameterValueException.class)
    public void validateIsolatedNetworkIpRulesApiConflictingRules() {
        long ipId = 1L;
        FirewallRule.Purpose purpose = FirewallRule.Purpose.Firewall;
        Network network = Mockito.mock(Network.class);
        Mockito.when(firewallRulesDao.listByIpPurposeProtocolAndNotRevoked(ipId, purpose, NetUtils.TCP_PROTO)).thenReturn(List.of(createRule(6440, 6445), createRule(443, 443)));
        kubernetesClusterManager.validateIsolatedNetworkIpRules(ipId, FirewallRule.Purpose.Firewall, network, 3);
    }

    @Test(expected = InvalidParameterValueException.class)
    public void validateIsolatedNetworkIpRulesSshConflictingRules() {
        long ipId = 1L;
        FirewallRule.Purpose purpose = FirewallRule.Purpose.Firewall;
        Network network = Mockito.mock(Network.class);
        Mockito.when(firewallRulesDao.listByIpPurposeProtocolAndNotRevoked(ipId, purpose, NetUtils.TCP_PROTO)).thenReturn(List.of(createRule(2200, KubernetesClusterActionWorker.CLUSTER_NODES_DEFAULT_START_SSH_PORT), createRule(443, 443)));
        kubernetesClusterManager.validateIsolatedNetworkIpRules(ipId, FirewallRule.Purpose.Firewall, network, 3);
    }

    @Test
    public void validateIsolatedNetworkIpRulesNearConflictingRules() {
        long ipId = 1L;
        FirewallRule.Purpose purpose = FirewallRule.Purpose.Firewall;
        Network network = Mockito.mock(Network.class);
        Mockito.when(firewallRulesDao.listByIpPurposeProtocolAndNotRevoked(ipId, purpose, NetUtils.TCP_PROTO)).thenReturn(List.of(createRule(2220, 2221), createRule(2225, 2227), createRule(6440, 6442), createRule(6444, 6446)));
        kubernetesClusterManager.validateIsolatedNetworkIpRules(ipId, FirewallRule.Purpose.Firewall, network, 3);
    }

    @Test
    public void testValidateKubernetesClusterScaleSizeNullNewSizeNoError() {
        kubernetesClusterManager.validateKubernetesClusterScaleSize(Mockito.mock(KubernetesClusterVO.class), null, 100, Mockito.mock(DataCenter.class));
    }

    @Test
    public void testValidateKubernetesClusterScaleSizeSameNewSizeNoError() {
        Long size = 2L;
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(clusterVO.getNodeCount()).thenReturn(size);
        kubernetesClusterManager.validateKubernetesClusterScaleSize(clusterVO, size, 100, Mockito.mock(DataCenter.class));
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterScaleSizeStoppedCluster() {
        Long size = 2L;
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(clusterVO.getNodeCount()).thenReturn(size);
        Mockito.when(clusterVO.getState()).thenReturn(KubernetesCluster.State.Stopped);
        kubernetesClusterManager.validateKubernetesClusterScaleSize(clusterVO, 3L, 100, Mockito.mock(DataCenter.class));
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateKubernetesClusterScaleSizeZeroNewSize() {
        Long size = 2L;
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(clusterVO.getState()).thenReturn(KubernetesCluster.State.Running);
        Mockito.when(clusterVO.getNodeCount()).thenReturn(size);
        kubernetesClusterManager.validateKubernetesClusterScaleSize(clusterVO, 0L, 100, Mockito.mock(DataCenter.class));
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateKubernetesClusterScaleSizeOverMaxSize() {
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(clusterVO.getState()).thenReturn(KubernetesCluster.State.Running);
        Mockito.when(clusterVO.getControlNodeCount()).thenReturn(1L);
        kubernetesClusterManager.validateKubernetesClusterScaleSize(clusterVO, 4L, 4, Mockito.mock(DataCenter.class));
    }

    @Test
    public void testValidateKubernetesClusterScaleSizeDownscaleNoError() {
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(clusterVO.getState()).thenReturn(KubernetesCluster.State.Running);
        Mockito.when(clusterVO.getControlNodeCount()).thenReturn(1L);
        Mockito.when(clusterVO.getNodeCount()).thenReturn(4L);
        kubernetesClusterManager.validateKubernetesClusterScaleSize(clusterVO, 2L, 10, Mockito.mock(DataCenter.class));
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateKubernetesClusterScaleSizeUpscaleDeletedTemplate() {
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(clusterVO.getState()).thenReturn(KubernetesCluster.State.Running);
        Mockito.when(clusterVO.getControlNodeCount()).thenReturn(1L);
        Mockito.when(clusterVO.getNodeCount()).thenReturn(2L);
        Mockito.when(templateDao.findById(Mockito.anyLong())).thenReturn(null);
        kubernetesClusterManager.validateKubernetesClusterScaleSize(clusterVO, 4L, 10, Mockito.mock(DataCenter.class));
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateKubernetesClusterScaleSizeUpscaleNotInZoneTemplate() {
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(clusterVO.getState()).thenReturn(KubernetesCluster.State.Running);
        Mockito.when(clusterVO.getControlNodeCount()).thenReturn(1L);
        Mockito.when(clusterVO.getNodeCount()).thenReturn(2L);
        Mockito.when(templateDao.findById(Mockito.anyLong())).thenReturn(Mockito.mock(VMTemplateVO.class));
        Mockito.when(templateJoinDao.newTemplateView(Mockito.any(VMTemplateVO.class), Mockito.anyLong(), Mockito.anyBoolean())).thenReturn(null);
        kubernetesClusterManager.validateKubernetesClusterScaleSize(clusterVO, 4L, 10, Mockito.mock(DataCenter.class));
    }

    @Test
    public void testValidateKubernetesClusterScaleSizeUpscaleNoError() {
        KubernetesClusterVO clusterVO = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(clusterVO.getState()).thenReturn(KubernetesCluster.State.Running);
        Mockito.when(clusterVO.getControlNodeCount()).thenReturn(1L);
        Mockito.when(clusterVO.getNodeCount()).thenReturn(2L);
        Mockito.when(templateDao.findById(Mockito.anyLong())).thenReturn(Mockito.mock(VMTemplateVO.class));
        Mockito.when(templateJoinDao.newTemplateView(Mockito.any(VMTemplateVO.class), Mockito.anyLong(), Mockito.anyBoolean())).thenReturn(List.of(Mockito.mock(TemplateJoinVO.class)));
        kubernetesClusterManager.validateKubernetesClusterScaleSize(clusterVO, 4L, 10, Mockito.mock(DataCenter.class));

    }

    @Before
    public void setUp() throws Exception {
        CallContext.register(Mockito.mock(User.class), Mockito.mock(Account.class));
        overrideDefaultConfigValue(KubernetesClusterService.KubernetesServiceEnabled, "_defaultValue", "true");
        Mockito.doNothing().when(accountManager).checkAccess(
                Mockito.any(Account.class), Mockito.any(), Mockito.anyBoolean(), Mockito.any());
    }

    @After
    public void tearDown() throws Exception {
        CallContext.unregister();
    }

    private void overrideDefaultConfigValue(final ConfigKey configKey, final String name, final Object o) throws IllegalAccessException, NoSuchFieldException {
        Field f = ConfigKey.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(configKey, o);
    }

    @Test
    public void addVmsToCluster() {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        VMInstanceVO vm = Mockito.mock(VMInstanceVO.class);
        AddVirtualMachinesToKubernetesClusterCmd cmd = Mockito.mock(AddVirtualMachinesToKubernetesClusterCmd.class);
        List<Long> vmIds = Arrays.asList(1L, 2L, 3L);

        Mockito.when(cmd.getId()).thenReturn(1L);
        Mockito.when(cmd.getVmIds()).thenReturn(vmIds);
        Mockito.when(cmd.getActualCommandName()).thenReturn(BaseCmd.getCommandNameByClass(RemoveVirtualMachinesFromKubernetesClusterCmd.class));
        Mockito.when(cluster.getClusterType()).thenReturn(KubernetesCluster.ClusterType.ExternalManaged);
        Mockito.when(vmInstanceDao.findById(Mockito.anyLong())).thenReturn(vm);
        Mockito.when(kubernetesClusterDao.findById(Mockito.anyLong())).thenReturn(cluster);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmIdsIn(1L, vmIds)).thenReturn(Collections.emptyList());
        Assert.assertTrue(kubernetesClusterManager.addVmsToCluster(cmd));
    }

    private void mockNodeForAdd(long vmId, long networkId) {
        VMInstanceVO vm = Mockito.mock(VMInstanceVO.class);
        VMTemplateVO template = Mockito.mock(VMTemplateVO.class);
        NicVO nic = Mockito.mock(NicVO.class);
        Mockito.when(vm.getTemplateId()).thenReturn(10L);
        Mockito.when(template.isForCks()).thenReturn(true);
        Mockito.when(nic.getNetworkId()).thenReturn(networkId);
        Mockito.when(vmInstanceDao.findById(vmId)).thenReturn(vm);
        Mockito.when(templateDao.findById(10L)).thenReturn(template);
        Mockito.when(nicDao.findDefaultNicForVM(vmId)).thenReturn(nic);
    }

    @Test
    public void validateNodesAcceptsNodeNotInAnyCluster() {
        mockNodeForAdd(1L, 5L);
        Mockito.when(kubernetesClusterVmMapDao.listByVmId(1L)).thenReturn(Collections.emptyList());
        Assert.assertEquals(List.of(1L), kubernetesClusterManager.validateNodes(List.of(1L), 5L, "net", false));
    }

    @Test
    public void validateNodesRejectsNodeInAnotherCluster() {
        mockNodeForAdd(1L, 5L);
        KubernetesClusterVmMapVO map = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(map.getClusterId()).thenReturn(7L);
        Mockito.when(kubernetesClusterVmMapDao.listByVmId(1L)).thenReturn(List.of(map));
        Mockito.when(kubernetesClusterDao.findById(7L)).thenReturn(Mockito.mock(KubernetesClusterVO.class));
        Assert.assertTrue(kubernetesClusterManager.validateNodes(List.of(1L), 5L, "net", false).isEmpty());
    }

    @Test
    public void validateNodesAcceptsNodeMappedToRemovedCluster() {
        mockNodeForAdd(1L, 5L);
        KubernetesClusterVmMapVO map = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(map.getClusterId()).thenReturn(7L);
        Mockito.when(kubernetesClusterVmMapDao.listByVmId(1L)).thenReturn(List.of(map));
        Mockito.when(kubernetesClusterDao.findById(7L)).thenReturn(null);
        Assert.assertEquals(List.of(1L), kubernetesClusterManager.validateNodes(List.of(1L), 5L, "net", false));
    }

    @Test
    public void validateNodesRejectsNodeWithLiveMappingBehindStaleOne() {
        mockNodeForAdd(1L, 5L);
        KubernetesClusterVmMapVO stale = Mockito.mock(KubernetesClusterVmMapVO.class);
        KubernetesClusterVmMapVO live = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(stale.getClusterId()).thenReturn(7L);
        Mockito.when(live.getClusterId()).thenReturn(8L);
        Mockito.when(kubernetesClusterVmMapDao.listByVmId(1L)).thenReturn(List.of(stale, live));
        Mockito.when(kubernetesClusterDao.findById(7L)).thenReturn(null);
        Mockito.when(kubernetesClusterDao.findById(8L)).thenReturn(Mockito.mock(KubernetesClusterVO.class));
        Assert.assertTrue(kubernetesClusterManager.validateNodes(List.of(1L), 5L, "net", false).isEmpty());
    }

    @Test
    public void validateNodesDropsDuplicateIds() {
        mockNodeForAdd(1L, 5L);
        Mockito.when(kubernetesClusterVmMapDao.listByVmId(1L)).thenReturn(Collections.emptyList());
        Assert.assertEquals(List.of(1L), kubernetesClusterManager.validateNodes(Arrays.asList(1L, 1L), 5L, "net", false));
    }

    @Test
    public void removeVmsFromCluster() {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        RemoveVirtualMachinesFromKubernetesClusterCmd cmd = Mockito.mock(RemoveVirtualMachinesFromKubernetesClusterCmd.class);
        List<Long> vmIds = Arrays.asList(1L, 2L, 3L);

        Mockito.when(cmd.getId()).thenReturn(1L);
        Mockito.when(cmd.getVmIds()).thenReturn(vmIds);
        Mockito.when(cmd.getActualCommandName()).thenReturn(BaseCmd.getCommandNameByClass(RemoveVirtualMachinesFromKubernetesClusterCmd.class));
        Mockito.when(cluster.getClusterType()).thenReturn(KubernetesCluster.ClusterType.ExternalManaged);
        Mockito.when(kubernetesClusterDao.findById(Mockito.anyLong())).thenReturn(cluster);
        Assert.assertTrue(kubernetesClusterManager.removeVmsFromCluster(cmd).size() > 0);
    }

    @Test
    public void testValidateServiceOfferingNodeType() {
        Map<String, Long> map = new HashMap<>();
        map.put(WORKER.name(), 1L);
        map.put(CONTROL.name(), 2L);
        ServiceOfferingVO serviceOffering = Mockito.mock(ServiceOfferingVO.class);
        Mockito.when(serviceOfferingDao.findById(1L)).thenReturn(serviceOffering);
        Mockito.when(serviceOffering.isDynamic()).thenReturn(false);
        Mockito.when(serviceOffering.getCpu()).thenReturn(2);
        Mockito.when(serviceOffering.getRamSize()).thenReturn(2048);
        KubernetesSupportedVersion version = Mockito.mock(KubernetesSupportedVersion.class);
        Mockito.when(version.getMinimumCpu()).thenReturn(2);
        Mockito.when(version.getMinimumRamSize()).thenReturn(2048);
        kubernetesClusterManager.validateServiceOfferingForNode(map, 1L, WORKER.name(), null, version);
        Mockito.verify(kubernetesClusterManager).validateServiceOffering(serviceOffering, version);
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateServiceOfferingNodeTypeInvalidOffering() {
        Map<String, Long> map = new HashMap<>();
        map.put(WORKER.name(), 1L);
        map.put(CONTROL.name(), 2L);
        ServiceOfferingVO serviceOffering = Mockito.mock(ServiceOfferingVO.class);
        Mockito.when(serviceOfferingDao.findById(1L)).thenReturn(serviceOffering);
        Mockito.when(serviceOffering.isDynamic()).thenReturn(true);
        kubernetesClusterManager.validateServiceOfferingForNode(map, 1L, WORKER.name(), null, null);
    }

    @Test
    public void testClusterCapacity() {
        long workerOfferingId = 1L;
        long controlOfferingId = 2L;
        long workerCount = 2L;
        long controlCount = 2L;

        int workerOfferingCpus = 4;
        int workerOfferingMemory = 4096;
        int controlOfferingCpus = 2;
        int controlOfferingMemory = 2048;

        Map<String, Long> map = Map.of(WORKER.name(), workerOfferingId, CONTROL.name(), controlOfferingId);
        Map<String, Long> nodeCount = Map.of(WORKER.name(), workerCount, CONTROL.name(), controlCount);

        ServiceOfferingVO workerOffering = Mockito.mock(ServiceOfferingVO.class);
        Mockito.when(serviceOfferingDao.findById(workerOfferingId)).thenReturn(workerOffering);
        ServiceOfferingVO controlOffering = Mockito.mock(ServiceOfferingVO.class);
        Mockito.when(serviceOfferingDao.findById(controlOfferingId)).thenReturn(controlOffering);
        Mockito.when(workerOffering.getCpu()).thenReturn(workerOfferingCpus);
        Mockito.when(workerOffering.getRamSize()).thenReturn(workerOfferingMemory);
        Mockito.when(controlOffering.getCpu()).thenReturn(controlOfferingCpus);
        Mockito.when(controlOffering.getRamSize()).thenReturn(controlOfferingMemory);

        Pair<Long, Long> pair = kubernetesClusterManager.calculateClusterCapacity(map, nodeCount, 1L);
        Long expectedCpu = (workerOfferingCpus * workerCount) + (controlOfferingCpus * controlCount);
        Long expectedMemory = (workerOfferingMemory * workerCount) + (controlOfferingMemory * controlCount);
        Assert.assertEquals(expectedCpu, pair.first());
        Assert.assertEquals(expectedMemory, pair.second());
    }

    @Test
    public void testIsAnyNodeOfferingEmptyNullMap() {
        Assert.assertTrue(kubernetesClusterManager.isAnyNodeOfferingEmpty(null));
    }

    @Test
    public void testIsAnyNodeOfferingEmptyNullValue() {
        Map<String, Long> map = new HashMap<>();
        map.put(WORKER.name(), 1L);
        map.put(CONTROL.name(), null);
        map.put(ETCD.name(), 2L);
        Assert.assertTrue(kubernetesClusterManager.isAnyNodeOfferingEmpty(map));
    }

    @Test
    public void testIsAnyNodeOfferingEmpty() {
        Map<String, Long> map = new HashMap<>();
        map.put(WORKER.name(), 1L);
        map.put(CONTROL.name(), 2L);
        Assert.assertFalse(kubernetesClusterManager.isAnyNodeOfferingEmpty(map));
    }

    @Test
    public void testCreateNodeTypeToServiceOfferingMapNoOfferingRequested() {
        Map<String, ServiceOffering> mapping = kubernetesClusterManager.createNodeTypeToServiceOfferingMap(new HashMap<>(), null);
        Assert.assertTrue(MapUtils.isEmpty(mapping));
        Mockito.verifyNoInteractions(serviceOfferingDao);
    }

    @Test
    public void testCreateNodeTypeToServiceOfferingMapDefaultOffering() {
        ServiceOfferingVO offering = Mockito.mock(ServiceOfferingVO.class);
        Mockito.when(serviceOfferingDao.findById(1L)).thenReturn(offering);
        Map<String, ServiceOffering> mapping = kubernetesClusterManager.createNodeTypeToServiceOfferingMap(new HashMap<>(), 1L);
        Assert.assertEquals(1, mapping.size());
        Assert.assertEquals(offering, mapping.get(DEFAULT.name()));
    }

    @Test
    public void testCreateNodeTypeToServiceOfferingMap() {
        Map<String, Long> idsMap = new HashMap<>();
        long workerOfferingId = 1L;
        long controlOfferingId = 2L;
        idsMap.put(WORKER.name(), workerOfferingId);
        idsMap.put(CONTROL.name(), controlOfferingId);

        ServiceOfferingVO workerOffering = Mockito.mock(ServiceOfferingVO.class);
        Mockito.when(serviceOfferingDao.findById(workerOfferingId)).thenReturn(workerOffering);
        ServiceOfferingVO controlOffering = Mockito.mock(ServiceOfferingVO.class);
        Mockito.when(serviceOfferingDao.findById(controlOfferingId)).thenReturn(controlOffering);

        Map<String, ServiceOffering> mapping = kubernetesClusterManager.createNodeTypeToServiceOfferingMap(idsMap, null);
        Assert.assertEquals(2, mapping.size());
        Assert.assertTrue(mapping.containsKey(WORKER.name()) && mapping.containsKey(CONTROL.name()));
        Assert.assertEquals(workerOffering, mapping.get(WORKER.name()));
        Assert.assertEquals(controlOffering, mapping.get(CONTROL.name()));
    }

    @Test
    public void testGetCksClusterPreferredArchDifferentArchsPreferCKSIsoArch() {
        String systemVMArch = "x86_64";
        VMTemplateVO cksIso = Mockito.mock(VMTemplateVO.class);
        Mockito.when(cksIso.getArch()).thenReturn(CPU.CPUArch.arm64);
        String cksClusterPreferredArch = kubernetesClusterManager.getCksClusterPreferredArch(systemVMArch, cksIso);
        Assert.assertEquals(CPU.CPUArch.arm64.getType(), cksClusterPreferredArch);
    }

    @Test
    public void testGetCksClusterPreferredArchSameArch() {
        String systemVMArch = "x86_64";
        VMTemplateVO cksIso = Mockito.mock(VMTemplateVO.class);
        Mockito.when(cksIso.getArch()).thenReturn(CPU.CPUArch.amd64);
        String cksClusterPreferredArch = kubernetesClusterManager.getCksClusterPreferredArch(systemVMArch, cksIso);
        Assert.assertEquals(CPU.CPUArch.amd64.getType(), cksClusterPreferredArch);
    }

    @Test
    public void testSetAffinityGroupResponseForNodeTypeControl() {
        KubernetesClusterResponse response = new KubernetesClusterResponse();
        long clusterId = 1L;

        AffinityGroupVO ag1 = Mockito.mock(AffinityGroupVO.class);
        AffinityGroupVO ag2 = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(ag1.getUuid()).thenReturn("uuid-1");
        Mockito.when(ag1.getName()).thenReturn("affinity-group-1");
        Mockito.when(ag2.getUuid()).thenReturn("uuid-2");
        Mockito.when(ag2.getName()).thenReturn("affinity-group-2");

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(clusterId, CONTROL.name()))
            .thenReturn(Arrays.asList(1L, 2L));
        Mockito.when(affinityGroupDao.findById(1L)).thenReturn(ag1);
        Mockito.when(affinityGroupDao.findById(2L)).thenReturn(ag2);

        kubernetesClusterManager.setAffinityGroupResponseForNodeType(response, clusterId, CONTROL.name());

        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(clusterId, CONTROL.name());
        Mockito.verify(affinityGroupDao).findById(1L);
        Mockito.verify(affinityGroupDao).findById(2L);
    }

    @Test
    public void testSetAffinityGroupResponseForNodeTypeWorker() {
        KubernetesClusterResponse response = new KubernetesClusterResponse();
        long clusterId = 1L;

        AffinityGroupVO ag = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(ag.getUuid()).thenReturn("worker-uuid");
        Mockito.when(ag.getName()).thenReturn("worker-affinity");

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(clusterId, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(ag);

        kubernetesClusterManager.setAffinityGroupResponseForNodeType(response, clusterId, WORKER.name());

        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(clusterId, WORKER.name());
        Mockito.verify(affinityGroupDao).findById(10L);
    }

    @Test
    public void testSetAffinityGroupResponseForNodeTypeEtcd() {
        KubernetesClusterResponse response = new KubernetesClusterResponse();
        long clusterId = 1L;

        AffinityGroupVO ag = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(ag.getUuid()).thenReturn("etcd-uuid");
        Mockito.when(ag.getName()).thenReturn("etcd-affinity");

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(clusterId, ETCD.name()))
            .thenReturn(Arrays.asList(20L));
        Mockito.when(affinityGroupDao.findById(20L)).thenReturn(ag);

        kubernetesClusterManager.setAffinityGroupResponseForNodeType(response, clusterId, ETCD.name());

        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(clusterId, ETCD.name());
        Mockito.verify(affinityGroupDao).findById(20L);
    }

    @Test
    public void testSetAffinityGroupResponseForNodeTypeEmptyList() {
        KubernetesClusterResponse response = new KubernetesClusterResponse();
        long clusterId = 1L;

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(clusterId, CONTROL.name()))
            .thenReturn(Collections.emptyList());

        kubernetesClusterManager.setAffinityGroupResponseForNodeType(response, clusterId, CONTROL.name());

        Mockito.verify(affinityGroupDao, Mockito.never()).findById(Mockito.anyLong());
    }

    @Test
    public void testSetAffinityGroupResponseForNodeTypeNullList() {
        KubernetesClusterResponse response = new KubernetesClusterResponse();
        long clusterId = 1L;

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(clusterId, ETCD.name()))
            .thenReturn(null);

        kubernetesClusterManager.setAffinityGroupResponseForNodeType(response, clusterId, ETCD.name());

        Mockito.verify(affinityGroupDao, Mockito.never()).findById(Mockito.anyLong());
    }

    @Test
    public void testSetAffinityGroupResponseForNodeTypeNullAffinityGroup() {
        KubernetesClusterResponse response = new KubernetesClusterResponse();
        long clusterId = 1L;

        AffinityGroupVO ag1 = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(ag1.getUuid()).thenReturn("uuid-1");
        Mockito.when(ag1.getName()).thenReturn("affinity-group-1");

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(clusterId, CONTROL.name()))
            .thenReturn(Arrays.asList(1L, 2L));
        Mockito.when(affinityGroupDao.findById(1L)).thenReturn(ag1);
        Mockito.when(affinityGroupDao.findById(2L)).thenReturn(null);

        kubernetesClusterManager.setAffinityGroupResponseForNodeType(response, clusterId, CONTROL.name());

        Mockito.verify(affinityGroupDao).findById(1L);
        Mockito.verify(affinityGroupDao).findById(2L);
    }

    @Test
    public void testSetNodeTypeAffinityGroupResponse() {
        KubernetesClusterResponse response = new KubernetesClusterResponse();
        long clusterId = 1L;

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(Mockito.eq(clusterId), Mockito.anyString()))
            .thenReturn(Collections.emptyList());

        kubernetesClusterManager.setNodeTypeAffinityGroupResponse(response, clusterId);

        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(clusterId, CONTROL.name());
        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(clusterId, WORKER.name());
        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(clusterId, ETCD.name());
    }

    @Test
    public void testValidateNodeAffinityGroupsNoAffinityGroups() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        List<Long> nodeIds = Arrays.asList(100L, 101L);

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Collections.emptyList());

        kubernetesClusterManager.validateNodeAffinityGroups(nodeIds, cluster);

        Mockito.verify(kubernetesClusterVmMapDao, Mockito.never()).listByClusterIdAndVmType(Mockito.anyLong(), Mockito.any());
    }

    @Test
    public void testValidateNodeAffinityGroupsNullAffinityGroups() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        List<Long> nodeIds = Arrays.asList(100L, 101L);

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(null);

        kubernetesClusterManager.validateNodeAffinityGroups(nodeIds, cluster);

        Mockito.verify(kubernetesClusterVmMapDao, Mockito.never()).listByClusterIdAndVmType(Mockito.anyLong(), Mockito.any());
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateNodeAffinityGroupsAntiAffinityNewNodeOnExistingHost() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId = 100L;
        Long existingWorkerVmId = 200L;
        Long sharedHostId = 1000L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(affinityGroup.getType()).thenReturn("host anti-affinity");
        Mockito.when(affinityGroup.getName()).thenReturn("anti-affinity-group");

        VMInstanceVO newNode = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode.getHostId()).thenReturn(sharedHostId);
        Mockito.when(newNode.getInstanceName()).thenReturn("new-node-vm");

        VMInstanceVO existingWorkerVm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(existingWorkerVm.getHostId()).thenReturn(sharedHostId);

        KubernetesClusterVmMapVO workerVmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(workerVmMap.getVmId()).thenReturn(existingWorkerVmId);

        HostVO host = Mockito.mock(HostVO.class);
        Mockito.when(host.getName()).thenReturn("host-1");

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Arrays.asList(workerVmMap));
        Mockito.when(vmInstanceDao.findById(existingWorkerVmId)).thenReturn(existingWorkerVm);
        Mockito.when(vmInstanceDao.findById(newNodeId)).thenReturn(newNode);
        Mockito.when(hostDao.findById(sharedHostId)).thenReturn(host);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId), cluster);
    }

    @Test
    public void testValidateNodeAffinityGroupsAntiAffinityNewNodeOnDifferentHost() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.lenient().when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId = 100L;
        Long existingWorkerVmId = 200L;
        Long existingHostId = 1000L;
        Long newNodeHostId = 1001L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(affinityGroup.getType()).thenReturn("host anti-affinity");
        Mockito.lenient().when(affinityGroup.getName()).thenReturn("anti-affinity-group");

        VMInstanceVO newNode = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode.getHostId()).thenReturn(newNodeHostId);

        VMInstanceVO existingWorkerVm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(existingWorkerVm.getHostId()).thenReturn(existingHostId);

        KubernetesClusterVmMapVO workerVmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(workerVmMap.getVmId()).thenReturn(existingWorkerVmId);

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Arrays.asList(workerVmMap));
        Mockito.when(vmInstanceDao.findById(existingWorkerVmId)).thenReturn(existingWorkerVm);
        Mockito.when(vmInstanceDao.findById(newNodeId)).thenReturn(newNode);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId), cluster);

        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name());
    }

    @Test
    public void testValidateNodeAffinityGroupsAffinityNewNodeOnSameHost() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.lenient().when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId = 100L;
        Long existingWorkerVmId = 200L;
        Long sharedHostId = 1000L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(affinityGroup.getType()).thenReturn("host affinity");
        Mockito.lenient().when(affinityGroup.getName()).thenReturn("affinity-group");

        VMInstanceVO newNode = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode.getHostId()).thenReturn(sharedHostId);

        VMInstanceVO existingWorkerVm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(existingWorkerVm.getHostId()).thenReturn(sharedHostId);

        KubernetesClusterVmMapVO workerVmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(workerVmMap.getVmId()).thenReturn(existingWorkerVmId);

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Arrays.asList(workerVmMap));
        Mockito.when(vmInstanceDao.findById(existingWorkerVmId)).thenReturn(existingWorkerVm);
        Mockito.when(vmInstanceDao.findById(newNodeId)).thenReturn(newNode);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId), cluster);

        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name());
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateNodeAffinityGroupsAffinityNewNodeOnDifferentHost() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId = 100L;
        Long existingWorkerVmId = 200L;
        Long existingHostId = 1000L;
        Long newNodeHostId = 1001L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(affinityGroup.getType()).thenReturn("host affinity");
        Mockito.when(affinityGroup.getName()).thenReturn("affinity-group");

        VMInstanceVO newNode = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode.getHostId()).thenReturn(newNodeHostId);
        Mockito.when(newNode.getInstanceName()).thenReturn("new-node-vm");

        VMInstanceVO existingWorkerVm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(existingWorkerVm.getHostId()).thenReturn(existingHostId);

        KubernetesClusterVmMapVO workerVmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(workerVmMap.getVmId()).thenReturn(existingWorkerVmId);

        HostVO newHost = Mockito.mock(HostVO.class);
        Mockito.when(newHost.getName()).thenReturn("host-2");

        HostVO existingHost = Mockito.mock(HostVO.class);
        Mockito.when(existingHost.getName()).thenReturn("host-1");

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Arrays.asList(workerVmMap));
        Mockito.when(vmInstanceDao.findById(existingWorkerVmId)).thenReturn(existingWorkerVm);
        Mockito.when(vmInstanceDao.findById(newNodeId)).thenReturn(newNode);
        Mockito.when(hostDao.findById(newNodeHostId)).thenReturn(newHost);
        Mockito.when(hostDao.findById(existingHostId)).thenReturn(existingHost);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId), cluster);
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateNodeAffinityGroupsAntiAffinityMultipleNewNodesOnSameHost() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId1 = 100L;
        Long newNodeId2 = 101L;
        Long sharedHostId = 1000L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.lenient().when(affinityGroup.getType()).thenReturn("host anti-affinity");
        Mockito.when(affinityGroup.getName()).thenReturn("anti-affinity-group");

        VMInstanceVO newNode1 = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode1.getHostId()).thenReturn(sharedHostId);
        Mockito.lenient().when(newNode1.getInstanceName()).thenReturn("new-node-vm-1");

        VMInstanceVO newNode2 = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode2.getHostId()).thenReturn(sharedHostId);
        Mockito.when(newNode2.getInstanceName()).thenReturn("new-node-vm-2");

        HostVO host = Mockito.mock(HostVO.class);
        Mockito.when(host.getName()).thenReturn("host-1");

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Collections.emptyList());
        Mockito.when(vmInstanceDao.findById(newNodeId1)).thenReturn(newNode1);
        Mockito.when(vmInstanceDao.findById(newNodeId2)).thenReturn(newNode2);
        Mockito.when(hostDao.findById(sharedHostId)).thenReturn(host);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId1, newNodeId2), cluster);
    }

    @Test
    public void testValidateNodeAffinityGroupsAntiAffinityMultipleNewNodesOnDifferentHosts() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.lenient().when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId1 = 100L;
        Long newNodeId2 = 101L;
        Long hostId1 = 1000L;
        Long hostId2 = 1001L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.lenient().when(affinityGroup.getType()).thenReturn("host anti-affinity");
        Mockito.lenient().when(affinityGroup.getName()).thenReturn("anti-affinity-group");

        VMInstanceVO newNode1 = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode1.getHostId()).thenReturn(hostId1);

        VMInstanceVO newNode2 = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode2.getHostId()).thenReturn(hostId2);

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Collections.emptyList());
        Mockito.when(vmInstanceDao.findById(newNodeId1)).thenReturn(newNode1);
        Mockito.when(vmInstanceDao.findById(newNodeId2)).thenReturn(newNode2);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId1, newNodeId2), cluster);

        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name());
    }

    @Test
    public void testValidateNodeAffinityGroupsNodeWithNullHost() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.lenient().when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId = 100L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.lenient().when(affinityGroup.getType()).thenReturn("host anti-affinity");
        Mockito.lenient().when(affinityGroup.getName()).thenReturn("anti-affinity-group");

        VMInstanceVO newNode = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode.getHostId()).thenReturn(null);

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Collections.emptyList());
        Mockito.when(vmInstanceDao.findById(newNodeId)).thenReturn(newNode);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId), cluster);

        Mockito.verify(vmInstanceDao, Mockito.atLeastOnce()).findById(newNodeId);
    }

    @Test
    public void testValidateNodeAffinityGroupsNullNode() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.lenient().when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId = 100L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.lenient().when(affinityGroup.getType()).thenReturn("host anti-affinity");
        Mockito.lenient().when(affinityGroup.getName()).thenReturn("anti-affinity-group");

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Collections.emptyList());
        Mockito.when(vmInstanceDao.findById(newNodeId)).thenReturn(null);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId), cluster);

        Mockito.verify(vmInstanceDao, Mockito.atLeastOnce()).findById(newNodeId);
    }

    @Test
    public void testValidateNodeAffinityGroupsAffinityNoExistingWorkers() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.lenient().when(cluster.getName()).thenReturn("test-cluster");

        Long newNodeId = 100L;
        Long newNodeHostId = 1000L;

        AffinityGroupVO affinityGroup = Mockito.mock(AffinityGroupVO.class);
        Mockito.lenient().when(affinityGroup.getType()).thenReturn("host affinity");
        Mockito.lenient().when(affinityGroup.getName()).thenReturn("affinity-group");

        VMInstanceVO newNode = Mockito.mock(VMInstanceVO.class);
        Mockito.when(newNode.getHostId()).thenReturn(newNodeHostId);

        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name()))
            .thenReturn(Arrays.asList(10L));
        Mockito.when(affinityGroupDao.findById(10L)).thenReturn(affinityGroup);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(1L, WORKER))
            .thenReturn(Collections.emptyList());
        Mockito.when(vmInstanceDao.findById(newNodeId)).thenReturn(newNode);

        kubernetesClusterManager.validateNodeAffinityGroups(Arrays.asList(newNodeId), cluster);

        Mockito.verify(kubernetesClusterAffinityGroupMapDao).listAffinityGroupIdsByClusterIdAndNodeType(1L, WORKER.name());
    }

    private KubernetesClusterVO mockClusterForDefaultAntiAffinity() {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(cluster.getUuid()).thenReturn("abc");
        return cluster;
    }

    private Account mockOwnerForDefaultAntiAffinity() {
        Account owner = Mockito.mock(Account.class);
        Mockito.when(owner.getId()).thenReturn(5L);
        return owner;
    }

    private void mockPersistedAffinityGroups() {
        Mockito.when(affinityGroupService.isAffinityGroupProcessorAvailable(AffinityProcessorBase.AFFINITY_TYPE_NON_STRICT_HOST_ANTI)).thenReturn(true);
        Mockito.when(affinityGroupDao.persist(Mockito.any(AffinityGroupVO.class))).thenAnswer(invocation -> {
            AffinityGroupVO group = Mockito.spy((AffinityGroupVO) invocation.getArgument(0));
            Mockito.doReturn(group.getName().endsWith("-control") ? 10L : 20L).when(group).getId();
            return group;
        });
    }

    @Test
    public void addDefaultAntiAffinityGroupsCreatesNonStrictGroupPerNodeType() {
        KubernetesClusterVO cluster = mockClusterForDefaultAntiAffinity();
        Account owner = mockOwnerForDefaultAntiAffinity();
        mockPersistedAffinityGroups();

        Map<String, List<Long>> result = kubernetesClusterManager.addDefaultAntiAffinityGroups(cluster, owner,
                Map.of(CONTROL.name(), 3L, WORKER.name(), 2L, ETCD.name(), 0L), new HashMap<>());

        Assert.assertEquals(List.of(10L), result.get(CONTROL.name()));
        Assert.assertEquals(List.of(20L), result.get(WORKER.name()));
        Assert.assertFalse(result.containsKey(ETCD.name()));
        Mockito.verify(affinityGroupDao).persist(Mockito.argThat((AffinityGroupVO g) ->
                "cks-abc-control".equals(g.getName()) && AffinityProcessorBase.AFFINITY_TYPE_NON_STRICT_HOST_ANTI.equals(g.getType()) && g.getAccountId() == 5L));
        Mockito.verify(affinityGroupDao).persist(Mockito.argThat((AffinityGroupVO g) -> "cks-abc-worker".equals(g.getName())));
    }

    @Test
    public void addDefaultAntiAffinityGroupsKeepsUserGroups() {
        KubernetesClusterVO cluster = mockClusterForDefaultAntiAffinity();
        Account owner = mockOwnerForDefaultAntiAffinity();
        mockPersistedAffinityGroups();
        Map<String, List<Long>> userMap = new HashMap<>();
        userMap.put(WORKER.name(), List.of(99L));

        Map<String, List<Long>> result = kubernetesClusterManager.addDefaultAntiAffinityGroups(cluster, owner,
                Map.of(CONTROL.name(), 3L, WORKER.name(), 2L, ETCD.name(), 0L), userMap);

        Assert.assertEquals(List.of(99L), result.get(WORKER.name()));
        Assert.assertEquals(List.of(10L), result.get(CONTROL.name()));
        Mockito.verify(affinityGroupDao, Mockito.times(1)).persist(Mockito.any(AffinityGroupVO.class));
    }

    @Test
    public void addDefaultAntiAffinityGroupsSkipsSingleControlAndEtcdNode() {
        KubernetesClusterVO cluster = mockClusterForDefaultAntiAffinity();
        Account owner = mockOwnerForDefaultAntiAffinity();
        mockPersistedAffinityGroups();

        Map<String, List<Long>> result = kubernetesClusterManager.addDefaultAntiAffinityGroups(cluster, owner,
                Map.of(CONTROL.name(), 1L, WORKER.name(), 1L, ETCD.name(), 1L), null);

        Assert.assertEquals(Map.of(WORKER.name(), List.of(20L)), result);
    }

    @Test
    public void addDefaultAntiAffinityGroupsSkippedWhenProcessorUnavailable() {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Account owner = Mockito.mock(Account.class);
        Mockito.when(affinityGroupService.isAffinityGroupProcessorAvailable(AffinityProcessorBase.AFFINITY_TYPE_NON_STRICT_HOST_ANTI)).thenReturn(false);

        Map<String, List<Long>> result = kubernetesClusterManager.addDefaultAntiAffinityGroups(cluster, owner,
                Map.of(CONTROL.name(), 1L, WORKER.name(), 1L, ETCD.name(), 0L), null);

        Assert.assertTrue(result.isEmpty());
        Mockito.verify(affinityGroupDao, Mockito.never()).persist(Mockito.any(AffinityGroupVO.class));
    }

    @Test
    public void addDefaultAntiAffinityGroupsSkippedWhenDisabled() throws Exception {
        overrideDefaultConfigValue(KubernetesClusterService.KubernetesClusterDefaultAntiAffinityEnabled, "_defaultValue", "false");
        overrideDefaultConfigValue(KubernetesClusterService.KubernetesClusterDefaultAntiAffinityEnabled, "_value", null);
        try {
            Map<String, List<Long>> result = kubernetesClusterManager.addDefaultAntiAffinityGroups(
                    Mockito.mock(KubernetesClusterVO.class), Mockito.mock(Account.class),
                    Map.of(CONTROL.name(), 1L, WORKER.name(), 1L, ETCD.name(), 0L), null);
            Assert.assertTrue(result.isEmpty());
            Mockito.verifyNoInteractions(affinityGroupService);
        } finally {
            overrideDefaultConfigValue(KubernetesClusterService.KubernetesClusterDefaultAntiAffinityEnabled, "_defaultValue", "true");
            overrideDefaultConfigValue(KubernetesClusterService.KubernetesClusterDefaultAntiAffinityEnabled, "_value", null);
        }
    }

    @Test
    public void deleteDefaultAntiAffinityGroupsDeletesOnlyCksGroups() {
        KubernetesClusterVO cluster = mockClusterForDefaultAntiAffinity();
        Mockito.when(cluster.getAccountId()).thenReturn(5L);
        AffinityGroupVO control = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(control.getId()).thenReturn(10L);
        Mockito.when(control.getType()).thenReturn(AffinityProcessorBase.AFFINITY_TYPE_NON_STRICT_HOST_ANTI);
        AffinityGroupVO worker = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(worker.getType()).thenReturn("host affinity");
        Mockito.when(affinityGroupDao.findByAccountAndNames(5L, "cks-abc-worker", "cks-abc-control", "cks-abc-etcd"))
                .thenReturn(List.of(control, worker));
        Mockito.when(affinityGroupVMMapDao.listVmIdsByAffinityGroup(10L)).thenReturn(List.of(7L, 8L));
        VMInstanceVO destroyedVm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(destroyedVm.getState()).thenReturn(VirtualMachine.State.Destroyed);
        Mockito.when(vmInstanceDao.findById(8L)).thenReturn(destroyedVm);

        kubernetesClusterManager.deleteDefaultAntiAffinityGroups(cluster);

        Mockito.verify(affinityGroupService).deleteAffinityGroup(10L, null, null, null, null);
        Mockito.verifyNoMoreInteractions(affinityGroupService);
    }

    @Test
    public void deleteDefaultAntiAffinityGroupsKeepsGroupWithLiveVms() {
        KubernetesClusterVO cluster = mockClusterForDefaultAntiAffinity();
        Mockito.when(cluster.getAccountId()).thenReturn(5L);
        AffinityGroupVO control = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(control.getId()).thenReturn(10L);
        Mockito.when(control.getType()).thenReturn(AffinityProcessorBase.AFFINITY_TYPE_NON_STRICT_HOST_ANTI);
        Mockito.when(affinityGroupDao.findByAccountAndNames(5L, "cks-abc-worker", "cks-abc-control", "cks-abc-etcd"))
                .thenReturn(List.of(control));
        Mockito.when(affinityGroupVMMapDao.listVmIdsByAffinityGroup(10L)).thenReturn(List.of(7L));
        VMInstanceVO vm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(vm.getState()).thenReturn(VirtualMachine.State.Stopped);
        Mockito.when(vmInstanceDao.findById(7L)).thenReturn(vm);

        kubernetesClusterManager.deleteDefaultAntiAffinityGroups(cluster);

        Mockito.verifyNoInteractions(affinityGroupService);
    }

    @Test
    public void deleteDefaultAntiAffinityGroupsKeepsGroupUsedByAnotherCluster() {
        KubernetesClusterVO cluster = mockClusterForDefaultAntiAffinity();
        Mockito.when(cluster.getAccountId()).thenReturn(5L);
        AffinityGroupVO worker = Mockito.mock(AffinityGroupVO.class);
        Mockito.when(worker.getId()).thenReturn(20L);
        Mockito.when(worker.getType()).thenReturn(AffinityProcessorBase.AFFINITY_TYPE_NON_STRICT_HOST_ANTI);
        Mockito.when(affinityGroupDao.findByAccountAndNames(5L, "cks-abc-worker", "cks-abc-control", "cks-abc-etcd"))
                .thenReturn(List.of(worker));
        Mockito.when(kubernetesClusterAffinityGroupMapDao.listByAffinityGroupId(20L))
                .thenReturn(List.of(new KubernetesClusterAffinityGroupMapVO(2L, WORKER.name(), 20L)));

        kubernetesClusterManager.deleteDefaultAntiAffinityGroups(cluster);

        Mockito.verifyNoInteractions(affinityGroupService);
    }

    private KubernetesClusterVO mockClusterForScaleState(KubernetesCluster.State state, VirtualMachine.State controlVmState) {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(cluster.getState()).thenReturn(state);
        Mockito.lenient().when(cluster.getId()).thenReturn(1L);
        Mockito.lenient().when(cluster.getServiceOfferingId()).thenReturn(1L);
        Mockito.lenient().when(cluster.getWorkerNodeServiceOfferingId()).thenReturn(2L);
        Mockito.lenient().when(cluster.getNodeCount()).thenReturn(2L);
        Mockito.lenient().when(cluster.getTotalNodeCount()).thenReturn(2L);
        Mockito.lenient().when(cluster.getAutoscalingEnabled()).thenReturn(false);
        KubernetesClusterVmMapVO controlVmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.lenient().when(controlVmMap.getVmId()).thenReturn(100L);
        Mockito.lenient().when(controlVmMap.isControlNode()).thenReturn(true);
        KubernetesClusterVmMapVO workerVmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.lenient().when(workerVmMap.getVmId()).thenReturn(10L);
        Mockito.lenient().when(kubernetesClusterVmMapDao.listByClusterId(1L)).thenReturn(List.of(controlVmMap, workerVmMap));
        VMInstanceVO controlVm = Mockito.mock(VMInstanceVO.class);
        Mockito.lenient().when(controlVm.getState()).thenReturn(controlVmState);
        Mockito.lenient().when(vmInstanceDao.findById(100L)).thenReturn(controlVm);
        // The usual reason for Alert: a worker VM is stopped
        VMInstanceVO workerVm = Mockito.mock(VMInstanceVO.class);
        Mockito.lenient().when(workerVm.getState()).thenReturn(VirtualMachine.State.Stopped);
        Mockito.lenient().when(vmInstanceDao.findById(10L)).thenReturn(workerVm);
        return cluster;
    }

    private KubernetesClusterVO mockClusterForScaleState(KubernetesCluster.State state) {
        return mockClusterForScaleState(state, VirtualMachine.State.Running);
    }

    @Test
    public void testValidateKubernetesClusterStateForScaleAlertSize() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, null, null, null);
    }

    @Test
    public void testValidateKubernetesClusterStateForScaleAlertNodeIds() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, null, List.of(10L), null, null, null, null, null);
    }

    @Test
    public void testValidateKubernetesClusterStateForScaleAlertSizeWithCurrentSettings() {
        // The offerings and autoscaling setting the cluster already has may be sent along with the new size
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, 1L, Map.of(WORKER.name(), 2L), false, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertOfferingChange() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, 5L, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertNodeOfferingChange() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, Map.of(WORKER.name(), 5L), null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertAutoscaling() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, null, null, null, null, true, null, null);
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateKubernetesClusterStateForScaleAlertNothingToDo() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, null, null, 1L, null, null, null, null);
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateKubernetesClusterStateForScaleAlertSameSize() {
        // Would do nothing, then move the cluster from Alert to Running
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 2L, null, null, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertControlNodeDown() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert, VirtualMachine.State.Stopped);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertAutoscalingSizeChange() {
        // Autoscaling stays disabled, but minsize/maxsize differ: the worker treats it as an autoscaling change
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, false, 1L, 3L);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertExtraVms() {
        // For example a worker VM left by a failed scale-up
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        Mockito.when(cluster.getTotalNodeCount()).thenReturn(1L);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertAllVmsRunning() {
        // In Alert for another reason, such as a failed upgrade
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        VMInstanceVO workerVm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(workerVm.getState()).thenReturn(VirtualMachine.State.Running);
        Mockito.when(vmInstanceDao.findById(10L)).thenReturn(workerVm);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertNoVmDown() {
        // A VM on a disconnected host may still be running: the worker can't work around it
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        VMInstanceVO workerVm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(workerVm.getState()).thenReturn(VirtualMachine.State.Unknown);
        Mockito.when(vmInstanceDao.findById(10L)).thenReturn(workerVm);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertEtcdVmDown() {
        // etcd may have lost quorum, so new nodes can't join
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        Mockito.when(cluster.getTotalNodeCount()).thenReturn(3L);
        KubernetesClusterVmMapVO etcdVmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(etcdVmMap.getVmId()).thenReturn(200L);
        Mockito.when(etcdVmMap.isEtcdNode()).thenReturn(true);
        List<KubernetesClusterVmMapVO> vmMaps = new ArrayList<>(kubernetesClusterVmMapDao.listByClusterId(1L));
        vmMaps.add(etcdVmMap);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterId(1L)).thenReturn(vmMaps);
        VMInstanceVO etcdVm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(etcdVm.getState()).thenReturn(VirtualMachine.State.Stopped);
        Mockito.when(vmInstanceDao.findById(200L)).thenReturn(etcdVm);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertOnlyExternalNodeDown() {
        // Scaling never acts on external nodes, so it can't work around one that is down
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        KubernetesClusterVmMapVO workerVmMap = kubernetesClusterVmMapDao.listByClusterId(1L).get(1);
        Mockito.when(workerVmMap.isExternalNode()).thenReturn(true);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleAlertRemoveControlNode() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Alert);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, null, List.of(100L), null, null, null, null, null);
    }

    @Test
    public void testValidateKubernetesClusterStateForScaleRunningOfferingChange() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Running);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, null, null, 5L, null, null, null, null);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateKubernetesClusterStateForScaleUpgrading() {
        KubernetesClusterVO cluster = mockClusterForScaleState(KubernetesCluster.State.Upgrading);
        kubernetesClusterManager.validateKubernetesClusterStateForScale(cluster, 3L, null, null, null, null, null, null);
    }

    private static final String CLUSTER_TYPE = ApiCommandResourceType.KubernetesCluster.toString();

    private KubernetesClusterVO mockClusterInState(long id, KubernetesCluster.State state) {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Mockito.lenient().when(cluster.getId()).thenReturn(id);
        Mockito.lenient().when(cluster.getState()).thenReturn(state);
        Mockito.lenient().when(cluster.getUuid()).thenReturn("uuid-" + id);
        Mockito.lenient().when(kubernetesClusterDao.findById(id)).thenReturn(cluster);
        return cluster;
    }

    private AsyncJobVO mockPendingJob(Long instanceId) {
        AsyncJobVO job = Mockito.mock(AsyncJobVO.class);
        Mockito.when(job.getInstanceId()).thenReturn(instanceId);
        return job;
    }

    @Test
    public void recoverOrphanedOperationStatesFailsOnlyOperationsWithoutJob() {
        KubernetesClusterVO orphaned = mockClusterInState(1L, KubernetesCluster.State.Upgrading);
        KubernetesClusterVO busy = mockClusterInState(2L, KubernetesCluster.State.Scaling);
        Mockito.when(kubernetesClusterDao.findManagedKubernetesClustersInStates(KubernetesClusterManagerImpl.OPERATION_STATES)).thenReturn(List.of(orphaned, busy));
        AsyncJobVO job = mockPendingJob(2L);
        Mockito.when(asyncJobDao.findInstancePendingAsyncJobs(CLUSTER_TYPE, null)).thenReturn(List.of(job));
        Mockito.when(asyncJobDao.findInstancePendingAsyncJob(CLUSTER_TYPE, 2L)).thenReturn(job);
        Mockito.when(kubernetesClusterDao.updateState(KubernetesCluster.State.Upgrading, KubernetesCluster.Event.OperationFailed,
                KubernetesCluster.State.Alert, orphaned, null)).thenReturn(true);

        kubernetesClusterManager.recoverOrphanedOperationStates();

        Mockito.verify(kubernetesClusterDao, Mockito.times(1)).updateState(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void recoverOrphanedOperationStatesLeavesClusterNamedByJobOfOlderManagementServer() {
        KubernetesClusterVO named = mockClusterInState(1L, KubernetesCluster.State.Starting);
        KubernetesClusterVO orphaned = mockClusterInState(2L, KubernetesCluster.State.Upgrading);
        Mockito.when(kubernetesClusterDao.findManagedKubernetesClustersInStates(KubernetesClusterManagerImpl.OPERATION_STATES)).thenReturn(List.of(named, orphaned));
        AsyncJobVO job = mockPendingJob(null);
        Mockito.when(job.getCmdInfo()).thenReturn("{\"id\":\"uuid-1\"}");
        Mockito.when(asyncJobDao.findInstancePendingAsyncJobs(CLUSTER_TYPE, null)).thenReturn(List.of(job));
        Mockito.when(kubernetesClusterDao.updateState(KubernetesCluster.State.Upgrading, KubernetesCluster.Event.OperationFailed,
                KubernetesCluster.State.Alert, orphaned, null)).thenReturn(true);

        kubernetesClusterManager.recoverOrphanedOperationStates();

        Mockito.verify(kubernetesClusterDao, Mockito.times(1)).updateState(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(kubernetesClusterDao).updateState(KubernetesCluster.State.Upgrading, KubernetesCluster.Event.OperationFailed,
                KubernetesCluster.State.Alert, orphaned, null);
    }

    @Test
    public void recoverOrphanedOperationStatesLeavesEveryClusterForJobOfOlderManagementServerWithoutParameters() {
        KubernetesClusterVO cluster = mockClusterInState(1L, KubernetesCluster.State.Starting);
        Mockito.when(kubernetesClusterDao.findManagedKubernetesClustersInStates(KubernetesClusterManagerImpl.OPERATION_STATES)).thenReturn(List.of(cluster));
        AsyncJobVO job = mockPendingJob(null);
        Mockito.when(asyncJobDao.findInstancePendingAsyncJobs(CLUSTER_TYPE, null)).thenReturn(List.of(job));

        kubernetesClusterManager.recoverOrphanedOperationStates();

        Mockito.verify(kubernetesClusterDao, Mockito.never()).updateState(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void recoverOrphanedOperationStatesSkipsJobLookupWithoutClusters() {
        kubernetesClusterManager.recoverOrphanedOperationStates();

        Mockito.verifyNoInteractions(asyncJobDao);
    }

    @Test
    public void recoverOrphanedOperationStateLeavesClusterWhoseOperationEnded() throws Exception {
        mockClusterInState(1L, KubernetesCluster.State.Running);

        kubernetesClusterManager.recoverOrphanedOperationState(1L);

        Mockito.verifyNoInteractions(asyncJobDao);
        Mockito.verify(kubernetesClusterDao, Mockito.never()).updateState(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void recoverOrphanedOperationStateLeavesClusterWithJobStartedSinceTheScan() throws Exception {
        mockClusterInState(1L, KubernetesCluster.State.Scaling);
        Mockito.when(asyncJobDao.findInstancePendingAsyncJob(CLUSTER_TYPE, 1L)).thenReturn(Mockito.mock(AsyncJobVO.class));

        kubernetesClusterManager.recoverOrphanedOperationState(1L);

        Mockito.verify(kubernetesClusterDao, Mockito.never()).updateState(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void recoverOrphanedOperationStateFailsStart() throws Exception {
        KubernetesClusterVO cluster = mockClusterInState(1L, KubernetesCluster.State.Starting);
        Mockito.when(kubernetesClusterDao.updateState(KubernetesCluster.State.Starting, KubernetesCluster.Event.OperationFailed,
                KubernetesCluster.State.Alert, cluster, null)).thenReturn(true);

        kubernetesClusterManager.recoverOrphanedOperationState(1L);

        Mockito.verify(kubernetesClusterDao).updateState(KubernetesCluster.State.Starting, KubernetesCluster.Event.OperationFailed,
                KubernetesCluster.State.Alert, cluster, null);
    }

    private KubernetesCluster mockClusterForUpgradeRetry(String... nodeVersions) {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.when(cluster.getKubernetesVersionId()).thenReturn(10L);
        List<KubernetesClusterVmMapVO> vmMaps = new ArrayList<>();
        for (String nodeVersion : nodeVersions) {
            KubernetesClusterVmMapVO vmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
            Mockito.when(vmMap.getNodeVersion()).thenReturn(nodeVersion);
            vmMaps.add(vmMap);
        }
        Mockito.when(kubernetesClusterVmMapDao.listByClusterId(1L)).thenReturn(vmMaps);
        mockClusterVersion("1.28.9");
        return cluster;
    }

    private void mockClusterVersion(String semanticVersion) {
        KubernetesSupportedVersionVO clusterVersion = Mockito.mock(KubernetesSupportedVersionVO.class);
        Mockito.when(clusterVersion.getSemanticVersion()).thenReturn(semanticVersion);
        Mockito.when(kubernetesSupportedVersionDao.findById(10L)).thenReturn(clusterVersion);
    }

    private KubernetesSupportedVersion mockUpgradeVersion(long id) {
        KubernetesSupportedVersion version = Mockito.mock(KubernetesSupportedVersion.class);
        Mockito.when(version.getId()).thenReturn(id);
        Mockito.lenient().when(version.getSemanticVersion()).thenReturn("1.29.2");
        return version;
    }

    @Test
    public void testIsRetryOfFailedUpgradeWithNodeAtTargetVersion() {
        KubernetesCluster cluster = mockClusterForUpgradeRetry("v1.29.2", "1.28.9", "1.28.9");
        Assert.assertTrue(kubernetesClusterManager.isRetryOfFailedUpgrade(cluster, mockUpgradeVersion(11L)));
    }

    @Test
    public void testIsRetryOfFailedUpgradeWithNoNodeAtTargetVersion() {
        KubernetesCluster cluster = mockClusterForUpgradeRetry("1.28.9", "v1.28.9");
        Assert.assertFalse(kubernetesClusterManager.isRetryOfFailedUpgrade(cluster, mockUpgradeVersion(11L)));
    }

    @Test
    public void testIsRetryOfFailedUpgradeIgnoresManuallyUpgradedNodes() {
        KubernetesCluster cluster = mockClusterForUpgradeRetry("1.28.9");
        KubernetesClusterVmMapVO manualNode = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(manualNode.isManualUpgrade()).thenReturn(true);
        Mockito.lenient().when(manualNode.getNodeVersion()).thenReturn("v1.29.2");
        List<KubernetesClusterVmMapVO> vmMaps = new ArrayList<>(kubernetesClusterVmMapDao.listByClusterId(1L));
        vmMaps.add(manualNode);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterId(1L)).thenReturn(vmMaps);
        Assert.assertFalse(kubernetesClusterManager.isRetryOfFailedUpgrade(cluster, mockUpgradeVersion(11L)));
    }

    @Test
    public void testIsRetryOfFailedUpgradeToCurrentVersion() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getKubernetesVersionId()).thenReturn(10L);
        KubernetesSupportedVersion version = Mockito.mock(KubernetesSupportedVersion.class);
        Mockito.when(version.getId()).thenReturn(10L);
        Assert.assertFalse(kubernetesClusterManager.isRetryOfFailedUpgrade(cluster, version));
        Mockito.verifyNoInteractions(kubernetesClusterVmMapDao);
    }

    @Test
    public void testIsRetryOfFailedUpgradeUsesRecordedUpgradeVersion() {
        KubernetesCluster cluster = Mockito.mock(KubernetesCluster.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.when(cluster.getKubernetesVersionId()).thenReturn(10L);
        Mockito.when(kubernetesClusterDetailsDao.findDetail(1L, KubernetesClusterUpgradeWorker.UPGRADE_VERSION_DETAIL))
                .thenReturn(new KubernetesClusterDetailsVO(1L, KubernetesClusterUpgradeWorker.UPGRADE_VERSION_DETAIL, "11", false));
        mockClusterVersion("1.28.9");
        KubernetesSupportedVersion sameVersion = Mockito.mock(KubernetesSupportedVersion.class);
        Mockito.when(sameVersion.getId()).thenReturn(11L);
        Mockito.when(sameVersion.getSemanticVersion()).thenReturn("1.29.2");
        KubernetesSupportedVersion otherVersion = Mockito.mock(KubernetesSupportedVersion.class);
        Mockito.when(otherVersion.getId()).thenReturn(12L);
        Mockito.when(otherVersion.getSemanticVersion()).thenReturn("1.29.3");
        // No node has to be at the version, for example when the first control node failed
        KubernetesSupportedVersionVO failedVersion = Mockito.mock(KubernetesSupportedVersionVO.class);
        Mockito.when(failedVersion.getSemanticVersion()).thenReturn("1.29.2");
        Mockito.when(kubernetesSupportedVersionDao.findByIdIncludingRemoved(11L)).thenReturn(failedVersion);
        KubernetesSupportedVersion equivalentVersion = Mockito.mock(KubernetesSupportedVersion.class);
        Mockito.when(equivalentVersion.getId()).thenReturn(13L);
        Mockito.when(equivalentVersion.getSemanticVersion()).thenReturn("1.29.2");
        Assert.assertTrue(kubernetesClusterManager.isRetryOfFailedUpgrade(cluster, sameVersion));
        Assert.assertTrue(kubernetesClusterManager.isRetryOfFailedUpgrade(cluster, equivalentVersion));
        Assert.assertFalse(kubernetesClusterManager.isRetryOfFailedUpgrade(cluster, otherVersion));
        Mockito.verifyNoInteractions(kubernetesClusterVmMapDao);
    }

    @Test
    public void testIsRetryOfFailedUpgradeToSameSemanticVersion() {
        // Another version row with the cluster's own semantic version, with every node recorded at it
        KubernetesCluster cluster = mockClusterForUpgradeRetry("1.29.2", "1.29.2");
        mockClusterVersion("1.29.2");
        Assert.assertFalse(kubernetesClusterManager.isRetryOfFailedUpgrade(cluster, mockUpgradeVersion(11L)));
    }

    private RolePermission mockRolePermission(String rule) {
        RolePermission permission = Mockito.mock(RolePermission.class);
        Mockito.when(permission.getRule()).thenReturn(new Rule(rule));
        return permission;
    }

    private Role mockProjectKubernetesAccountRole(String description) {
        Role role = Mockito.mock(Role.class);
        Mockito.when(role.getDescription()).thenReturn(description);
        return role;
    }

    @Test
    public void addMissingProjectKubernetesAccountRolePermissionsAddsMissingApisBeforeDenyAll() {
        Role role = mockProjectKubernetesAccountRole(KubernetesClusterService.PROJECT_KUBEADMIN_ACCOUNT_ROLE_NAME);
        Mockito.when(role.getId()).thenReturn(1L);
        Mockito.when(roleService.isEnabled()).thenReturn(true);
        RolePermission listClusters = mockRolePermission("listKubernetesClusters");
        RolePermission adminDeny = mockRolePermission("createVolume");
        RolePermission denyAll = mockRolePermission("*");
        Mockito.when(roleService.findAllPermissionsBy(1L)).thenReturn(Arrays.asList(listClusters, adminDeny, denyAll));
        Mockito.when(roleService.updateRolePermission(Mockito.eq(role), Mockito.anyList())).thenReturn(true);
        Mockito.when(roleService.createRolePermission(Mockito.eq(role), Mockito.any(Rule.class),
                Mockito.eq(RolePermissionEntity.Permission.ALLOW), Mockito.anyString()))
                .thenAnswer(invocation -> mockRolePermission(invocation.getArgument(1, Rule.class).toString()));

        kubernetesClusterManager.addMissingProjectKubernetesAccountRolePermissions(role);

        List<String> created = Mockito.mockingDetails(roleService).getInvocations().stream()
                .filter(i -> i.getMethod().getName().equals("createRolePermission"))
                .map(i -> i.getArgument(1).toString())
                .collect(Collectors.toList());
        Assert.assertTrue(created.contains("attachVolume"));
        Assert.assertTrue(created.contains("createSnapshot"));
        Assert.assertFalse(created.contains("listKubernetesClusters"));
        Assert.assertFalse(created.contains("createVolume"));
        Assert.assertFalse(created.contains("*"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<RolePermission>> order = ArgumentCaptor.forClass(List.class);
        Mockito.verify(roleService).updateRolePermission(Mockito.eq(role), order.capture());
        Assert.assertSame(adminDeny, order.getValue().get(1));
        List<String> rules = order.getValue().stream().map(p -> p.getRule().toString()).collect(Collectors.toList());
        Assert.assertEquals(3 + created.size(), rules.size());
        Assert.assertEquals("listKubernetesClusters", rules.get(0));
        Assert.assertEquals("createVolume", rules.get(1));
        Assert.assertEquals(created, rules.subList(2, rules.size() - 1));
        Assert.assertEquals("*", rules.get(rules.size() - 1));
    }

    @Test
    public void addMissingProjectKubernetesAccountRolePermissionsNothingMissing() {
        Role role = mockProjectKubernetesAccountRole(KubernetesClusterService.PROJECT_KUBEADMIN_ACCOUNT_ROLE_NAME);
        Mockito.when(role.getId()).thenReturn(1L);
        Mockito.when(roleService.isEnabled()).thenReturn(true);
        RolePermission denyAll = mockRolePermission("*");
        List<RolePermission> permissions = new ArrayList<>();
        for (Class<?> api : KubernetesClusterManagerImpl.PROJECT_KUBERNETES_ACCOUNT_ROLE_ALLOWED_APIS) {
            permissions.add(mockRolePermission(BaseCmd.getCommandNameByClass(api)));
        }
        permissions.add(denyAll);
        Mockito.when(roleService.findAllPermissionsBy(1L)).thenReturn(permissions);

        kubernetesClusterManager.addMissingProjectKubernetesAccountRolePermissions(role);

        Mockito.verify(roleService, Mockito.never()).createRolePermission(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(roleService, Mockito.never()).updateRolePermission(Mockito.any(), Mockito.anyList());
    }

    @Test
    public void addMissingProjectKubernetesAccountRolePermissionsNoDenyAllRule() {
        Role role = mockProjectKubernetesAccountRole(KubernetesClusterService.PROJECT_KUBEADMIN_ACCOUNT_ROLE_NAME);
        Mockito.when(role.getId()).thenReturn(1L);
        Mockito.when(roleService.isEnabled()).thenReturn(true);
        RolePermission listClusters = mockRolePermission("LISTKUBERNETESCLUSTERS");
        Mockito.when(roleService.findAllPermissionsBy(1L)).thenReturn(Collections.singletonList(listClusters));
        Mockito.when(roleService.createRolePermission(Mockito.eq(role), Mockito.any(Rule.class),
                Mockito.eq(RolePermissionEntity.Permission.ALLOW), Mockito.anyString()))
                .thenAnswer(invocation -> mockRolePermission(invocation.getArgument(1, Rule.class).toString()));

        kubernetesClusterManager.addMissingProjectKubernetesAccountRolePermissions(role);

        Mockito.verify(roleService, Mockito.times(KubernetesClusterManagerImpl.PROJECT_KUBERNETES_ACCOUNT_ROLE_ALLOWED_APIS.size() - 1))
                .createRolePermission(Mockito.eq(role), Mockito.any(Rule.class), Mockito.any(), Mockito.anyString());
        Mockito.verify(roleService, Mockito.never()).updateRolePermission(Mockito.any(), Mockito.anyList());
    }

    @Test
    public void addMissingProjectKubernetesAccountRolePermissionsSkipsRoleNotCreatedByCks() {
        Role role = mockProjectKubernetesAccountRole("Role made by an admin");
        Mockito.when(roleService.isEnabled()).thenReturn(true);

        kubernetesClusterManager.addMissingProjectKubernetesAccountRolePermissions(role);

        Mockito.verify(roleService, Mockito.never()).findAllPermissionsBy(Mockito.anyLong());
    }

    @Test
    public void addMissingProjectKubernetesAccountRolePermissionsSkipsWhenRolesDisabled() {
        Role role = Mockito.mock(Role.class);
        Mockito.when(roleService.isEnabled()).thenReturn(false);

        kubernetesClusterManager.addMissingProjectKubernetesAccountRolePermissions(role);

        Mockito.verify(roleService, Mockito.never()).findAllPermissionsBy(Mockito.anyLong());
    }

    @Test
    public void testCheckIfVmsAssociatedWithBackupOfferingSkipsMissingVms() {
        VMInstanceVO vm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(vm.getBackupOfferingId()).thenReturn(null);
        Assert.assertFalse(KubernetesClusterManagerImpl.checkIfVmsAssociatedWithBackupOffering(Arrays.asList(null, vm)));
        Mockito.when(vm.getBackupOfferingId()).thenReturn(1L);
        Assert.assertTrue(KubernetesClusterManagerImpl.checkIfVmsAssociatedWithBackupOffering(Arrays.asList(null, vm)));
    }

    private KubernetesClusterVmMapVO clusterNode(long vmId, boolean control, boolean etcd, boolean external) {
        KubernetesClusterVmMapVO map = new KubernetesClusterVmMapVO(1L, vmId, control);
        map.setEtcdNode(etcd);
        map.setExternalNode(external);
        return map;
    }

    private void assertNodeNotRemovable(long vmId, List<KubernetesClusterVmMapVO> clusterNodes, String expectedMessage) {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.when(cluster.getUuid()).thenReturn("cks");
        VMInstanceVO vm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(vm.getUuid()).thenReturn("vm-uuid");
        Mockito.when(vmInstanceDao.findById(vmId)).thenReturn(vm);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmIdsIn(1L, List.of(vmId))).thenReturn(clusterNodes);
        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> kubernetesClusterManager.validateNodesRemovable(cluster, List.of(vmId)));
        Assert.assertTrue(e.getMessage().contains("vm-uuid"));
        Assert.assertTrue(e.getMessage().contains(expectedMessage));
    }

    @Test
    public void validateNodesRemovableAcceptsAddedWorker() {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmIdsIn(1L, List.of(10L, 11L)))
                .thenReturn(List.of(clusterNode(10L, false, false, true), clusterNode(11L, false, false, true)));
        kubernetesClusterManager.validateNodesRemovable(cluster, List.of(10L, 11L));
        Mockito.verify(vmInstanceDao, Mockito.never()).findById(Mockito.anyLong());
    }

    @Test
    public void validateNodesRemovableRejectsVmNotInCluster() {
        assertNodeNotRemovable(10L, Collections.emptyList(), "is not a node of");
    }

    @Test
    public void validateNodesRemovableRejectsControlNode() {
        assertNodeNotRemovable(10L, List.of(clusterNode(10L, true, false, false)), "is a control node");
    }

    @Test
    public void validateNodesRemovableRejectsEtcdNode() {
        assertNodeNotRemovable(10L, List.of(clusterNode(10L, false, true, false)), "is an etcd node");
    }

    @Test
    public void validateNodesRemovableRejectsExternalControlNode() {
        assertNodeNotRemovable(10L, List.of(clusterNode(10L, true, false, true)), "is a control node");
    }

    @Test
    public void validateNodesRemovableRejectsExternalEtcdNode() {
        assertNodeNotRemovable(10L, List.of(clusterNode(10L, false, true, true)), "is an etcd node");
    }

    @Test
    public void validateNodesRemovableRejectsCksManagedWorker() {
        assertNodeNotRemovable(10L, List.of(clusterNode(10L, false, false, false)), "use scaleKubernetesCluster");
    }

    @Test
    public void validateNodesRemovableRejectsWhenAnyNodeIsNotRemovable() {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(cluster.getId()).thenReturn(1L);
        Mockito.when(cluster.getUuid()).thenReturn("cks");
        VMInstanceVO vm = Mockito.mock(VMInstanceVO.class);
        Mockito.when(vm.getUuid()).thenReturn("other-vm-uuid");
        Mockito.when(vmInstanceDao.findById(11L)).thenReturn(vm);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmIdsIn(1L, List.of(10L, 11L)))
                .thenReturn(List.of(clusterNode(10L, false, false, true)));
        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> kubernetesClusterManager.validateNodesRemovable(cluster, List.of(10L, 11L)));
        Assert.assertTrue(e.getMessage().contains("other-vm-uuid"));
    }

    private RemoveNodesFromKubernetesClusterCmd mockRemoveNodesCmd(KubernetesCluster.ClusterType clusterType, List<Long> nodeIds) {
        KubernetesClusterVO cluster = Mockito.mock(KubernetesClusterVO.class);
        Mockito.when(cluster.getClusterType()).thenReturn(clusterType);
        Mockito.when(kubernetesClusterDao.findById(1L)).thenReturn(cluster);
        for (Long id : nodeIds) {
            VMInstanceVO vm = Mockito.mock(VMInstanceVO.class);
            Mockito.lenient().when(vm.getUuid()).thenReturn("vm-uuid-" + id);
            Mockito.when(vmInstanceDao.findById(id)).thenReturn(vm);
        }
        RemoveNodesFromKubernetesClusterCmd cmd = Mockito.mock(RemoveNodesFromKubernetesClusterCmd.class);
        Mockito.when(cmd.getClusterId()).thenReturn(1L);
        Mockito.when(cmd.getNodeIds()).thenReturn(nodeIds);
        Mockito.when(cmd.getActualCommandName()).thenReturn(BaseCmd.getCommandNameByClass(RemoveNodesFromKubernetesClusterCmd.class));
        return cmd;
    }

    @Test
    public void removeNodesFromKubernetesClusterRejectsControlNode() {
        RemoveNodesFromKubernetesClusterCmd cmd = mockRemoveNodesCmd(KubernetesCluster.ClusterType.CloudManaged, List.of(10L));
        Mockito.when(kubernetesClusterDao.findById(1L).getId()).thenReturn(1L);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmIdsIn(1L, List.of(10L)))
                .thenReturn(List.of(clusterNode(10L, true, false, false)));
        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> kubernetesClusterManager.removeNodesFromKubernetesCluster(cmd));
        Assert.assertTrue(e.getMessage().contains("vm-uuid-10 is a control node"));
    }

    @Test
    public void removeNodesFromKubernetesClusterRejectsExternalManagedCluster() {
        RemoveNodesFromKubernetesClusterCmd cmd = mockRemoveNodesCmd(KubernetesCluster.ClusterType.ExternalManaged, List.of(10L));
        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> kubernetesClusterManager.removeNodesFromKubernetesCluster(cmd));
        Assert.assertTrue(e.getMessage().contains("ExternalManaged"));
        Mockito.verify(kubernetesClusterVmMapDao, Mockito.never()).listByClusterIdAndVmIdsIn(Mockito.anyLong(), Mockito.anyList());
    }

    @Test
    public void testSetCniConfigResponseMissingConfig() {
        KubernetesClusterResponse response = Mockito.mock(KubernetesClusterResponse.class);
        Mockito.when(userDataDao.findByIdIncludingRemoved(5L)).thenReturn(null);
        kubernetesClusterManager.setCniConfigResponse(response, 5L);
        Mockito.verify(response, Mockito.never()).setCniConfigId(Mockito.any());
        Mockito.verify(response, Mockito.never()).setCniConfigName(Mockito.any());
    }

    @Test
    public void testSetCniConfigResponseRemovedConfig() {
        KubernetesClusterResponse response = Mockito.mock(KubernetesClusterResponse.class);
        UserDataVO cniConfig = Mockito.mock(UserDataVO.class);
        Mockito.when(cniConfig.getUuid()).thenReturn("cni-uuid");
        Mockito.when(cniConfig.getName()).thenReturn("cni-name");
        Mockito.when(userDataDao.findByIdIncludingRemoved(5L)).thenReturn(cniConfig);
        kubernetesClusterManager.setCniConfigResponse(response, 5L);
        Mockito.verify(response).setCniConfigId("cni-uuid");
        Mockito.verify(response).setCniConfigName("cni-name");
    }

    @Test
    public void testSetCniConfigResponseNoConfig() {
        KubernetesClusterResponse response = Mockito.mock(KubernetesClusterResponse.class);
        kubernetesClusterManager.setCniConfigResponse(response, null);
        Mockito.verifyNoInteractions(userDataDao, response);
    }

    @Test
    public void testValidateCniConfigNotSpecified() {
        kubernetesClusterManager.validateCniConfigForKubernetesCreateParameters(null);
        Mockito.verifyNoInteractions(userDataDao);
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testValidateCniConfigMissing() {
        Mockito.when(userDataDao.findById(5L)).thenReturn(null);
        kubernetesClusterManager.validateCniConfigForKubernetesCreateParameters(5L);
    }

    @Test(expected = PermissionDeniedException.class)
    public void testValidateCniConfigNoAccess() {
        UserDataVO cniConfig = Mockito.mock(UserDataVO.class);
        Mockito.when(userDataDao.findById(5L)).thenReturn(cniConfig);
        Mockito.doThrow(new PermissionDeniedException("denied")).when(accountManager).checkAccess(
                Mockito.eq(CallContext.current().getCallingAccount()), Mockito.isNull(), Mockito.eq(false), Mockito.eq(cniConfig));
        kubernetesClusterManager.validateCniConfigForKubernetesCreateParameters(5L);
    }

    @Test
    public void testValidateCniConfigAccessible() {
        UserDataVO cniConfig = Mockito.mock(UserDataVO.class);
        Mockito.when(userDataDao.findById(5L)).thenReturn(cniConfig);
        kubernetesClusterManager.validateCniConfigForKubernetesCreateParameters(5L);
        Mockito.verify(accountManager).checkAccess(CallContext.current().getCallingAccount(), null, false, cniConfig);
    }

    private KubernetesClusterVmMapVO clusterVmMap(long vmId, boolean etcdNode) {
        KubernetesClusterVmMapVO map = new KubernetesClusterVmMapVO(1L, vmId, false);
        map.setEtcdNode(etcdNode);
        return map;
    }

    private PortForwardingRuleVO portForwardingRule(long ipId, int publicPort, int privatePort, String protocol, long vmId) {
        PortForwardingRuleVO rule = new PortForwardingRuleVO(null, ipId, publicPort, new Ip("10.1.1.10"), privatePort, protocol, 1L, 1L, 1L, vmId);
        rule.setState(FirewallRule.State.Active);
        return rule;
    }

    private void mockIpAddress(long id, String address) {
        IPAddressVO ip = Mockito.mock(IPAddressVO.class);
        Mockito.when(ip.getUuid()).thenReturn("ip-" + id);
        Mockito.when(ip.getAddress()).thenReturn(new Ip(address));
        Mockito.when(ipAddressDao.findById(id)).thenReturn(ip);
    }

    @Test
    public void testGetEtcdIpsFindsRulesOutsideCurrentEtcdStartPort() {
        Mockito.when(pfRuleDao.listByVm(101L)).thenReturn(List.of(portForwardingRule(10L, 60000, 22, NetUtils.TCP_PROTO, 101L)));
        Mockito.when(pfRuleDao.listByVm(102L)).thenReturn(List.of(portForwardingRule(10L, 60001, 22, NetUtils.TCP_PROTO, 102L)));
        mockIpAddress(10L, "203.0.113.10");

        Map<String, String> etcdIps = kubernetesClusterManager.getEtcdIps(List.of(clusterVmMap(100L, false), clusterVmMap(101L, true), clusterVmMap(102L, true)));

        Assert.assertEquals(Map.of("ip-10", "203.0.113.10"), etcdIps);
        Mockito.verify(pfRuleDao, Mockito.never()).listByVm(100L);
    }

    @Test
    public void testGetEtcdIpsUsesOnlyUnrevokedTcpRulesToSshPort() {
        PortForwardingRuleVO revoked = portForwardingRule(13L, 50003, 22, NetUtils.TCP_PROTO, 101L);
        revoked.setState(FirewallRule.State.Revoke);
        PortForwardingRuleVO notYetApplied = portForwardingRule(14L, 50004, 22, NetUtils.TCP_PROTO, 101L);
        notYetApplied.setState(FirewallRule.State.Add);
        PortForwardingRuleVO portRange = new PortForwardingRuleVO(null, 15L, 50005, 50013, new Ip("10.1.1.10"), 22, 30, NetUtils.TCP_PROTO, 1L, 1L, 1L, 101L);
        Mockito.when(pfRuleDao.listByVm(101L)).thenReturn(List.of(
                portForwardingRule(11L, 50000, 2379, NetUtils.TCP_PROTO, 101L),
                portForwardingRule(12L, 50001, 22, NetUtils.UDP_PROTO, 101L),
                revoked,
                portRange,
                notYetApplied,
                portForwardingRule(10L, 50002, 22, NetUtils.TCP_PROTO, 101L)));
        mockIpAddress(10L, "203.0.113.10");
        mockIpAddress(14L, "203.0.113.14");

        Map<String, String> etcdIps = kubernetesClusterManager.getEtcdIps(List.of(clusterVmMap(101L, true)));

        Assert.assertEquals(Map.of("ip-10", "203.0.113.10", "ip-14", "203.0.113.14"), etcdIps);
        for (long excludedIpId : List.of(11L, 12L, 13L, 15L)) {
            Mockito.verify(ipAddressDao, Mockito.never()).findById(excludedIpId);
        }
    }

    @Test
    public void testGetEtcdIpsSkipsMissingIpAddress() {
        Mockito.when(pfRuleDao.listByVm(101L)).thenReturn(List.of(portForwardingRule(10L, 50000, 22, NetUtils.TCP_PROTO, 101L)));
        Mockito.when(pfRuleDao.listByVm(102L)).thenReturn(List.of(portForwardingRule(11L, 50001, 22, NetUtils.TCP_PROTO, 102L)));
        mockIpAddress(11L, "203.0.113.11");

        Map<String, String> etcdIps = kubernetesClusterManager.getEtcdIps(List.of(clusterVmMap(101L, true), clusterVmMap(102L, true)));

        Assert.assertEquals(Map.of("ip-11", "203.0.113.11"), etcdIps);
    }
}
