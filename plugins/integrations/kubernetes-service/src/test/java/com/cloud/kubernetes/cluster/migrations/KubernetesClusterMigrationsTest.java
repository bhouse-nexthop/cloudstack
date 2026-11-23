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

import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.CONTROL;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.ETCD;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.WORKER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.cloud.kubernetes.cluster.KubernetesClusterDetailsVO;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterService;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterActionWorker;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDetailsDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.network.dao.NetworkDao;
import com.cloud.network.dao.NetworkVO;
import com.cloud.service.ServiceOfferingVO;
import com.cloud.service.dao.ServiceOfferingDao;
import com.cloud.storage.VMTemplateVO;
import com.cloud.storage.dao.VMTemplateDao;
import com.cloud.utils.Pair;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.dao.UserVmDao;

@RunWith(MockitoJUnitRunner.Silent.class)
public class KubernetesClusterMigrationsTest {

    private static final long CLUSTER_ID = 1L;
    private static final long ZONE_ID = 2L;
    private static final long OFFERING = 10L;
    private static final long TEMPLATE = 20L;
    private static final long WORKER_TEMPLATE = 21L;
    private static final long NETWORK_ID = 30L;

    @Mock
    KubernetesClusterManagerImpl manager;
    @Mock
    KubernetesClusterVmMapDao vmMapDao;
    @Mock
    UserVmDao userVmDao;
    @Mock
    ServiceOfferingDao serviceOfferingDao;
    @Mock
    VMTemplateDao templateDao;
    @Mock
    KubernetesClusterVO cluster;
    @Mock
    ServiceOfferingVO offering;
    @Mock
    VMTemplateVO template;
    @Mock
    VMTemplateVO workerTemplate;
    @Mock
    KubernetesClusterRollingUpdateWorker worker;
    @Mock
    KubernetesClusterDetailsDao detailsDao;
    @Mock
    NetworkDao networkDao;
    @Mock
    NetworkVO network;
    @Mock
    com.cloud.vm.dao.VMInstanceDetailsDao vmDetailsDao;

    final List<KubernetesClusterVmMapVO> maps = new ArrayList<>();
    DynamicScalingMigration dynamicScaling;
    boolean zoneAllowsDynamicScaling = true;
    long nextVmId = 100;

    @Before
    public void setUp() {
        manager.kubernetesClusterVmMapDao = vmMapDao;
        Mockito.when(cluster.getId()).thenReturn(CLUSTER_ID);
        Mockito.when(cluster.getZoneId()).thenReturn(ZONE_ID);
        Mockito.when(vmMapDao.listByClusterId(CLUSTER_ID)).thenReturn(maps);
        Mockito.when(manager.getNodeTypeServiceOfferingId(Mockito.eq(cluster), Mockito.any())).thenReturn(OFFERING);
        Mockito.when(manager.getNodeTypeTemplateId(Mockito.eq(cluster), Mockito.any())).thenReturn(null);
        Mockito.when(serviceOfferingDao.findByIdIncludingRemoved(OFFERING)).thenReturn(offering);
        Mockito.when(offering.isDynamicScalingEnabled()).thenReturn(true);
        Mockito.when(templateDao.findByIdIncludingRemoved(TEMPLATE)).thenReturn(template);
        Mockito.when(templateDao.findByIdIncludingRemoved(WORKER_TEMPLATE)).thenReturn(workerTemplate);
        Mockito.when(template.isDynamicallyScalable()).thenReturn(true);
        Mockito.when(template.getName()).thenReturn("template");
        Mockito.when(workerTemplate.isDynamicallyScalable()).thenReturn(true);
        dynamicScaling = Mockito.spy(new DynamicScalingMigration(manager));
        dynamicScaling.userVmDao = userVmDao;
        dynamicScaling.serviceOfferingDao = serviceOfferingDao;
        dynamicScaling.templateDao = templateDao;
        Mockito.doAnswer(i -> zoneAllowsDynamicScaling).when(dynamicScaling).isDynamicScalingEnabled(ZONE_ID);
    }

    private UserVmVO addNode(boolean control, boolean scalable) {
        long id = nextVmId++;
        maps.add(new KubernetesClusterVmMapVO(CLUSTER_ID, id, control));
        UserVmVO vm = Mockito.mock(UserVmVO.class);
        Mockito.when(vm.getId()).thenReturn(id);
        Mockito.when(vm.getHostName()).thenReturn("node" + id);
        Mockito.when(vm.getServiceOfferingId()).thenReturn(OFFERING);
        Mockito.when(vm.getTemplateId()).thenReturn(TEMPLATE);
        Mockito.when(vm.isDynamicallyScalable()).thenReturn(scalable);
        Mockito.when(userVmDao.findById(id)).thenReturn(vm);
        return vm;
    }

    // gen 1 to gen 2

    @Test
    public void nodeTypeSettingsVerifiesEveryNodeTypeHasAnOffering() {
        NodeTypeSettingsMigration migration = new NodeTypeSettingsMigration(manager);
        Mockito.when(manager.getNodeTypeServiceOfferingIdIfSet(cluster, CONTROL)).thenReturn(OFFERING);
        Mockito.when(manager.getNodeTypeServiceOfferingIdIfSet(cluster, WORKER)).thenReturn(null);
        Mockito.when(manager.getNodeTypeServiceOfferingIdIfSet(cluster, ETCD)).thenReturn(null);

        Assert.assertFalse(migration.verify(cluster, null));

        Mockito.when(manager.getNodeTypeServiceOfferingIdIfSet(cluster, WORKER)).thenReturn(OFFERING);
        Assert.assertTrue(migration.verify(cluster, null));

        Mockito.when(cluster.getEtcdNodeCount()).thenReturn(3L);
        Assert.assertFalse(migration.verify(cluster, null));
        Mockito.when(manager.getNodeTypeServiceOfferingIdIfSet(cluster, ETCD)).thenReturn(OFFERING);
        Assert.assertTrue(migration.verify(cluster, null));
    }

    @Test
    public void nodeTypeSettingsNeedsAnOfferingToCopy() {
        NodeTypeSettingsMigration migration = new NodeTypeSettingsMigration(manager);
        Mockito.when(cluster.getServiceOfferingId()).thenReturn(null);
        Mockito.when(manager.getNodeTypeServiceOfferingIdIfSet(Mockito.eq(cluster), Mockito.any())).thenReturn(null);

        Assert.assertNotNull(migration.checkPrecondition(cluster));
        Mockito.when(cluster.getServiceOfferingId()).thenReturn(OFFERING);
        Assert.assertNull(migration.checkPrecondition(cluster));
    }

    @Test
    public void nodeTypeSettingsConvertsTheCluster() {
        new NodeTypeSettingsMigration(manager).applyToDatabase(cluster);

        Mockito.verify(manager).convertToNodeTypeSettings(cluster);
    }

    // dynamic scaling

    @Test
    public void dynamicScalingPreconditionIsMetWhenEverythingAllowsIt() {
        addNode(true, false);
        addNode(true, false);
        addNode(true, false);
        addNode(false, false);

        Assert.assertNull(dynamicScaling.checkPrecondition(cluster));
    }

    @Test
    public void dynamicScalingNeedsNodesThatCanBeRestartedOneAtATime() {
        addNode(true, false);
        addNode(false, false);

        Assert.assertTrue(dynamicScaling.checkPrecondition(cluster).contains("1 control node(s) can not be restarted one at a time"));
    }

    @Test
    public void dynamicScalingNeedsNothingWhenEveryNodeIsScalable() {
        addNode(true, true);
        addNode(false, true);
        zoneAllowsDynamicScaling = false;

        Assert.assertNull(dynamicScaling.checkPrecondition(cluster));
    }

    @Test
    public void dynamicScalingNeedsTheZoneSetting() {
        addNode(false, false);
        zoneAllowsDynamicScaling = false;

        Assert.assertTrue(dynamicScaling.checkPrecondition(cluster).contains("enable.dynamic.scale.vm"));
    }

    @Test
    public void dynamicScalingNeedsAnOfferingThatAllowsIt() {
        addNode(false, false);
        Mockito.when(offering.isDynamicScalingEnabled()).thenReturn(false);

        Assert.assertTrue(dynamicScaling.checkPrecondition(cluster).contains("does not allow dynamic scaling"));
    }

    @Test
    public void dynamicScalingNeedsAScalableTemplate() {
        addNode(false, false);
        Mockito.when(template.isDynamicallyScalable()).thenReturn(false);

        Assert.assertTrue(dynamicScaling.checkPrecondition(cluster).contains("template template"));
    }

    @Test
    public void dynamicScalingChecksTheTemplateOfTheWorkerTypeTheNodeIsRecreatedFrom() {
        addNode(false, false);
        Mockito.when(template.isDynamicallyScalable()).thenReturn(false);
        Mockito.when(manager.getNodeTypeTemplateId(cluster, WORKER)).thenReturn(WORKER_TEMPLATE);

        Assert.assertNull(dynamicScaling.checkPrecondition(cluster));
    }

    @Test
    public void dynamicScalingIgnoresNodesThatAreScalableAndExternalNodes() {
        addNode(false, true);
        KubernetesClusterVmMapVO external = new KubernetesClusterVmMapVO(CLUSTER_ID, 999L, false);
        external.setExternalNode(true);
        maps.add(external);
        Mockito.when(offering.isDynamicScalingEnabled()).thenReturn(false);

        Assert.assertNull(dynamicScaling.checkPrecondition(cluster));
        Assert.assertTrue(dynamicScaling.verify(cluster, null));
        Mockito.verify(userVmDao, Mockito.never()).findById(999L);
    }

    @Test
    public void dynamicScalingMarksTheStoppedVm() {
        UserVmVO vm = addNode(false, false);

        Assert.assertTrue(dynamicScaling.isPendingOnNode(cluster, vm));
        Assert.assertFalse(dynamicScaling.verify(cluster, null));
        dynamicScaling.applyToStoppedNode(cluster, vm);

        Mockito.verify(vm).setDynamicallyScalable(true);
        Mockito.verify(userVmDao).update(vm.getId(), vm);
    }

    @Test
    public void dynamicScalingRefusesAVmItCanNotMakeScalable() {
        UserVmVO vm = addNode(false, false);
        Mockito.when(template.isDynamicallyScalable()).thenReturn(false);

        Assert.assertThrows(CloudRuntimeException.class, () -> dynamicScaling.applyToStoppedNode(cluster, vm));
        Mockito.verify(vm, Mockito.never()).setDynamicallyScalable(true);
    }

    // Headlamp

    @Test
    public void headlampDeletesTheDashboardWhenHeadlampRuns() {
        Mockito.when(worker.runKubectl(HeadlampMigration.HEADLAMP_COMMAND)).thenReturn("replicas=1.sudo: unable to resolve host x\n");
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_PHASE_COMMAND)).thenReturn("Active");
        Mockito.when(worker.runKubectl(HeadlampMigration.DELETE_DASHBOARD_COMMAND)).thenReturn("namespace \"kubernetes-dashboard\" deleted");

        new HeadlampMigration(manager).applyToCluster(worker);

        Mockito.verify(worker).runKubectl(HeadlampMigration.DELETE_DASHBOARD_COMMAND);
    }

    @Test
    public void headlampHasNothingToDoWithoutTheDashboard() {
        Mockito.when(worker.runKubectl(HeadlampMigration.HEADLAMP_COMMAND)).thenReturn(null);
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_PHASE_COMMAND)).thenReturn("");
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_RBAC_COMMAND)).thenReturn("");

        new HeadlampMigration(manager).applyToCluster(worker);

        Mockito.verify(worker, Mockito.never()).runKubectl(HeadlampMigration.DELETE_DASHBOARD_COMMAND);
    }

    @Test
    public void headlampLeavesTheDashboardWhileHeadlampHasNoReplicaAvailable() {
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_PHASE_COMMAND)).thenReturn("Active");
        Mockito.when(worker.runKubectl(HeadlampMigration.HEADLAMP_COMMAND)).thenReturn("replicas=0.sudo: unable to resolve host x\n");

        Assert.assertThrows(CloudRuntimeException.class, () -> new HeadlampMigration(manager).applyToCluster(worker));
        Mockito.verify(worker, Mockito.never()).runKubectl(HeadlampMigration.DELETE_DASHBOARD_COMMAND);
    }

    @Test
    public void headlampLeavesTheDashboardWithoutHeadlamp() {
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_PHASE_COMMAND)).thenReturn("Active");
        Mockito.when(worker.runKubectl(HeadlampMigration.HEADLAMP_COMMAND)).thenReturn("replicas=.");

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> new HeadlampMigration(manager).applyToCluster(worker));

        Assert.assertTrue(e.getMessage(), e.getMessage().contains("Upgrade"));
        Mockito.verify(worker, Mockito.never()).runKubectl(HeadlampMigration.DELETE_DASHBOARD_COMMAND);
    }

    @Test
    public void headlampVerifiesTheDashboardIsGoneOrGoing() {
        HeadlampMigration migration = new HeadlampMigration(manager);
        Mockito.when(worker.runKubectl(HeadlampMigration.HEADLAMP_COMMAND)).thenReturn("replicas=1.sudo: unable to resolve host x\n");

        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_RBAC_COMMAND)).thenReturn("");
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_PHASE_COMMAND)).thenReturn("");
        Assert.assertTrue(migration.verify(cluster, worker));
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_PHASE_COMMAND)).thenReturn("Terminating");
        Assert.assertTrue(migration.verify(cluster, worker));
        // the cluster wide access of the dashboard is left
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_RBAC_COMMAND)).thenReturn("clusterrolebinding.rbac.authorization.k8s.io/kubernetes-dashboard-ui");
        Assert.assertFalse(migration.verify(cluster, worker));
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_RBAC_COMMAND)).thenReturn("");
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_PHASE_COMMAND)).thenReturn("Active");
        Assert.assertFalse(migration.verify(cluster, worker));
        Mockito.when(worker.runKubectl(HeadlampMigration.DASHBOARD_PHASE_COMMAND)).thenReturn(null);
        Assert.assertFalse(migration.verify(cluster, worker));
        Assert.assertFalse(migration.verify(cluster, null));
    }

    // the bootstrap token derived from the cluster UUID

    private static final String UUID = "0123abcd-4567-89ef-0123-456789abcdef";
    private static final String TOKEN_SECRET_COMMAND = String.format(DeleteForeverJoinTokenMigration.TOKEN_SECRET_COMMAND, "0123ab");
    private static final String DELETE_TOKEN_COMMAND = String.format(DeleteForeverJoinTokenMigration.DELETE_TOKEN_COMMAND, "0123ab");

    private static String tokenSecretOutput(String secret) {
        return "sudo: unable to resolve host x\nsecret=" + org.apache.commons.codec.binary.Base64.encodeBase64String(secret.getBytes()) + ".";
    }

    private void withUuidToken() {
        Mockito.when(cluster.getUuid()).thenReturn(UUID);
        Mockito.when(worker.getKubernetesCluster()).thenReturn(cluster);
    }

    @Test
    public void uuidTokenIsTheOneOlderVersionsGaveTheCluster() {
        Mockito.when(cluster.getUuid()).thenReturn(UUID);

        Assert.assertArrayEquals(new String[] {"0123ab", "cd456789ef012345"}, DeleteForeverJoinTokenMigration.getUuidToken(cluster));
    }

    @Test
    public void uuidTokenIsDeletedWhenItsSecretMatches() {
        withUuidToken();
        Mockito.when(worker.runKubectl(TOKEN_SECRET_COMMAND)).thenReturn(tokenSecretOutput("cd456789ef012345"));
        Mockito.when(worker.runKubectl(DELETE_TOKEN_COMMAND)).thenReturn("secret \"bootstrap-token-0123ab\" deleted");

        new DeleteForeverJoinTokenMigration(manager).applyToCluster(worker);

        Mockito.verify(worker).runKubectl(DELETE_TOKEN_COMMAND);
    }

    @Test
    public void uuidTokenIsKeptWhileANodeHasNotRegistered() {
        withUuidToken();
        Mockito.when(worker.runKubectl(TOKEN_SECRET_COMMAND)).thenReturn(tokenSecretOutput("cd456789ef012345"));
        Mockito.when(worker.getUnregisteredNodes()).thenReturn(List.of("w2"));

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> new DeleteForeverJoinTokenMigration(manager).applyToCluster(worker));

        Assert.assertTrue(e.getMessage(), e.getMessage().contains("w2"));
        Mockito.verify(worker, Mockito.never()).runKubectl(DELETE_TOKEN_COMMAND);
    }

    @Test
    public void tokenOfTheSameIdWithAnotherSecretIsLeftAlone() {
        withUuidToken();
        DeleteForeverJoinTokenMigration migration = new DeleteForeverJoinTokenMigration(manager);
        Mockito.when(worker.runKubectl(TOKEN_SECRET_COMMAND)).thenReturn(tokenSecretOutput("0000000000000000"));

        migration.applyToCluster(worker);

        Mockito.verify(worker, Mockito.never()).runKubectl(DELETE_TOKEN_COMMAND);
        Assert.assertTrue(migration.verify(cluster, worker));
    }

    @Test
    public void uuidTokenMigrationHasNothingToDoOnceTheTokenIsGone() {
        withUuidToken();
        DeleteForeverJoinTokenMigration migration = new DeleteForeverJoinTokenMigration(manager);
        Mockito.when(worker.runKubectl(TOKEN_SECRET_COMMAND)).thenReturn("sudo: unable to resolve host x\n");

        migration.applyToCluster(worker);
        migration.applyToCluster(worker);

        Mockito.verify(worker, Mockito.never()).runKubectl(DELETE_TOKEN_COMMAND);
        Assert.assertTrue(migration.verify(cluster, worker));
        Assert.assertNull(migration.checkPrecondition(cluster));
    }

    @Test
    public void uuidTokenMigrationFailsWhenKubectlDoes() {
        withUuidToken();
        DeleteForeverJoinTokenMigration migration = new DeleteForeverJoinTokenMigration(manager);
        Mockito.when(worker.runKubectl(TOKEN_SECRET_COMMAND)).thenReturn(null);

        Assert.assertThrows(CloudRuntimeException.class, () -> migration.applyToCluster(worker));
        Assert.assertFalse(migration.verify(cluster, worker));

        Mockito.when(worker.runKubectl(TOKEN_SECRET_COMMAND)).thenReturn(tokenSecretOutput("cd456789ef012345"));
        Mockito.when(worker.runKubectl(DELETE_TOKEN_COMMAND)).thenReturn(null);
        Assert.assertThrows(CloudRuntimeException.class, () -> migration.applyToCluster(worker));
    }

    @Test
    public void uuidTokenMigrationVerifiesOnlyOnTheCluster() {
        withUuidToken();
        DeleteForeverJoinTokenMigration migration = new DeleteForeverJoinTokenMigration(manager);
        Mockito.when(worker.runKubectl(TOKEN_SECRET_COMMAND)).thenReturn(tokenSecretOutput("cd456789ef012345"));

        Assert.assertFalse(migration.verify(cluster, worker));
        Assert.assertFalse(migration.verify(cluster, null));
        Assert.assertEquals(KubernetesClusterMigration.Scope.CLUSTER, migration.getScope());
        Assert.assertFalse(migration.isDisruptive());
    }

    // the external cloud provider

    private static final String PROVIDER_RUNNING = "sudo: unable to resolve host x\nimage=apache/cloudstack-kubernetes-provider:v1.3.0 replicas=1.tolerations=node.cloudprovider.kubernetes.io/uninitialized CriticalAddonsOnly.";
    private static final String TEMPLATE_KUBELET_LINE = "echo \"KUBELET_EXTRA_ARGS=--cgroup-driver=systemd{{ k8s.kubelet.cloud.provider.arg }}\" > /etc/default/kubelet";

    /** A Kubernetes node as the simulated kubectl reports it to the external cloud provider migration */
    static class ProviderNode {
        String ready = "True";
        String providerId = "";
        boolean tainted;
    }

    final Map<String, ProviderNode> providerNodes = new HashMap<>();
    final Map<Long, String> userData = new HashMap<>();
    final List<String> providerCalls = new ArrayList<>();
    final java.util.Set<Long> migratedVms = new java.util.HashSet<>();
    boolean providerInitialises = true;
    boolean clusterDetail;
    boolean zoneEnabled;
    ExternalCloudProviderMigration externalCloudProvider;

    private static String nodeUserData(String kubeletLine) {
        return "#cloud-config\nruncmd:\n  - |\n        " + kubeletLine + "\n";
    }

    private void setUpExternalCloudProvider() {
        manager.kubernetesClusterDetailsDao = detailsDao;
        Mockito.when(cluster.getNetworkId()).thenReturn(NETWORK_ID);
        Mockito.when(networkDao.findById(NETWORK_ID)).thenReturn(network);
        Mockito.when(manager.isDirectAccess(network)).thenReturn(false);
        Mockito.when(detailsDao.findDetail(CLUSTER_ID, KubernetesClusterActionWorker.EXTERNAL_CLOUD_PROVIDER_DETAIL)).thenAnswer(i -> {
            if (!clusterDetail) {
                return null;
            }
            KubernetesClusterDetailsVO detail = Mockito.mock(KubernetesClusterDetailsVO.class);
            Mockito.when(detail.getValue()).thenReturn("true");
            return detail;
        });
        Mockito.doAnswer(i -> clusterDetail = true).when(detailsDao).addDetail(CLUSTER_ID, KubernetesClusterActionWorker.EXTERNAL_CLOUD_PROVIDER_DETAIL, "true", false);
        externalCloudProvider = Mockito.spy(new ExternalCloudProviderMigration(manager));
        externalCloudProvider.userVmDao = userVmDao;
        externalCloudProvider.networkDao = networkDao;
        externalCloudProvider.vmInstanceDetailsDao = vmDetailsDao;
        Mockito.when(vmDetailsDao.findDetail(Mockito.anyLong(), Mockito.eq(ExternalCloudProviderMigration.MIGRATED_VM_DETAIL)))
                .thenAnswer(i -> migratedVms.contains((Long) i.getArgument(0)) ? Mockito.mock(com.cloud.vm.VMInstanceDetailVO.class) : null);
        Mockito.doAnswer(i -> migratedVms.add(i.getArgument(0))).when(vmDetailsDao)
                .addDetail(Mockito.anyLong(), Mockito.eq(ExternalCloudProviderMigration.MIGRATED_VM_DETAIL), Mockito.eq("true"), Mockito.eq(false));
        Mockito.doAnswer(i -> zoneEnabled).when(externalCloudProvider).isEnabledInZone(ZONE_ID);
        Mockito.when(worker.getKubernetesCluster()).thenReturn(cluster);
        Mockito.when(worker.pollUntil(Mockito.anyLong(), Mockito.any())).thenAnswer(i -> {
            BooleanSupplier condition = i.getArgument(1);
            for (int attempt = 0; attempt < 3; attempt++) {
                if (condition.getAsBoolean()) {
                    return true;
                }
            }
            return false;
        });
        Mockito.when(worker.runKubectl(Mockito.anyString())).thenAnswer(i -> providerKubectl(i.getArgument(0)));
        Mockito.when(worker.runOnNode(Mockito.any(), Mockito.anyString())).thenAnswer(i -> {
            UserVmVO vm = i.getArgument(0);
            providerCalls.add("kubelet:" + vm.getHostName());
            return new Pair<>(ExternalCloudProviderMigration.KUBELET_COMMAND.equals(i.getArgument(1)), "");
        });
    }

    private String providerKubectl(String command) {
        if (ExternalCloudProviderMigration.PROVIDER_COMMAND.equals(command)) {
            return PROVIDER_RUNNING;
        }
        if (ExternalCloudProviderMigration.PROVIDER_IDS_COMMAND.equals(command)) {
            StringBuilder output = new StringBuilder("sudo: unable to resolve host x\n");
            providerNodes.forEach((name, node) -> output.append("node=").append(name).append("=").append(node.providerId).append(".\n"));
            return output.toString();
        }
        for (Map.Entry<String, ProviderNode> entry : providerNodes.entrySet()) {
            String name = entry.getKey();
            ProviderNode node = entry.getValue();
            if (String.format(ExternalCloudProviderMigration.NODE_COMMAND, name).equals(command)) {
                if (node.tainted && providerInitialises) {
                    node.providerId = "external-cloudstack://" + name;
                    node.tainted = false;
                }
                return "ready=" + node.ready + ".provider=" + node.providerId + ".taint=" + (node.tainted ? "NoSchedule" : "") + ".";
            }
            if (String.format(ExternalCloudProviderMigration.TAINT_COMMAND, name).equals(command)) {
                providerCalls.add("taint:" + name);
                node.tainted = true;
                return "";
            }
            if (String.format(ExternalCloudProviderMigration.UNTAINT_COMMAND, name).equals(command)) {
                providerCalls.add("untaint:" + name);
                node.tainted = false;
                return "";
            }
        }
        return null;
    }

    private UserVmVO addProviderNode(boolean control, String kubeletLine) {
        UserVmVO vm = addNode(control, true);
        userData.put(vm.getId(), nodeUserData(kubeletLine));
        Mockito.when(vm.getUserData()).thenAnswer(i -> org.apache.commons.codec.binary.Base64.encodeBase64String(userData.get(vm.getId()).getBytes()));
        Mockito.doAnswer(i -> userData.put(vm.getId(), new String(org.apache.commons.codec.binary.Base64.decodeBase64((String) i.getArgument(0)))))
                .when(vm).setUserData(Mockito.anyString());
        providerNodes.put(vm.getHostName(), new ProviderNode());
        return vm;
    }

    private UserVmVO addUnmigratedNode(boolean control) {
        return addProviderNode(control, ExternalCloudProviderMigration.KUBELET_ARGS_LINE);
    }

    private UserVmVO addMigratedNode(boolean control) {
        UserVmVO vm = addProviderNode(control, TEMPLATE_KUBELET_LINE.replace("{{ k8s.kubelet.cloud.provider.arg }}", KubernetesClusterActionWorker.KUBELET_CLOUD_PROVIDER_ARGS));
        providerNodes.get(vm.getHostName()).providerId = "external-cloudstack://" + vm.getHostName();
        return vm;
    }

    @Test
    public void externalCloudProviderIsPendingOnNodesWhoseKubeletLacksTheFlag() {
        setUpExternalCloudProvider();
        UserVmVO unmigrated = addUnmigratedNode(false);
        UserVmVO migrated = addMigratedNode(false);
        UserVmVO etcd = addProviderNode(false, "etcd --name etcd-1");
        UserVmVO none = addNode(false, true);

        Assert.assertTrue(externalCloudProvider.isPendingOnNode(cluster, unmigrated));
        Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, migrated));
        Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, etcd));
        Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, none));
    }

    @Test
    public void externalCloudProviderWaitsForTheZoneSetting() {
        setUpExternalCloudProvider();
        addUnmigratedNode(true);
        addUnmigratedNode(true);
        addUnmigratedNode(true);
        addUnmigratedNode(false);

        String reason = externalCloudProvider.checkPrecondition(cluster);
        Assert.assertNotNull(reason);
        Assert.assertTrue(reason, reason.contains(KubernetesClusterService.KubernetesClusterExternalCloudProviderEnabled.key()));

        zoneEnabled = true;
        Assert.assertNull(externalCloudProvider.checkPrecondition(cluster));
        // a cluster that has the provider set up already keeps it once the setting is off
        zoneEnabled = false;
        clusterDetail = true;
        Assert.assertNull(externalCloudProvider.checkPrecondition(cluster));
    }

    @Test
    public void externalCloudProviderNeedsAnIsolatedNetwork() {
        setUpExternalCloudProvider();
        zoneEnabled = true;
        // the VM of a node is not restarted, so a single control node is no reason to refuse
        addUnmigratedNode(true);
        addUnmigratedNode(false);
        Assert.assertNull(externalCloudProvider.checkPrecondition(cluster));

        // on a shared network, which gets no provider, there is nothing to migrate
        Mockito.when(manager.isDirectAccess(network)).thenReturn(true);
        Assert.assertNull(externalCloudProvider.checkPrecondition(cluster));
        Assert.assertTrue(externalCloudProvider.verify(cluster, null));
        maps.forEach(map -> Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, userVmDao.findById(map.getVmId()))));
        Mockito.when(networkDao.findById(NETWORK_ID)).thenReturn(null);
        String reason = externalCloudProvider.checkPrecondition(cluster);
        Assert.assertTrue(reason, reason.contains("network of the cluster is not found"));
    }

    @Test
    public void externalCloudProviderRefusesAClusterWithAddedNodes() {
        setUpExternalCloudProvider();
        zoneEnabled = true;
        addUnmigratedNode(false);
        KubernetesClusterVmMapVO external = new KubernetesClusterVmMapVO(CLUSTER_ID, 999L, false);
        external.setExternalNode(true);
        maps.add(external);

        String reason = externalCloudProvider.checkPrecondition(cluster);
        Assert.assertTrue(reason, reason.contains("addNodesToKubernetesCluster"));
    }

    @Test
    public void externalCloudProviderRefusesKubeletArgumentsCloudStackDidNotSet() {
        setUpExternalCloudProvider();
        zoneEnabled = true;
        UserVmVO vm = addProviderNode(false, "echo \"KUBELET_EXTRA_ARGS=--cgroup-driver=systemd --max-pods=200\" > /etc/default/kubelet");

        String reason = externalCloudProvider.checkPrecondition(cluster);
        Assert.assertTrue(reason, reason.contains("user data"));
        Assert.assertThrows(CloudRuntimeException.class, () -> externalCloudProvider.applyToRunningNode(worker, vm));
        Mockito.verify(worker, Mockito.never()).runOnNode(Mockito.any(), Mockito.anyString());

        // nor a node whose user data does not set them at all, which would never get a provider ID
        maps.clear();
        addProviderNode(false, "echo hello");
        reason = externalCloudProvider.checkPrecondition(cluster);
        Assert.assertTrue(reason, reason != null && reason.contains("user data"));
    }

    @Test
    public void externalCloudProviderChecksTheProviderRuns() {
        setUpExternalCloudProvider();

        externalCloudProvider.applyToCluster(worker);

        Mockito.verify(worker).runKubectl(ExternalCloudProviderMigration.PROVIDER_COMMAND);
        // new nodes only get the flag once the provider has initialised a node
        Assert.assertFalse(clusterDetail);
    }

    @Test
    public void externalCloudProviderHasNothingToCheckOnAMigratedCluster() {
        setUpExternalCloudProvider();
        clusterDetail = true;
        addMigratedNode(false);
        Mockito.doReturn("replicas=0.").when(worker).runKubectl(ExternalCloudProviderMigration.PROVIDER_COMMAND);

        externalCloudProvider.applyToCluster(worker);

        Mockito.verify(worker, Mockito.never()).runKubectl(ExternalCloudProviderMigration.PROVIDER_COMMAND);
    }

    @Test
    public void externalCloudProviderDoesNotTaintANodeWhoseStateCanNotBeRead() {
        setUpExternalCloudProvider();
        UserVmVO vm = addUnmigratedNode(false);
        String nodeCommand = String.format(ExternalCloudProviderMigration.NODE_COMMAND, vm.getHostName());
        Mockito.doReturn(null).when(worker).runKubectl(nodeCommand);

        Assert.assertThrows(CloudRuntimeException.class, () -> externalCloudProvider.applyToRunningNode(worker, vm));

        Assert.assertEquals(List.of("kubelet:" + vm.getHostName()), providerCalls);
    }

    @Test
    public void externalCloudProviderRefusesWithoutTheProvider() {
        setUpExternalCloudProvider();
        Mockito.doReturn("replicas=.").when(worker).runKubectl(ExternalCloudProviderMigration.PROVIDER_COMMAND);

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> externalCloudProvider.applyToCluster(worker));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("cloud-controller-manager"));
        Mockito.doReturn(null).when(worker).runKubectl(ExternalCloudProviderMigration.PROVIDER_COMMAND);
        Assert.assertThrows(CloudRuntimeException.class, () -> externalCloudProvider.applyToCluster(worker));
        Assert.assertFalse(clusterDetail);
    }

    @Test
    public void externalCloudProviderRestartsTheKubeletAndHasTheProviderInitialiseTheNode() {
        setUpExternalCloudProvider();
        UserVmVO vm = addUnmigratedNode(false);

        externalCloudProvider.applyToRunningNode(worker, vm);

        Assert.assertEquals(List.of("kubelet:" + vm.getHostName(), "taint:" + vm.getHostName()), providerCalls);
        Assert.assertEquals("external-cloudstack://" + vm.getHostName(), providerNodes.get(vm.getHostName()).providerId);
        // the user data is what a new node gets from the template, so a recreate keeps the flag
        Assert.assertTrue(userData.get(vm.getId()), userData.get(vm.getId()).contains(
                TEMPLATE_KUBELET_LINE.replace("{{ k8s.kubelet.cloud.provider.arg }}", KubernetesClusterActionWorker.KUBELET_CLOUD_PROVIDER_ARGS)));
        Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, vm));
        Mockito.verify(userVmDao).update(vm.getId(), vm);
        Assert.assertTrue(clusterDetail);
    }

    @Test
    public void externalCloudProviderSetsTheFlagAsTheTemplateDoes() throws Exception {
        for (String name : List.of("k8s-node.yml", "k8s-control-node.yml", "k8s-control-node-add.yml")) {
            try (java.io.InputStream in = getClass().getResourceAsStream("/conf/" + name)) {
                Assert.assertTrue(name, new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).contains(TEMPLATE_KUBELET_LINE));
            }
        }
        String script = new String(org.apache.commons.codec.binary.Base64.decodeBase64(
                ExternalCloudProviderMigration.KUBELET_COMMAND.split(" ")[4]), java.nio.charset.StandardCharsets.UTF_8);
        Assert.assertTrue(ExternalCloudProviderMigration.KUBELET_COMMAND.startsWith("sudo bash -c \"$(echo "));
        Assert.assertEquals(ExternalCloudProviderMigration.KUBELET_SCRIPT, script);
        Assert.assertTrue(script, script.contains(TEMPLATE_KUBELET_LINE.replace("{{ k8s.kubelet.cloud.provider.arg }}", KubernetesClusterActionWorker.KUBELET_CLOUD_PROVIDER_ARGS)));
        Assert.assertTrue(script, script.contains("systemctl restart kubelet"));
        Assert.assertTrue(script, script.contains("systemctl is-active --quiet kubelet"));
    }

    @Test
    public void externalCloudProviderTakesTheTaintOffWhenTheProviderDoesNotInitialiseTheNode() {
        setUpExternalCloudProvider();
        providerInitialises = false;
        UserVmVO vm = addUnmigratedNode(false);

        Assert.assertThrows(CloudRuntimeException.class, () -> externalCloudProvider.applyToRunningNode(worker, vm));

        Assert.assertEquals(List.of("kubelet:" + vm.getHostName(), "taint:" + vm.getHostName(), "untaint:" + vm.getHostName()), providerCalls);
        Assert.assertTrue(externalCloudProvider.isPendingOnNode(cluster, vm));
        Mockito.verify(userVmDao, Mockito.never()).update(Mockito.anyLong(), Mockito.any());
        Assert.assertFalse(clusterDetail);
    }

    @Test
    public void externalCloudProviderTakesANodeTheProviderInitialisesAsTheTaintIsTakenOffForInitialised() {
        setUpExternalCloudProvider();
        providerInitialises = false;
        UserVmVO vm = addUnmigratedNode(false);
        String untaint = String.format(ExternalCloudProviderMigration.UNTAINT_COMMAND, vm.getHostName());
        Mockito.when(worker.runKubectl(untaint)).thenAnswer(i -> {
            // kubectl fails, as the provider has just taken the taint off
            providerInitialises = true;
            return null;
        });

        externalCloudProvider.applyToRunningNode(worker, vm);

        Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, vm));
        Assert.assertTrue(clusterDetail);
    }

    @Test
    public void externalCloudProviderRefusesUserDataTheFlagMakesTooLong() {
        setUpExternalCloudProvider();
        UserVmVO vm = addUnmigratedNode(false);
        Mockito.doThrow(new com.cloud.exception.InvalidParameterValueException("too long")).when(worker).validateNodeUserDataLength(Mockito.anyString(), Mockito.any());

        Assert.assertThrows(com.cloud.exception.InvalidParameterValueException.class, () -> externalCloudProvider.applyToRunningNode(worker, vm));

        Assert.assertTrue(providerCalls.isEmpty());
    }

    /** User data as CloudStack makes it for the first control node of a cluster with a CNI configuration */
    private static String multipartUserData(String nodeConfig) {
        String part = org.apache.commons.codec.binary.Base64.encodeBase64String(nodeConfig.getBytes()).replaceAll("(.{76})", "$1\r\n");
        return "Content-Type: multipart/mixed; boundary=\"b\"\r\nMIME-Version: 1.0\r\n\r\n--b\r\nContent-Type: text/cloud-config; charset=us-ascii\r\n"
                + "Content-Transfer-Encoding: base64\r\n\r\n" + part + "\r\n--b\r\nContent-Transfer-Encoding: base64\r\n\r\nIyBjbmk=\r\n--b--\r\n";
    }

    @Test
    public void externalCloudProviderFindsTheFlagInMultipartUserData() {
        setUpExternalCloudProvider();
        UserVmVO vm = addProviderNode(true, "");
        userData.put(vm.getId(), multipartUserData(nodeUserData(TEMPLATE_KUBELET_LINE.replace("{{ k8s.kubelet.cloud.provider.arg }}",
                KubernetesClusterActionWorker.KUBELET_CLOUD_PROVIDER_ARGS))));

        Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, vm));
        Assert.assertFalse(externalCloudProvider.isMissingKubeletArgs(cluster, vm));
    }

    @Test
    public void externalCloudProviderRefusesAProviderReleasedBeforeV120() {
        setUpExternalCloudProvider();
        for (String image : List.of("apache/cloudstack-kubernetes-provider:v1.1.0", "apache/cloudstack-kubernetes-provider:latest", "apache/cloudstack-kubernetes-provider:v0.9.1",
                "registry.local/cloudstack-kubernetes-provider:v20250101123456.0.0")) {
            Mockito.doReturn("image=" + image + " replicas=1.tolerations=node.cloudprovider.kubernetes.io/uninitialized.").when(worker).runKubectl(ExternalCloudProviderMigration.PROVIDER_COMMAND);
            CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> externalCloudProvider.applyToCluster(worker));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(image));
        }
        Mockito.doReturn("image=registry.local/cloudstack-kubernetes-provider:v2.0.1 proxy:v1.0.0 replicas=1.tolerations=node.cloudprovider.kubernetes.io/uninitialized.").when(worker).runKubectl(ExternalCloudProviderMigration.PROVIDER_COMMAND);
        externalCloudProvider.applyToCluster(worker);
    }

    @Test
    public void externalCloudProviderRefusesAProviderThatDoesNotTolerateTheUninitializedTaint() {
        setUpExternalCloudProvider();
        Mockito.doReturn("image=apache/cloudstack-kubernetes-provider:v1.3.0 replicas=1.tolerations=CriticalAddonsOnly.").when(worker).runKubectl(ExternalCloudProviderMigration.PROVIDER_COMMAND);

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> externalCloudProvider.applyToCluster(worker));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("v1.1.0"));
    }

    @Test
    public void externalCloudProviderMigratesAControlNodeWithMultipartUserDataWithoutChangingIt() {
        setUpExternalCloudProvider();
        zoneEnabled = true;
        UserVmVO vm = addProviderNode(true, "");
        String multipart = multipartUserData(nodeUserData(ExternalCloudProviderMigration.KUBELET_ARGS_LINE));
        userData.put(vm.getId(), multipart);

        Assert.assertTrue(externalCloudProvider.isPendingOnNode(cluster, vm));
        Assert.assertNull(externalCloudProvider.checkPrecondition(cluster));

        externalCloudProvider.applyToRunningNode(worker, vm);

        Assert.assertEquals(List.of("kubelet:" + vm.getHostName(), "taint:" + vm.getHostName()), providerCalls);
        Assert.assertEquals(multipart, userData.get(vm.getId()));
        Mockito.verify(userVmDao, Mockito.never()).update(Mockito.anyLong(), Mockito.any());
        Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, vm));
        Assert.assertTrue(externalCloudProvider.verify(cluster, worker));
    }

    @Test
    public void externalCloudProviderDoesNotTaintANodeThatHasAProviderId() {
        setUpExternalCloudProvider();
        UserVmVO vm = addUnmigratedNode(false);
        providerNodes.get(vm.getHostName()).providerId = "external-cloudstack://x";

        externalCloudProvider.applyToRunningNode(worker, vm);

        Assert.assertEquals(List.of("kubelet:" + vm.getHostName()), providerCalls);
        Assert.assertFalse(externalCloudProvider.isPendingOnNode(cluster, vm));
    }

    @Test
    public void externalCloudProviderStopsWhenTheKubeletCanNotBeRestarted() {
        setUpExternalCloudProvider();
        UserVmVO vm = addUnmigratedNode(false);
        Mockito.doReturn(new Pair<>(false, "/etc/default/kubelet is not as CloudStack wrote it\n")).when(worker).runOnNode(Mockito.any(), Mockito.anyString());

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> externalCloudProvider.applyToRunningNode(worker, vm));
        Assert.assertTrue(e.getMessage(), e.getMessage().endsWith(": /etc/default/kubelet is not as CloudStack wrote it"));

        Assert.assertTrue(providerCalls.isEmpty());
        Assert.assertTrue(externalCloudProvider.isPendingOnNode(cluster, vm));
    }

    @Test
    public void externalCloudProviderVerifiesEveryNodeHasTheFlagAndAProviderId() {
        setUpExternalCloudProvider();
        UserVmVO control = addMigratedNode(true);
        UserVmVO node = addUnmigratedNode(false);

        Assert.assertFalse(externalCloudProvider.verify(cluster, worker));
        clusterDetail = true;
        Assert.assertFalse(externalCloudProvider.verify(cluster, null));

        externalCloudProvider.applyToRunningNode(worker, node);
        Assert.assertTrue(externalCloudProvider.verify(cluster, null));
        Assert.assertTrue(externalCloudProvider.verify(cluster, worker));
        // and once migrated, nothing is left to do, whatever the setting
        Assert.assertNull(externalCloudProvider.checkPrecondition(cluster));

        providerNodes.get(control.getHostName()).providerId = "";
        Assert.assertFalse(externalCloudProvider.verify(cluster, worker));
        Mockito.doReturn(null).when(worker).runKubectl(ExternalCloudProviderMigration.PROVIDER_IDS_COMMAND);
        Assert.assertFalse(externalCloudProvider.verify(cluster, worker));
    }
}
