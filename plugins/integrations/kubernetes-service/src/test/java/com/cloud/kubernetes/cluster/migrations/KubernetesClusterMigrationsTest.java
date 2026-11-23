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
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.service.ServiceOfferingVO;
import com.cloud.service.dao.ServiceOfferingDao;
import com.cloud.storage.VMTemplateVO;
import com.cloud.storage.dao.VMTemplateDao;
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
}
