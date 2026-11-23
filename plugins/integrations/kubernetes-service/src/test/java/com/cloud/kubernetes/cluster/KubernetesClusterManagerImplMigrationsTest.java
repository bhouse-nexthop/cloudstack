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
package com.cloud.kubernetes.cluster;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.command.user.kubernetes.cluster.ApplyKubernetesClusterMigrationsCmd;
import org.apache.cloudstack.api.command.user.kubernetes.cluster.ListKubernetesClusterMigrationsCmd;
import org.apache.cloudstack.api.response.KubernetesClusterMigrationResponse;
import org.apache.cloudstack.context.CallContext;
import org.apache.cloudstack.framework.config.ConfigKey;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import com.cloud.exception.InvalidParameterValueException;
import com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterAffinityGroupMapDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterMigrationDao;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.kubernetes.cluster.migrations.DynamicScalingMigration;
import com.cloud.kubernetes.cluster.migrations.HeadlampMigration;
import com.cloud.kubernetes.cluster.migrations.KubernetesClusterMigration;
import com.cloud.kubernetes.cluster.migrations.KubernetesClusterMigration.Scope;
import com.cloud.kubernetes.cluster.migrations.NodeTypeSettingsMigration;
import com.cloud.user.Account;
import com.cloud.user.AccountManager;
import com.cloud.user.User;
import com.cloud.utils.db.GlobalLock;
import com.cloud.utils.db.Transaction;
import com.cloud.utils.db.TransactionCallback;
import com.cloud.utils.exception.CloudRuntimeException;

/** Tests of the migrations: the registry, the database migrations at startup, and the list and apply APIs */
@RunWith(MockitoJUnitRunner.Silent.class)
public class KubernetesClusterManagerImplMigrationsTest {

    private static final long CLUSTER_ID = 1L;
    private static final long CLUSTER_OFFERING = 3L;

    @Mock
    KubernetesClusterDao kubernetesClusterDao;
    @Mock
    KubernetesClusterVmMapDao kubernetesClusterVmMapDao;
    @Mock
    KubernetesClusterAffinityGroupMapDao kubernetesClusterAffinityGroupMapDao;
    @Mock
    KubernetesClusterMigrationDao kubernetesClusterMigrationDao;
    @Mock
    AccountManager accountManager;

    @Spy
    @InjectMocks
    KubernetesClusterManagerImpl manager;

    MockedStatic<Transaction> transactionMock;
    KubernetesClusterVO cluster;
    final List<KubernetesClusterMigrationVO> applied = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        CallContext.register(Mockito.mock(User.class), Mockito.mock(Account.class));
        Field f = ConfigKey.class.getDeclaredField("_defaultValue");
        f.setAccessible(true);
        f.set(KubernetesClusterService.KubernetesServiceEnabled, "true");
        transactionMock = Mockito.mockStatic(Transaction.class);
        transactionMock.when(() -> Transaction.execute(Mockito.any(TransactionCallback.class)))
                .thenAnswer(invocation -> ((TransactionCallback<?>) invocation.getArgument(0)).doInTransaction(null));
        cluster = Mockito.spy(new KubernetesClusterVO());
        Mockito.doReturn(CLUSTER_ID).when(cluster).getId();
        Mockito.doReturn("cluster").when(cluster).getName();
        cluster.setState(KubernetesCluster.State.Running);
        cluster.setClusterType(KubernetesCluster.ClusterType.CloudManaged);
        cluster.setServiceOfferingId(CLUSTER_OFFERING);
        Mockito.when(kubernetesClusterDao.findById(CLUSTER_ID)).thenReturn(cluster);
        Mockito.when(kubernetesClusterDao.lockRow(CLUSTER_ID, true)).thenReturn(cluster);
        Mockito.when(kubernetesClusterDao.listAll()).thenReturn(List.of(cluster));
        Mockito.when(kubernetesClusterVmMapDao.listByClusterId(CLUSTER_ID)).thenReturn(List.of());
        Mockito.when(kubernetesClusterAffinityGroupMapDao.listAffinityGroupIdsByClusterIdAndNodeType(Mockito.anyLong(), Mockito.anyString())).thenReturn(List.of());
        Mockito.when(kubernetesClusterMigrationDao.listByClusterId(CLUSTER_ID)).thenAnswer(i -> new ArrayList<>(applied));
        Mockito.when(kubernetesClusterMigrationDao.findByClusterIdAndMigrationId(Mockito.eq(CLUSTER_ID), Mockito.anyString())).thenAnswer(i ->
                applied.stream().filter(a -> a.getMigrationId().equals(i.getArgument(1))).findFirst().orElse(null));
        Mockito.when(kubernetesClusterMigrationDao.persist(Mockito.any())).thenAnswer(i -> {
            applied.add(i.getArgument(0));
            return i.getArgument(0);
        });
    }

    @After
    public void tearDown() {
        transactionMock.close();
        CallContext.unregister();
    }

    private KubernetesClusterMigration migration(String id, Scope scope) {
        KubernetesClusterMigration migration = Mockito.mock(KubernetesClusterMigration.class);
        Mockito.when(migration.getId()).thenReturn(id);
        Mockito.when(migration.getScope()).thenReturn(scope);
        Mockito.when(migration.getDescription()).thenReturn("migration " + id);
        Mockito.when(migration.verify(Mockito.any(), Mockito.any())).thenReturn(scope == Scope.DATABASE);
        return migration;
    }

    /** A worker whose rollout applies the migrations it is given */
    private KubernetesClusterRollingUpdateWorker worker(int maxUnavailable) {
        KubernetesClusterRollingUpdateWorker worker = Mockito.mock(KubernetesClusterRollingUpdateWorker.class);
        List<KubernetesClusterMigration> given = new ArrayList<>();
        Mockito.doAnswer(i -> given.addAll(i.getArgument(0))).when(worker).setMigrations(Mockito.anyList());
        Mockito.when(worker.rollout()).thenAnswer(i -> {
            given.forEach(m -> applied.add(new KubernetesClusterMigrationVO(CLUSTER_ID, m.getId())));
            return true;
        });
        Mockito.doReturn(worker).when(manager).createRollingUpdateWorker(cluster, maxUnavailable);
        return worker;
    }

    private java.util.Set<String> appliedSet() {
        return new java.util.HashSet<>(appliedIds());
    }

    private List<String> appliedIds() {
        return applied.stream().map(KubernetesClusterMigrationVO::getMigrationId).collect(Collectors.toList());
    }

    private ApplyKubernetesClusterMigrationsCmd applyCmd(List<String> migrations) {
        ApplyKubernetesClusterMigrationsCmd cmd = Mockito.mock(ApplyKubernetesClusterMigrationsCmd.class);
        Mockito.when(cmd.getId()).thenReturn(CLUSTER_ID);
        Mockito.when(cmd.getActualCommandName()).thenReturn(BaseCmd.getCommandNameByClass(ApplyKubernetesClusterMigrationsCmd.class));
        Mockito.when(cmd.getMigrations()).thenReturn(migrations);
        Mockito.when(cmd.getMaxUnavailable()).thenReturn(null);
        return cmd;
    }

    // the registry

    @Test
    public void registryIsInOrderWithUniqueIds() {
        List<KubernetesClusterMigration> migrations = manager.createMigrations();

        Assert.assertEquals(List.of(NodeTypeSettingsMigration.ID, DynamicScalingMigration.ID, HeadlampMigration.ID),
                migrations.stream().map(KubernetesClusterMigration::getId).collect(Collectors.toList()));
        Assert.assertEquals(List.of(Scope.DATABASE, Scope.NODE, Scope.CLUSTER),
                migrations.stream().map(KubernetesClusterMigration::getScope).collect(Collectors.toList()));
        Assert.assertTrue(migrations.stream().allMatch(m -> m.getId().length() <= 64));
    }

    // database migrations at startup

    @Test
    public void startupConvertsAGen1ClusterOnce() {
        Mockito.doReturn(List.of(new NodeTypeSettingsMigration(manager))).when(manager).getMigrations();
        Assert.assertNull(cluster.getControlNodeServiceOfferingId());

        manager.applyDatabaseMigrations();
        manager.applyDatabaseMigrations();

        Assert.assertEquals(CLUSTER_OFFERING, (long) cluster.getControlNodeServiceOfferingId());
        Assert.assertEquals(CLUSTER_OFFERING, (long) cluster.getWorkerNodeServiceOfferingId());
        Assert.assertNull(cluster.getEtcdNodeServiceOfferingId());
        Assert.assertEquals(List.of(NodeTypeSettingsMigration.ID), appliedIds());
        Mockito.verify(manager, Mockito.times(1)).convertToNodeTypeSettings(cluster);
    }

    @Test
    public void startupAppliesOnlyDatabaseMigrationsInOrder() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        KubernetesClusterMigration onCluster = migration("cluster", Scope.CLUSTER);
        KubernetesClusterMigration second = migration("second", Scope.DATABASE);
        Mockito.doReturn(List.of(first, node, onCluster, second)).when(manager).getMigrations();

        manager.applyDatabaseMigrations();

        InOrder inOrder = Mockito.inOrder(first, second);
        inOrder.verify(first).applyToDatabase(cluster);
        inOrder.verify(second).applyToDatabase(cluster);
        Mockito.verify(node, Mockito.never()).applyToStoppedNode(Mockito.any(), Mockito.any());
        Mockito.verify(onCluster, Mockito.never()).applyToCluster(Mockito.any());
        Assert.assertEquals(List.of("first", "second"), appliedIds());
    }

    @Test
    public void startupStopsAtAMigrationThatCanNotBeAppliedYet() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        KubernetesClusterMigration second = migration("second", Scope.DATABASE);
        Mockito.when(first.checkPrecondition(cluster)).thenReturn("not yet");
        Mockito.doReturn(List.of(first, second)).when(manager).getMigrations();

        manager.applyDatabaseMigrations();

        Mockito.verify(first, Mockito.never()).applyToDatabase(Mockito.any());
        Mockito.verify(second, Mockito.never()).applyToDatabase(Mockito.any());
        Assert.assertTrue(applied.isEmpty());
    }

    @Test
    public void startupLeavesAClusterWithAnOperationInProgress() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        Mockito.doReturn(List.of(first)).when(manager).getMigrations();
        cluster.setState(KubernetesCluster.State.Updating);

        manager.applyDatabaseMigrations();

        Mockito.verify(first, Mockito.never()).applyToDatabase(Mockito.any());
        Assert.assertTrue(applied.isEmpty());
    }

    @Test
    public void startupLeavesExternallyManagedClusters() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        Mockito.doReturn(List.of(first)).when(manager).getMigrations();
        cluster.setClusterType(KubernetesCluster.ClusterType.ExternalManaged);

        manager.applyDatabaseMigrations();

        Mockito.verify(first, Mockito.never()).applyToDatabase(Mockito.any());
    }

    @Test
    public void startupDoesNotRecordAMigrationThatDidNotVerify() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        Mockito.when(first.verify(Mockito.any(), Mockito.any())).thenReturn(false);
        Mockito.doReturn(List.of(first)).when(manager).getMigrations();

        manager.applyDatabaseMigrations();

        Assert.assertTrue(applied.isEmpty());
    }

    @Test
    public void runnerSkipsWhileAnotherManagementServerHoldsTheLock() {
        GlobalLock lock = Mockito.mock(GlobalLock.class);
        Mockito.when(lock.lock(Mockito.anyInt())).thenReturn(false);
        try (MockedStatic<GlobalLock> lockMock = Mockito.mockStatic(GlobalLock.class)) {
            lockMock.when(() -> GlobalLock.getInternLock(KubernetesClusterManagerImpl.MIGRATIONS_LOCK)).thenReturn(lock);

            manager.new KubernetesClusterMigrationRunner().runInContext();
        }

        Mockito.verify(manager, Mockito.never()).applyDatabaseMigrations();
        Mockito.verify(lock).releaseRef();
    }

    @Test
    public void runnerAppliesUnderTheLock() {
        GlobalLock lock = Mockito.mock(GlobalLock.class);
        Mockito.when(lock.lock(Mockito.anyInt())).thenReturn(true);
        Mockito.doNothing().when(manager).applyDatabaseMigrations();
        try (MockedStatic<GlobalLock> lockMock = Mockito.mockStatic(GlobalLock.class)) {
            lockMock.when(() -> GlobalLock.getInternLock(KubernetesClusterManagerImpl.MIGRATIONS_LOCK)).thenReturn(lock);

            manager.new KubernetesClusterMigrationRunner().runInContext();
        }

        InOrder inOrder = Mockito.inOrder(lock, manager);
        inOrder.verify(lock).lock(Mockito.anyInt());
        inOrder.verify(manager).applyDatabaseMigrations();
        inOrder.verify(lock).unlock();
    }

    // list

    @Test
    public void listShowsAppliedAndPendingWithWhatBlocksThem() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        Mockito.when(node.checkPrecondition(cluster)).thenReturn("not scalable");
        Mockito.when(node.isDisruptive()).thenReturn(true);
        Mockito.doReturn(List.of(first, node)).when(manager).getMigrations();
        applied.add(new KubernetesClusterMigrationVO(CLUSTER_ID, "first"));
        ListKubernetesClusterMigrationsCmd cmd = Mockito.mock(ListKubernetesClusterMigrationsCmd.class);
        Mockito.when(cmd.getId()).thenReturn(CLUSTER_ID);
        Mockito.when(cmd.getActualCommandName()).thenReturn(BaseCmd.getCommandNameByClass(ListKubernetesClusterMigrationsCmd.class));

        List<KubernetesClusterMigrationResponse> responses = manager.listKubernetesClusterMigrations(cmd).getResponses();

        Assert.assertEquals(List.of("first", "node"), responses.stream().map(KubernetesClusterMigrationResponse::getId).collect(Collectors.toList()));
        Assert.assertEquals("Applied", responses.get(0).getState());
        Assert.assertNull(responses.get(0).getUnmetPrecondition());
        Assert.assertEquals("Pending", responses.get(1).getState());
        Assert.assertEquals("not scalable", responses.get(1).getUnmetPrecondition());
    }

    @Test(expected = InvalidParameterValueException.class)
    public void listRefusesAnExternallyManagedCluster() {
        cluster.setClusterType(KubernetesCluster.ClusterType.ExternalManaged);
        ListKubernetesClusterMigrationsCmd cmd = Mockito.mock(ListKubernetesClusterMigrationsCmd.class);
        Mockito.when(cmd.getId()).thenReturn(CLUSTER_ID);
        Mockito.when(cmd.getActualCommandName()).thenReturn(BaseCmd.getCommandNameByClass(ListKubernetesClusterMigrationsCmd.class));

        manager.listKubernetesClusterMigrations(cmd);
    }

    // apply

    @Test
    public void applyRunsTheDatabaseMigrationsThenOneRolloutOfTheOthers() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        KubernetesClusterMigration onCluster = migration("cluster", Scope.CLUSTER);
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        Mockito.doReturn(List.of(first, onCluster, node)).when(manager).getMigrations();
        Mockito.doReturn(Map.of(KubernetesClusterNodeType.CONTROL, 0, KubernetesClusterNodeType.WORKER, 0)).when(manager).countOutOfDateNodes(cluster);
        KubernetesClusterRollingUpdateWorker worker = worker(2);
        ApplyKubernetesClusterMigrationsCmd cmd = applyCmd(null);
        Mockito.when(cmd.getMaxUnavailable()).thenReturn(2);

        Assert.assertTrue(manager.applyKubernetesClusterMigrations(cmd));

        InOrder inOrder = Mockito.inOrder(first, worker, manager);
        inOrder.verify(first).applyToDatabase(cluster);
        inOrder.verify(worker).setMigrations(List.of(onCluster, node));
        inOrder.verify(worker).begin();
        inOrder.verify(manager).countOutOfDateNodes(cluster);
        inOrder.verify(worker).rollout();
        Assert.assertEquals(List.of("first", "cluster", "node"), appliedIds());
    }

    @Test
    public void applyTakesOnlyTheRequestedPendingMigrations() {
        KubernetesClusterMigration onCluster = migration("cluster", Scope.CLUSTER);
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        KubernetesClusterMigration done = migration("done", Scope.NODE);
        applied.add(new KubernetesClusterMigrationVO(CLUSTER_ID, "done"));
        Mockito.doReturn(List.of(onCluster, node, done)).when(manager).getMigrations();

        Assert.assertEquals(List.of(node), manager.selectMigrations(cluster, List.of("node", "done")));
        Assert.assertEquals(List.of(onCluster, node), manager.selectMigrations(cluster, null));
    }

    @Test
    public void applyWithNothingPendingStartsNoRollout() {
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        applied.add(new KubernetesClusterMigrationVO(CLUSTER_ID, "node"));
        Mockito.doReturn(List.of(node)).when(manager).getMigrations();
        cluster.setState(KubernetesCluster.State.Stopped);

        Assert.assertTrue(manager.applyKubernetesClusterMigrations(applyCmd(null)));

        Mockito.verify(manager, Mockito.never()).createRollingUpdateWorker(Mockito.any(), Mockito.anyInt());
    }

    @Test
    public void applyRefusesAnUnknownMigration() {
        Mockito.doReturn(List.of(migration("node", Scope.NODE))).when(manager).getMigrations();

        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> manager.applyKubernetesClusterMigrations(applyCmd(List.of("nope"))));

        Assert.assertTrue(e.getMessage(), e.getMessage().contains("Unknown migration nope"));
    }

    @Test
    public void applyRefusesAnUnmetPrecondition() {
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        Mockito.when(node.checkPrecondition(cluster)).thenReturn("not scalable");
        Mockito.doReturn(List.of(node)).when(manager).getMigrations();

        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> manager.applyKubernetesClusterMigrations(applyCmd(List.of("node"))));

        Assert.assertTrue(e.getMessage(), e.getMessage().contains("not scalable"));
        Mockito.verify(manager, Mockito.never()).createRollingUpdateWorker(Mockito.any(), Mockito.anyInt());
    }

    @Test
    public void applyRefusesAStoppedCluster() {
        Mockito.doReturn(List.of(migration("node", Scope.NODE))).when(manager).getMigrations();
        cluster.setState(KubernetesCluster.State.Stopped);

        Assert.assertThrows(InvalidParameterValueException.class, () -> manager.applyKubernetesClusterMigrations(applyCmd(null)));
        Mockito.verify(manager, Mockito.never()).createRollingUpdateWorker(Mockito.any(), Mockito.anyInt());
    }

    @Test
    public void applyRefusesANamedNodeMigrationWhileNodesDifferFromTheirSettings() {
        Mockito.doReturn(List.of(migration("node", Scope.NODE))).when(manager).getMigrations();
        Mockito.doReturn(Map.of(KubernetesClusterNodeType.CONTROL, 0, KubernetesClusterNodeType.WORKER, 1)).when(manager).countOutOfDateNodes(cluster);

        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> manager.applyKubernetesClusterMigrations(applyCmd(List.of("node"))));

        Assert.assertTrue(e.getMessage(), e.getMessage().contains("rollout=true"));
        Mockito.verify(manager, Mockito.never()).createRollingUpdateWorker(Mockito.any(), Mockito.anyInt());
    }

    @Test
    public void applyAllLeavesTheNodeMigrationsWhileNodesDifferFromTheirSettings() {
        KubernetesClusterMigration onCluster = migration("cluster", Scope.CLUSTER);
        Mockito.doReturn(List.of(onCluster, migration("node", Scope.NODE))).when(manager).getMigrations();
        Mockito.doReturn(Map.of(KubernetesClusterNodeType.CONTROL, 0, KubernetesClusterNodeType.WORKER, 1)).when(manager).countOutOfDateNodes(cluster);
        KubernetesClusterRollingUpdateWorker worker = worker(1);

        Assert.assertTrue(manager.applyKubernetesClusterMigrations(applyCmd(null)));

        Mockito.verify(worker).setMigrations(List.of(onCluster));
        Assert.assertEquals(List.of("cluster"), appliedIds());
    }

    @Test
    public void applyAbortsWhenNodesDifferOnceTheClusterIsUpdating() {
        Mockito.doReturn(List.of(migration("node", Scope.NODE))).when(manager).getMigrations();
        Mockito.doReturn(Map.of(KubernetesClusterNodeType.CONTROL, 0, KubernetesClusterNodeType.WORKER, 0))
                .doReturn(Map.of(KubernetesClusterNodeType.CONTROL, 0, KubernetesClusterNodeType.WORKER, 1)).when(manager).countOutOfDateNodes(cluster);
        KubernetesClusterRollingUpdateWorker worker = Mockito.mock(KubernetesClusterRollingUpdateWorker.class);
        Mockito.doReturn(worker).when(manager).createRollingUpdateWorker(cluster, 1);

        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> manager.applyKubernetesClusterMigrations(applyCmd(null)));

        Assert.assertTrue(e.getMessage(), e.getMessage().contains("rollout=true"));
        InOrder inOrder = Mockito.inOrder(worker);
        inOrder.verify(worker).begin();
        inOrder.verify(worker).abort();
        Mockito.verify(worker, Mockito.never()).rollout();
    }

    @Test
    public void allPendingLeavesOutThoseWhosePreconditionIsUnmet() {
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        Mockito.when(node.checkPrecondition(cluster)).thenReturn("one control node");
        KubernetesClusterMigration onCluster = migration("cluster", Scope.CLUSTER);
        Mockito.doReturn(List.of(node, onCluster)).when(manager).getMigrations();

        Assert.assertEquals(List.of(onCluster), manager.selectMigrations(cluster, null));
        Assert.assertThrows(InvalidParameterValueException.class, () -> manager.selectMigrations(cluster, List.of("node")));
    }

    @Test
    public void listGivesTheStateThatKeepsAMigrationFromBeingApplied() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        Mockito.doReturn(List.of(first, node)).when(manager).getMigrations();
        applied.add(new KubernetesClusterMigrationVO(CLUSTER_ID, "first"));
        cluster.setState(KubernetesCluster.State.Stopped);

        Assert.assertNull(manager.getUnmetPrecondition(cluster, first, appliedSet()));
        Assert.assertTrue(manager.getUnmetPrecondition(cluster, node, appliedSet()).contains("Stopped"));
        cluster.setState(KubernetesCluster.State.Updating);
        Assert.assertTrue(manager.getUnmetPrecondition(cluster, first, appliedSet()).contains("Updating"));
    }

    @Test
    public void listTellsAnEarlierDatabaseMigrationCanNotBeApplied() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        Mockito.doReturn(List.of(first, node)).when(manager).getMigrations();

        // applied by applyKubernetesClusterMigrations first
        Assert.assertNull(manager.getUnmetPrecondition(cluster, node, appliedSet()));
        Mockito.when(first.checkPrecondition(cluster)).thenReturn("no offering");
        Assert.assertTrue(manager.getUnmetPrecondition(cluster, node, appliedSet()).contains("database migration first, which comes first, can not be applied: no offering"));
        applied.add(new KubernetesClusterMigrationVO(CLUSTER_ID, "first"));
        Assert.assertNull(manager.getUnmetPrecondition(cluster, node, appliedSet()));
    }

    @Test
    public void applyAllLeavesAClusterMigrationRefusedOnTheClusterPending() {
        Mockito.doReturn(List.of(migration("cluster", Scope.CLUSTER))).when(manager).getMigrations();
        KubernetesClusterRollingUpdateWorker worker = Mockito.mock(KubernetesClusterRollingUpdateWorker.class);
        Mockito.when(worker.rollout()).thenReturn(true);
        Mockito.doReturn(worker).when(manager).createRollingUpdateWorker(cluster, 1);

        Assert.assertTrue(manager.applyKubernetesClusterMigrations(applyCmd(null)));
        Assert.assertTrue(applied.isEmpty());
    }

    @Test
    public void applyRefusesWhileADatabaseMigrationIsPending() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        Mockito.when(first.verify(Mockito.any(), Mockito.any())).thenReturn(false);
        Mockito.doReturn(List.of(first, migration("node", Scope.NODE))).when(manager).getMigrations();
        cluster.setState(KubernetesCluster.State.Scaling);

        Assert.assertThrows(InvalidParameterValueException.class, () -> manager.selectMigrations(cluster, null));
        Assert.assertThrows(InvalidParameterValueException.class, () -> manager.selectMigrations(cluster, List.of("node")));
    }

    @Test
    public void applyRefusesARequestedDatabaseMigrationThatIsStillPending() {
        KubernetesClusterMigration first = migration("first", Scope.DATABASE);
        Mockito.when(first.checkPrecondition(cluster)).thenReturn("not yet");
        Mockito.when(first.verify(Mockito.any(), Mockito.any())).thenReturn(false);
        Mockito.doReturn(List.of(first)).when(manager).getMigrations();

        InvalidParameterValueException e = Assert.assertThrows(InvalidParameterValueException.class,
                () -> manager.applyKubernetesClusterMigrations(applyCmd(List.of("first"))));

        Assert.assertTrue(e.getMessage(), e.getMessage().contains("not yet"));
    }

    @Test
    public void aRecoveredClusterGetsItsDatabaseMigrations() throws Exception {
        cluster.setState(KubernetesCluster.State.Updating);
        manager.asyncJobDao = Mockito.mock(org.apache.cloudstack.framework.jobs.dao.AsyncJobDao.class);
        Mockito.doNothing().when(manager).updateKubernetesClusterCapacity(CLUSTER_ID);
        Mockito.doNothing().when(manager).applyDatabaseMigrations(CLUSTER_ID);
        Mockito.when(kubernetesClusterDao.updateState(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(true);

        manager.recoverOrphanedOperationState(CLUSTER_ID);

        Mockito.verify(manager).applyDatabaseMigrations(CLUSTER_ID);
    }

    @Test
    public void clusterMigrationsDoNotNeedTheNodesToMatchTheirSettings() {
        KubernetesClusterMigration onCluster = migration("cluster", Scope.CLUSTER);
        Mockito.doReturn(List.of(onCluster)).when(manager).getMigrations();
        Mockito.doReturn(Map.of(KubernetesClusterNodeType.CONTROL, 1, KubernetesClusterNodeType.WORKER, 1)).when(manager).countOutOfDateNodes(cluster);
        KubernetesClusterRollingUpdateWorker worker = worker(1);

        Assert.assertTrue(manager.applyKubernetesClusterMigrations(applyCmd(null)));

        Mockito.verify(worker).rollout();
    }

    @Test
    public void nodeMigrationsIgnoreNodeTypesARolloutCanNotRestart() {
        Mockito.doReturn(List.of(migration("node", Scope.NODE))).when(manager).getMigrations();
        Mockito.doReturn(Map.of(KubernetesClusterNodeType.CONTROL, 1, KubernetesClusterNodeType.WORKER, 0)).when(manager).countOutOfDateNodes(cluster);
        Mockito.doReturn(java.util.Set.of(KubernetesClusterNodeType.CONTROL)).when(manager).getUnrollableNodeTypes(cluster);
        KubernetesClusterRollingUpdateWorker worker = worker(1);

        Assert.assertTrue(manager.applyKubernetesClusterMigrations(applyCmd(null)));

        Mockito.verify(worker).rollout();
    }

    @Test
    public void applyFailsWhenARequestedMigrationIsLeftPending() {
        Mockito.doReturn(List.of(migration("cluster", Scope.CLUSTER))).when(manager).getMigrations();
        KubernetesClusterRollingUpdateWorker worker = Mockito.mock(KubernetesClusterRollingUpdateWorker.class);
        Mockito.when(worker.rollout()).thenReturn(true);
        Mockito.doReturn(worker).when(manager).createRollingUpdateWorker(cluster, 1);
        manager.kubernetesClusterDetailsDao = Mockito.mock(com.cloud.kubernetes.cluster.dao.KubernetesClusterDetailsDao.class);

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> manager.applyKubernetesClusterMigrations(applyCmd(List.of("cluster"))));

        Assert.assertTrue(e.getMessage(), e.getMessage().contains("[cluster] were not applied"));
    }

    @Test
    public void applyRechecksThePreconditionsOnceUpdating() {
        KubernetesClusterMigration node = migration("node", Scope.NODE);
        Mockito.when(node.checkPrecondition(cluster)).thenReturn(null, "changed");
        Mockito.doReturn(List.of(node)).when(manager).getMigrations();
        KubernetesClusterRollingUpdateWorker worker = Mockito.mock(KubernetesClusterRollingUpdateWorker.class);
        Mockito.doReturn(worker).when(manager).createRollingUpdateWorker(cluster, 1);

        Assert.assertThrows(InvalidParameterValueException.class, () -> manager.applyKubernetesClusterMigrations(applyCmd(null)));

        Mockito.verify(worker).abort();
        Mockito.verify(worker, Mockito.never()).rollout();
    }
}
