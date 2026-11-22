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
package org.apache.cloudstack.storage.motion;

import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.any;

import com.cloud.storage.Storage;
import com.cloud.storage.StorageManager;
import com.cloud.storage.StoragePool;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPoint;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPointSelector;
import org.apache.cloudstack.engine.subsystem.api.storage.ClusterScope;
import org.apache.cloudstack.engine.subsystem.api.storage.HostScope;
import org.apache.cloudstack.engine.subsystem.api.storage.Scope;
import org.apache.cloudstack.engine.subsystem.api.storage.StorageCacheManager;
import org.apache.cloudstack.engine.subsystem.api.storage.ZoneScope;
import org.apache.cloudstack.framework.config.ConfigKey;
import org.apache.cloudstack.framework.config.dao.ConfigurationDao;
import org.apache.cloudstack.storage.datastore.db.SnapshotDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.SnapshotDataStoreVO;
import org.apache.cloudstack.storage.command.CopyCommand;
import org.apache.cloudstack.storage.image.store.TemplateObject;
import org.apache.cloudstack.storage.to.PrimaryDataStoreTO;
import org.apache.cloudstack.storage.volume.VolumeObject;
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

import com.cloud.agent.api.Answer;
import com.cloud.storage.dao.VolumeDao;
import org.apache.cloudstack.storage.command.CopyCmdAnswer;
import com.cloud.agent.api.to.DataTO;
import com.cloud.storage.DataStoreRole;
import com.cloud.hypervisor.Hypervisor.HypervisorType;


@RunWith(MockitoJUnitRunner.class)
public class AncientDataMotionStrategyTest {

    @Mock
    VolumeDao volDao;

    @Spy
    @InjectMocks
    private AncientDataMotionStrategy strategy = new AncientDataMotionStrategy();

    @Mock
    DataTO dataTO;
    @Mock
    PrimaryDataStoreTO dataStoreTO;
    @Mock
    StorageManager storageManager;
    @Mock
    StoragePool storagePool;
    @Mock
    StorageCacheManager cacheMgr;
    @Mock
    ConfigurationDao configDao;
    @Mock
    SnapshotDataStoreDao snapshotDataStoreDao;
    @Mock
    EndPointSelector selector;

    private static final long POOL_ID = 1l;
    private static final Boolean FULL_CLONE_FLAG = true;

    @Before
    public void setup() throws Exception {
        overrideDefaultConfigValue(StorageManager.VmwareCreateCloneFull, String.valueOf(FULL_CLONE_FLAG));

        when(dataTO.getHypervisorType()).thenReturn(HypervisorType.VMware);
        when(dataTO.getDataStore()).thenReturn(dataStoreTO);
        when(dataStoreTO.getId()).thenReturn(POOL_ID);
        when(storageManager.getStoragePool(POOL_ID)).thenReturn(storagePool);
    }

    private void overrideDefaultConfigValue(final ConfigKey configKey, final String value) throws IllegalAccessException, NoSuchFieldException {
        final Field f = ConfigKey.class.getDeclaredField("_defaultValue");
        f.setAccessible(true);
        f.set(configKey, value);
    }

    @Test
    public void testAddFullCloneFlagOnVMwareDest(){
        strategy.addFullCloneAndDiskprovisiongStrictnessFlagOnVMwareDest(dataTO);
        verify(dataStoreTO).setFullCloneFlag(FULL_CLONE_FLAG);
    }

    @Test
    public void testAddFullCloneFlagOnNotVmwareDest(){
        verify(dataStoreTO, never()).setFullCloneFlag(any(Boolean.class));
    }

    @Test
    public void testCanBypassSecondaryStorageForDirectDownload() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(true).when(srcVolumeInfo).isDirectDownload();

        VolumeObject destVolumeInfo = Mockito.spy(new VolumeObject());

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);
    }

    @Test
    public void testCanBypassSecondaryStorageForUnsupportedDataObject() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(srcVolumeInfo).getHypervisorType();

        TemplateObject destTemplateInfo = Mockito.spy(new TemplateObject());

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destTemplateInfo);
        Assert.assertFalse(canBypassSecondaryStorage);
    }

    @Test
    public void testCanBypassSecondaryStorageForUnsupportedSrcPoolType() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(srcVolumeInfo).getHypervisorType();
        DataStore srcDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ZoneScope(1L)).when(srcDataStore).getScope();
        Mockito.doReturn(srcDataStore).when(srcVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.PowerFlex).when(srcVolumeInfo).getStoragePoolType();

        VolumeObject destVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(destVolumeInfo).getHypervisorType();
        DataStore destDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ZoneScope(1L)).when(destDataStore).getScope();
        Mockito.doReturn(destDataStore).when(destVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(destVolumeInfo).getStoragePoolType();

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destVolumeInfo);
        Assert.assertFalse(canBypassSecondaryStorage);
    }

    @Test
    public void testCanBypassSecondaryStorageForUnsupportedDestPoolType() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(srcVolumeInfo).getHypervisorType();
        DataStore srcDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ZoneScope(1L)).when(srcDataStore).getScope();
        Mockito.doReturn(srcDataStore).when(srcVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(srcVolumeInfo).getStoragePoolType();

        VolumeObject destVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(destVolumeInfo).getHypervisorType();
        DataStore destDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ZoneScope(1L)).when(destDataStore).getScope();
        Mockito.doReturn(destDataStore).when(destVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.Iscsi).when(destVolumeInfo).getStoragePoolType();

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destVolumeInfo);
        Assert.assertFalse(canBypassSecondaryStorage);
    }

    @Test
    public void testCanBypassSecondaryStorageWithZoneWideNFSPoolsInSameZone() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(srcVolumeInfo).getHypervisorType();
        DataStore srcDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ZoneScope(1L)).when(srcDataStore).getScope();
        Mockito.doReturn(srcDataStore).when(srcVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(srcVolumeInfo).getStoragePoolType();

        VolumeObject destVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(destVolumeInfo).getHypervisorType();
        DataStore destDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ZoneScope(1L)).when(destDataStore).getScope();
        Mockito.doReturn(destDataStore).when(destVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(destVolumeInfo).getStoragePoolType();

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);
    }

    @Test
    public void testCanBypassSecondaryStorageWithClusterWideNFSPoolsInSameCluster() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(srcVolumeInfo).getHypervisorType();
        DataStore srcDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ClusterScope(5L, 2L, 1L)).when(srcDataStore).getScope();
        Mockito.doReturn(srcDataStore).when(srcVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(srcVolumeInfo).getStoragePoolType();

        VolumeObject destVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(destVolumeInfo).getHypervisorType();
        DataStore destDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ClusterScope(5L, 2L, 1L)).when(destDataStore).getScope();
        Mockito.doReturn(destDataStore).when(destVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(destVolumeInfo).getStoragePoolType();

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);
    }

    @Test
    public void testCanBypassSecondaryStorageWithLocalAndClusterWideNFSPoolsInSameCluster() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(srcVolumeInfo).getHypervisorType();
        DataStore srcDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new HostScope(1L, 1L, 1L)).when(srcDataStore).getScope();
        Mockito.doReturn(srcDataStore).when(srcVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.Filesystem).when(srcVolumeInfo).getStoragePoolType();

        VolumeObject destVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(destVolumeInfo).getHypervisorType();
        DataStore destDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ClusterScope(1L, 1L, 1L)).when(destDataStore).getScope();
        Mockito.doReturn(destDataStore).when(destVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(destVolumeInfo).getStoragePoolType();

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);

        canBypassSecondaryStorage = (boolean) method.invoke(strategy, destVolumeInfo, srcVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);
    }

    @Test
    public void testCanBypassSecondaryStorageWithLocalAndZoneWideNFSPoolsInSameZone() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(srcVolumeInfo).getHypervisorType();
        DataStore srcDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new HostScope(1L, 1L, 1L)).when(srcDataStore).getScope();
        Mockito.doReturn(srcDataStore).when(srcVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.Filesystem).when(srcVolumeInfo).getStoragePoolType();

        VolumeObject destVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(destVolumeInfo).getHypervisorType();
        DataStore destDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ZoneScope(1L)).when(destDataStore).getScope();
        Mockito.doReturn(destDataStore).when(destVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(destVolumeInfo).getStoragePoolType();

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);

        canBypassSecondaryStorage = (boolean) method.invoke(strategy, destVolumeInfo, srcVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);
    }

    @Test
    public void testCanBypassSecondaryStorageWithClusterWideNFSAndZoneWideNFSPoolsInSameZone() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        VolumeObject srcVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(srcVolumeInfo).getHypervisorType();
        DataStore srcDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ClusterScope(5L, 2L, 1L)).when(srcDataStore).getScope();
        Mockito.doReturn(srcDataStore).when(srcVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(srcVolumeInfo).getStoragePoolType();

        VolumeObject destVolumeInfo = Mockito.spy(new VolumeObject());
        Mockito.doReturn(HypervisorType.KVM).when(destVolumeInfo).getHypervisorType();
        DataStore destDataStore = Mockito.mock(DataStore.class);
        Mockito.doReturn(new ZoneScope(1L)).when(destDataStore).getScope();
        Mockito.doReturn(destDataStore).when(destVolumeInfo).getDataStore();
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(destVolumeInfo).getStoragePoolType();

        Method method;
        method = AncientDataMotionStrategy.class.getDeclaredMethod("canBypassSecondaryStorage", DataObject.class, DataObject.class);
        method.setAccessible(true);
        boolean canBypassSecondaryStorage = (boolean) method.invoke(strategy, srcVolumeInfo, destVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);

        canBypassSecondaryStorage = (boolean) method.invoke(strategy, destVolumeInfo, srcVolumeInfo);
        Assert.assertTrue(canBypassSecondaryStorage);
    }

    private SnapshotDataStoreVO snapshotRef(long snapshotId, DataStoreRole role) {
        SnapshotDataStoreVO ref = Mockito.mock(SnapshotDataStoreVO.class);
        Mockito.lenient().when(ref.getSnapshotId()).thenReturn(snapshotId);
        Mockito.lenient().when(ref.getRole()).thenReturn(role);
        return ref;
    }

    /** A stopped KVM volume on a zone wide RBD pool, with the given snapshot store refs. */
    private VolumeObject rbdVolume(long id, SnapshotDataStoreVO... refs) {
        VolumeObject volume = Mockito.spy(new VolumeObject());
        DataStore store = Mockito.mock(DataStore.class);
        Mockito.lenient().when(store.getScope()).thenReturn(new ZoneScope(1L));
        Mockito.lenient().doReturn(id).when(volume).getId();
        Mockito.lenient().doReturn("volume-" + id).when(volume).toString();
        Mockito.lenient().doReturn(store).when(volume).getDataStore();
        Mockito.lenient().doReturn(false).when(volume).isDirectDownload();
        Mockito.lenient().doReturn(HypervisorType.KVM).when(volume).getHypervisorType();
        Mockito.lenient().doReturn(Storage.StoragePoolType.RBD).when(volume).getStoragePoolType();
        Mockito.lenient().doReturn(null).when(volume).getPassphraseId();
        DataTO to = Mockito.mock(DataTO.class);
        Mockito.lenient().when(to.getPath()).thenReturn("image-" + id);
        Mockito.lenient().doReturn("image-" + id).when(volume).getPath();
        Mockito.lenient().doReturn(to).when(volume).getTO();
        Mockito.lenient().when(snapshotDataStoreDao.listReadyByVolumeId(id)).thenReturn(Arrays.asList(refs));
        return volume;
    }

    private VolumeObject volumeWithPrimaryOnlySnapshot(long id) {
        return rbdVolume(id, snapshotRef(1L, DataStoreRole.Primary), snapshotRef(1L, DataStoreRole.Image), snapshotRef(2L, DataStoreRole.Primary));
    }

    private EndPoint endPointAnswering(VolumeObject srcVolume, VolumeObject destVolume, String newPath) {
        EndPoint ep = Mockito.mock(EndPoint.class);
        when(selector.select(srcVolume, destVolume, false)).thenReturn(ep);
        DataTO newData = Mockito.mock(DataTO.class);
        Mockito.lenient().when(newData.getPath()).thenReturn(newPath);
        when(ep.sendMessage(any())).thenReturn(new CopyCmdAnswer(newData));
        return ep;
    }

    @Test
    public void mustKeepPrimarySnapshotsWhenASnapshotHasNoSecondaryCopy() {
        Assert.assertTrue(strategy.mustKeepPrimarySnapshots(volumeWithPrimaryOnlySnapshot(10L), rbdVolume(11L)));
    }

    @Test
    public void mustKeepPrimarySnapshotsNotWhenEverySnapshotIsOnSecondary() {
        VolumeObject volume = rbdVolume(10L, snapshotRef(1L, DataStoreRole.Primary), snapshotRef(1L, DataStoreRole.Image), snapshotRef(2L, DataStoreRole.Image));
        Assert.assertFalse(strategy.mustKeepPrimarySnapshots(volume, rbdVolume(11L)));
    }

    @Test
    public void mustKeepPrimarySnapshotsNotWithoutSnapshots() {
        Assert.assertFalse(strategy.mustKeepPrimarySnapshots(rbdVolume(10L), rbdVolume(11L)));
    }

    @Test
    public void mustKeepPrimarySnapshotsOnlyBetweenRbdPools() {
        VolumeObject destVolume = rbdVolume(11L);
        Mockito.doReturn(Storage.StoragePoolType.NetworkFilesystem).when(destVolume).getStoragePoolType();
        Assert.assertFalse(strategy.mustKeepPrimarySnapshots(volumeWithPrimaryOnlySnapshot(10L), destVolume));
    }

    @Test
    public void mustKeepPrimarySnapshotsOnlyOnKvm() {
        VolumeObject srcVolume = volumeWithPrimaryOnlySnapshot(10L);
        Mockito.doReturn(HypervisorType.XenServer).when(srcVolume).getHypervisorType();
        Assert.assertFalse(strategy.mustKeepPrimarySnapshots(srcVolume, rbdVolume(11L)));
    }

    @Test
    public void copyVolumeBetweenPoolsAsksTheAgentToKeepPrimaryOnlySnapshots() {
        VolumeObject srcVolume = volumeWithPrimaryOnlySnapshot(10L);
        VolumeObject destVolume = rbdVolume(11L);
        EndPoint ep = endPointAnswering(srcVolume, destVolume, "image-10");

        Answer answer = strategy.copyVolumeBetweenPools(srcVolume, destVolume);

        Assert.assertTrue(answer.getResult());
        ArgumentCaptor<CopyCommand> cmd = ArgumentCaptor.forClass(CopyCommand.class);
        verify(ep).sendMessage(cmd.capture());
        Assert.assertEquals("true", cmd.getValue().getOptions().get(CopyCommand.VOLUME_MIGRATION));
        Assert.assertEquals("true", cmd.getValue().getOptions().get(CopyCommand.KEEP_PRIMARY_SNAPSHOTS));
    }

    @Test
    public void copyVolumeBetweenPoolsCopiesDirectlyDespiteACacheStoreWhenSnapshotsMustBeKept() {
        VolumeObject srcVolume = volumeWithPrimaryOnlySnapshot(10L);
        VolumeObject destVolume = rbdVolume(11L);
        when(cacheMgr.getCacheStorage(any())).thenReturn(Mockito.mock(DataStore.class));
        EndPoint ep = endPointAnswering(srcVolume, destVolume, "image-10");

        Assert.assertTrue(strategy.copyVolumeBetweenPools(srcVolume, destVolume).getResult());

        verify(ep).sendMessage(any());
        verify(cacheMgr, never()).createCacheObject(any(DataObject.class), any(Scope.class));
    }

    @Test
    public void copyVolumeBetweenPoolsFailsWhenTheAgentDidNotKeepTheImage() {
        VolumeObject srcVolume = volumeWithPrimaryOnlySnapshot(10L);
        VolumeObject destVolume = rbdVolume(11L);
        endPointAnswering(srcVolume, destVolume, "new-image.raw");

        Answer answer = strategy.copyVolumeBetweenPools(srcVolume, destVolume);

        Assert.assertFalse(answer.getResult());
        Assert.assertTrue(answer.getDetails().contains("new-image.raw"));
        // the copy is reported, never removed on a guess
        verify(volDao, never()).update(Mockito.anyLong(), any());
    }

    @Test
    public void copyVolumeBetweenPoolsAcceptsANewImageWithoutSnapshotsToKeep() {
        VolumeObject srcVolume = rbdVolume(10L);
        VolumeObject destVolume = rbdVolume(11L);
        EndPoint ep = endPointAnswering(srcVolume, destVolume, "new-image.raw");

        Assert.assertTrue(strategy.copyVolumeBetweenPools(srcVolume, destVolume).getResult());

        ArgumentCaptor<CopyCommand> cmd = ArgumentCaptor.forClass(CopyCommand.class);
        verify(ep).sendMessage(cmd.capture());
        Assert.assertNull(cmd.getValue().getOptions().get(CopyCommand.KEEP_PRIMARY_SNAPSHOTS));
    }

    @Test
    public void copyVolumeBetweenPoolsRefusesAStagedCopyOfAVolumeWithPrimaryOnlySnapshots() {
        VolumeObject srcVolume = volumeWithPrimaryOnlySnapshot(10L);
        VolumeObject destVolume = rbdVolume(11L);
        // pools of two different clusters cannot be copied between directly
        DataStore srcStore = Mockito.mock(DataStore.class);
        when(srcStore.getScope()).thenReturn(new ClusterScope(1L, 1L, 1L));
        Mockito.doReturn(srcStore).when(srcVolume).getDataStore();
        DataStore destStore = Mockito.mock(DataStore.class);
        when(destStore.getScope()).thenReturn(new ClusterScope(2L, 1L, 1L));
        Mockito.doReturn(destStore).when(destVolume).getDataStore();

        Answer answer = strategy.copyVolumeBetweenPools(srcVolume, destVolume);

        Assert.assertFalse(answer.getResult());
        Assert.assertTrue(answer.getDetails().contains("exist only on its primary storage"));
        verify(selector, never()).select(any(DataObject.class), any(DataObject.class), Mockito.anyBoolean());
    }
}
