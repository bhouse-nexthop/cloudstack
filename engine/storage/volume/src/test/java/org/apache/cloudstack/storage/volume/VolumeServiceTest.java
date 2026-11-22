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

package org.apache.cloudstack.storage.volume;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.storage.CheckAndRepairVolumeAnswer;
import com.cloud.agent.api.storage.CheckAndRepairVolumeCommand;
import com.cloud.agent.api.to.StorageFilerTO;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.exception.StorageUnavailableException;
import com.cloud.host.HostVO;
import com.cloud.host.dao.HostDao;
import com.cloud.storage.DataStoreRole;
import com.cloud.storage.CheckAndRepairVolumePayload;
import com.cloud.storage.DiskOfferingVO;
import com.cloud.storage.Storage;
import com.cloud.storage.StorageManager;
import com.cloud.storage.StoragePool;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.DiskOfferingDao;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.storage.snapshot.SnapshotManager;
import com.cloud.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.cloudstack.engine.subsystem.api.storage.ObjectInDataStoreStateMachine;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeService;
import org.apache.cloudstack.framework.async.AsyncCallFuture;
import org.apache.cloudstack.storage.command.CopyCmdAnswer;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.SnapshotDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.SnapshotDataStoreVO;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import junit.framework.TestCase;

@RunWith(MockitoJUnitRunner.class)
public class VolumeServiceTest extends TestCase{

    @Spy
    VolumeServiceImpl volumeServiceImplSpy;

    @Mock
    VolumeDataFactory volumeDataFactoryMock;

    @Mock
    VolumeInfo volumeInfoMock;

    @Mock
    AsyncCallFuture<VolumeService.VolumeApiResult> asyncCallFutureVolumeApiResultMock;

    @Mock
    VolumeService.VolumeApiResult volumeApiResultMock;

    @Mock
    VolumeDao volumeDaoMock;

    @Mock
    SnapshotManager snapshotManagerMock;

    @Mock
    StorageManager storageManagerMock;

    @Mock
    PrimaryDataStoreDao primaryDataStoreDao;

    @Mock
    VolumeVO volumeVoMock;

    @Mock
    HostVO hostMock;

    @Mock
    HostDao hostDaoMock;

    @Mock
    DiskOfferingDao diskOfferingDaoMock;

    @Mock
    SnapshotDataStoreDao snapshotDataStoreDaoMock;

    @Before
    public void setup(){
        volumeServiceImplSpy = Mockito.spy(new VolumeServiceImpl());
        volumeServiceImplSpy.volFactory = volumeDataFactoryMock;
        volumeServiceImplSpy.volDao = volumeDaoMock;
        volumeServiceImplSpy.snapshotMgr = snapshotManagerMock;
        volumeServiceImplSpy._storageMgr = storageManagerMock;
        volumeServiceImplSpy._hostDao = hostDaoMock;
        volumeServiceImplSpy.storagePoolDao = primaryDataStoreDao;
        volumeServiceImplSpy.diskOfferingDao = diskOfferingDaoMock;
        volumeServiceImplSpy._snapshotStoreDao = snapshotDataStoreDaoMock;
    }

    @Test(expected = InterruptedException.class)
    public void validateExpungeSourceVolumeAfterMigrationThrowInterruptedExceptionOnFirstFutureGetCall() throws InterruptedException, ExecutionException{
        Mockito.doReturn(volumeInfoMock).when(volumeDataFactoryMock).getVolume(Mockito.anyLong());
        Mockito.doReturn(asyncCallFutureVolumeApiResultMock).when(volumeServiceImplSpy).expungeVolumeAsync(Mockito.any());
        Mockito.doThrow(new InterruptedException()).when(asyncCallFutureVolumeApiResultMock).get();

        volumeServiceImplSpy.expungeSourceVolumeAfterMigration(new VolumeVO() {}, false);
    }

    @Test(expected = ExecutionException.class)
    public void validateExpungeSourceVolumeAfterMigrationThrowExecutionExceptionOnFirstFutureGetCall() throws InterruptedException, ExecutionException{
        Mockito.doReturn(volumeInfoMock).when(volumeDataFactoryMock).getVolume(Mockito.anyLong());
        Mockito.doReturn(asyncCallFutureVolumeApiResultMock).when(volumeServiceImplSpy).expungeVolumeAsync(Mockito.any());
        Mockito.doThrow(new ExecutionException() {}).when(asyncCallFutureVolumeApiResultMock).get();

        volumeServiceImplSpy.expungeSourceVolumeAfterMigration(new VolumeVO() {}, false);
    }

    @Test
    public void validateExpungeSourceVolumeAfterMigrationVolumeApiResultSucceedDoNoMoreInteractions() throws InterruptedException, ExecutionException{
        Mockito.doReturn(volumeInfoMock).when(volumeDataFactoryMock).getVolume(Mockito.anyLong());
        Mockito.doReturn(asyncCallFutureVolumeApiResultMock).when(volumeServiceImplSpy).expungeVolumeAsync(Mockito.any());
        Mockito.doReturn(volumeApiResultMock).when(asyncCallFutureVolumeApiResultMock).get();
        Mockito.doReturn(true).when(volumeApiResultMock).isSuccess();

        volumeServiceImplSpy.expungeSourceVolumeAfterMigration(new VolumeVO() {}, false);
        Mockito.verify(volumeApiResultMock, Mockito.never()).getResult();
    }

    @Test
    public void validateExpungeSourceVolumeAfterMigrationVolumeApiResultFailedDoNotRetryExpungeVolume() throws InterruptedException, ExecutionException{
        Mockito.doReturn(volumeInfoMock).when(volumeDataFactoryMock).getVolume(Mockito.anyLong());
        Mockito.doReturn(asyncCallFutureVolumeApiResultMock).when(volumeServiceImplSpy).expungeVolumeAsync(Mockito.any());
        Mockito.doReturn(volumeApiResultMock).when(asyncCallFutureVolumeApiResultMock).get();
        Mockito.doReturn(false).when(volumeApiResultMock).isSuccess();
        boolean retryExpungeVolume = false;

        volumeServiceImplSpy.expungeSourceVolumeAfterMigration(new VolumeVO() {}, retryExpungeVolume);
        Mockito.verify(volumeServiceImplSpy, Mockito.times(1)).expungeVolumeAsync(volumeInfoMock);
    }

    @Test (expected = InterruptedException.class)
    public void validateExpungeSourceVolumeAfterMigrationVolumeApiResultFailedRetryExpungeVolumeThrowInterruptedException() throws InterruptedException, ExecutionException{
        Mockito.doReturn(volumeInfoMock).when(volumeDataFactoryMock).getVolume(Mockito.anyLong());
        Mockito.doReturn(asyncCallFutureVolumeApiResultMock).when(volumeServiceImplSpy).expungeVolumeAsync(Mockito.any());
        Mockito.doReturn(volumeApiResultMock).doThrow(new InterruptedException()).when(asyncCallFutureVolumeApiResultMock).get();
        Mockito.doReturn(false).when(volumeApiResultMock).isSuccess();
        boolean retryExpungeVolume = true;

        volumeServiceImplSpy.expungeSourceVolumeAfterMigration(new VolumeVO() {}, retryExpungeVolume);
    }

    @Test (expected = ExecutionException.class)
    public void validateExpungeSourceVolumeAfterMigrationVolumeApiResultFailedRetryExpungeVolumeThrowExecutionException() throws InterruptedException, ExecutionException{
        Mockito.doReturn(volumeInfoMock).when(volumeDataFactoryMock).getVolume(Mockito.anyLong());
        Mockito.doReturn(asyncCallFutureVolumeApiResultMock).when(volumeServiceImplSpy).expungeVolumeAsync(Mockito.any());
        Mockito.doReturn(volumeApiResultMock).doThrow(new ExecutionException(){}).when(asyncCallFutureVolumeApiResultMock).get();
        Mockito.doReturn(false).when(volumeApiResultMock).isSuccess();
        boolean retryExpungeVolume = true;

        volumeServiceImplSpy.expungeSourceVolumeAfterMigration(new VolumeVO() {}, retryExpungeVolume);
    }

    @Test
    public void validateCopyPoliciesBetweenVolumesAndDestroySourceVolumeAfterMigrationReturnTrueOrFalse() throws ExecutionException, InterruptedException{
        VolumeObject volumeObject = Mockito.mock(VolumeObject.class);
        Mockito.doReturn(new VolumeVO() {}).when(volumeObject).getVolume();
        volumeObject.configure(null, new VolumeVO() {});

        Mockito.doNothing().when(snapshotManagerMock).copySnapshotPoliciesBetweenVolumes(Mockito.any(), Mockito.any());
        Mockito.doReturn(true, false).when(volumeServiceImplSpy).destroySourceVolumeAfterMigration(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyBoolean());

        boolean result = volumeServiceImplSpy.copyPoliciesBetweenVolumesAndDestroySourceVolumeAfterMigration(ObjectInDataStoreStateMachine.Event.DestroyRequested, null,
          volumeObject, volumeObject, true);
        boolean result2 = volumeServiceImplSpy.copyPoliciesBetweenVolumesAndDestroySourceVolumeAfterMigration(ObjectInDataStoreStateMachine.Event.DestroyRequested, null,
          volumeObject, volumeObject, true);

        Assert.assertTrue(result);
        Assert.assertFalse(result2);
    }

    @Test (expected = Exception.class)
    public void validateCopyPoliciesBetweenVolumesAndDestroySourceVolumeAfterMigrationThrowAnyOtherException() throws
      ExecutionException, InterruptedException{
        VolumeObject volumeObject = Mockito.mock(VolumeObject.class);
        Mockito.doReturn(new VolumeVO() {}).when(volumeObject).getVolume();

        volumeServiceImplSpy.copyPoliciesBetweenVolumesAndDestroySourceVolumeAfterMigration(ObjectInDataStoreStateMachine.Event.DestroyRequested, null, volumeObject,
          volumeObject, true);
    }

    @Test
    public void validateDestroySourceVolumeAfterMigrationReturnTrue() throws ExecutionException, InterruptedException{
        VolumeObject volumeObject = Mockito.mock(VolumeObject.class);
        Mockito.doReturn(new VolumeVO() {}).when(volumeObject).getVolume();

        Mockito.doReturn(true).when(volumeDaoMock).updateUuid(Mockito.anyLong(), Mockito.anyLong());
        Mockito.doNothing().when(volumeServiceImplSpy).destroyVolume(Mockito.anyLong());
        Mockito.doNothing().when(volumeServiceImplSpy).expungeSourceVolumeAfterMigration(Mockito.any(), Mockito.anyBoolean());

        boolean result = volumeServiceImplSpy.destroySourceVolumeAfterMigration(ObjectInDataStoreStateMachine.Event.DestroyRequested, null, volumeObject,
          volumeObject, true);

        Assert.assertTrue(result);
    }

    @Test
    public void validateDestroySourceVolumeAfterMigrationExpungeSourceVolumeAfterMigrationThrowExceptionReturnFalse() throws
      ExecutionException, InterruptedException{
        VolumeVO vo = new VolumeVO() {};
        vo.setPoolType(Storage.StoragePoolType.Filesystem);

        VolumeObject volumeObject = Mockito.mock(VolumeObject.class);
        Mockito.doReturn(vo).when(volumeObject).getVolume();
        vo.setPoolId(1L);

        List<Exception> exceptions = new ArrayList<>(Arrays.asList(new InterruptedException(), new ExecutionException() {}));

        for (Exception exception : exceptions) {
            Mockito.doReturn(true).when(volumeDaoMock).updateUuid(Mockito.anyLong(), Mockito.anyLong());
            Mockito.doNothing().when(volumeServiceImplSpy).destroyVolume(Mockito.anyLong());
            Mockito.doThrow(exception).when(volumeServiceImplSpy).expungeSourceVolumeAfterMigration(Mockito.any(), Mockito.anyBoolean());

            boolean result = volumeServiceImplSpy.destroySourceVolumeAfterMigration(ObjectInDataStoreStateMachine.Event.DestroyRequested, null,
              volumeObject, volumeObject, true);

            Assert.assertFalse(result);
        }
    }

    @Test (expected = Exception.class)
    public void validateDestroySourceVolumeAfterMigrationThrowAnyOtherException() throws
      ExecutionException, InterruptedException{
        VolumeObject volumeObject = new VolumeObject();
        volumeObject.configure(null, new VolumeVO() {});

        volumeServiceImplSpy.destroySourceVolumeAfterMigration(ObjectInDataStoreStateMachine.Event.DestroyRequested, null, volumeObject,
          volumeObject, true);
    }

    @Test
    public void testCheckAndRepairVolume() throws StorageUnavailableException {
        VolumeInfo volume = Mockito.mock(VolumeInfo.class);
        Mockito.when(volume.getPoolId()).thenReturn(1L);
        StoragePool pool = Mockito.mock(StoragePool.class);
        Mockito.when(storageManagerMock.getStoragePool(1L)).thenReturn(pool);
        List<Long> hostIds = new ArrayList<>();
        hostIds.add(1L);
        Mockito.when(storageManagerMock.getUpHostsInPool(1L)).thenReturn(hostIds);
        Mockito.when(hostMock.getId()).thenReturn(1L);
        Mockito.when(hostDaoMock.findById(1L)).thenReturn(hostMock);

        CheckAndRepairVolumePayload payload = new CheckAndRepairVolumePayload(null);
        Mockito.when(volume.getpayload()).thenReturn(payload);
        Mockito.when(volume.getPath()).thenReturn("cbac516a-0f1f-4559-921c-1a7c6c408ccf");
        Mockito.when(volume.getPassphrase()).thenReturn(new byte[] {3, 1, 2, 3});
        Mockito.when(volume.getEncryptFormat()).thenReturn("LUKS");

        String checkResult = "{\n" +
                "    \"image-end-offset\": 6442582016,\n" +
                "    \"total-clusters\": 163840,\n" +
                "    \"check-errors\": 0,\n" +
                "    \"leaks\": 124,\n" +
                "    \"allocated-clusters\": 98154,\n" +
                "    \"filename\": \"/var/lib/libvirt/images/26be20c7-b9d0-43f6-a76e-16c70737a0e0\",\n" +
                "    \"format\": \"qcow2\",\n" +
                "    \"fragmented-clusters\": 96135\n" +
                "}";

        CheckAndRepairVolumeCommand command = new CheckAndRepairVolumeCommand(volume.getPath(), new StorageFilerTO(pool), payload.getRepair(),
                volume.getPassphrase(), volume.getEncryptFormat());

        CheckAndRepairVolumeAnswer answer = new CheckAndRepairVolumeAnswer(command, true, checkResult);
        answer.setVolumeCheckExecutionResult(checkResult);
        Mockito.when(storageManagerMock.sendToPool(pool, new long[]{1L}, command)).thenReturn(answer);

        Pair<String, String> result = volumeServiceImplSpy.checkAndRepairVolume(volume);

        Assert.assertEquals(result.first(), checkResult);
        Assert.assertEquals(result.second(), null);
    }

    @Test
    public void testCheckAndRepairVolumeWhenFailure() throws StorageUnavailableException {
        VolumeInfo volume = Mockito.mock(VolumeInfo.class);
        Mockito.when(volume.getPoolId()).thenReturn(1L);
        StoragePool pool = Mockito.mock(StoragePool.class);
        Mockito.when(storageManagerMock.getStoragePool(1L)).thenReturn(pool);
        List<Long> hostIds = new ArrayList<>();
        hostIds.add(1L);
        Mockito.when(storageManagerMock.getUpHostsInPool(1L)).thenReturn(hostIds);
        Mockito.when(hostMock.getId()).thenReturn(1L);
        Mockito.when(hostDaoMock.findById(1L)).thenReturn(hostMock);

        CheckAndRepairVolumePayload payload = new CheckAndRepairVolumePayload(null);
        Mockito.when(volume.getpayload()).thenReturn(payload);
        Mockito.when(volume.getPath()).thenReturn("cbac516a-0f1f-4559-921c-1a7c6c408ccf");
        Mockito.when(volume.getPassphrase()).thenReturn(new byte[] {3, 1, 2, 3});
        Mockito.when(volume.getEncryptFormat()).thenReturn("LUKS");

        CheckAndRepairVolumeCommand command = new CheckAndRepairVolumeCommand(volume.getPath(), new StorageFilerTO(pool), payload.getRepair(),
                volume.getPassphrase(), volume.getEncryptFormat());

        CheckAndRepairVolumeAnswer answer = new CheckAndRepairVolumeAnswer(command, false, "Unable to execute qemu command");
        Mockito.when(storageManagerMock.sendToPool(pool, new long[]{1L}, command)).thenReturn(answer);

        Pair<String, String> result = volumeServiceImplSpy.checkAndRepairVolume(volume);

        Assert.assertEquals(null, result);
    }

    @Test
    public void validateDiskOfferingCheckForEncryption1Test() {
        prepareOfferingsForEncryptionValidation(1L, true);
        prepareOfferingsForEncryptionValidation(2L, true);
        volumeServiceImplSpy.validateChangeDiskOfferingEncryptionType(1L, 2L);
    }

    @Test
    public void validateDiskOfferingCheckForEncryption2Test() {
        prepareOfferingsForEncryptionValidation(1L, false);
        prepareOfferingsForEncryptionValidation(2L, false);
        volumeServiceImplSpy.validateChangeDiskOfferingEncryptionType(1L, 2L);
    }

    @Test (expected = InvalidParameterValueException.class)
    public void validateDiskOfferingCheckForEncryptionFail1Test() {
        prepareOfferingsForEncryptionValidation(1L, false);
        prepareOfferingsForEncryptionValidation(2L, true);
        volumeServiceImplSpy.validateChangeDiskOfferingEncryptionType(1L, 2L);
    }

    @Test (expected = InvalidParameterValueException.class)
    public void validateDiskOfferingCheckForEncryptionFail2Test() {
        prepareOfferingsForEncryptionValidation(1L, true);
        prepareOfferingsForEncryptionValidation(2L, false);
        volumeServiceImplSpy.validateChangeDiskOfferingEncryptionType(1L, 2L);
    }

    private void prepareOfferingsForEncryptionValidation(long diskOfferingId, boolean encryption) {
        DiskOfferingVO diskOffering = Mockito.mock(DiskOfferingVO.class);

        Mockito.when(diskOffering.getEncrypt()).thenReturn(encryption);
        Mockito.when(diskOfferingDaoMock.findByIdIncludingRemoved(diskOfferingId)).thenReturn(diskOffering);
        Mockito.when(diskOfferingDaoMock.findById(diskOfferingId)).thenReturn(diskOffering);
    }

    private VolumeInfo mockRbdVolume(long id, long poolId, String path) {
        VolumeInfo volume = Mockito.mock(VolumeInfo.class);
        Mockito.lenient().when(volume.getId()).thenReturn(id);
        Mockito.lenient().when(volume.getPoolId()).thenReturn(poolId);
        Mockito.lenient().when(volume.getPath()).thenReturn(path);
        Mockito.lenient().when(volume.getStoragePoolType()).thenReturn(Storage.StoragePoolType.RBD);
        return volume;
    }

    private Answer copyAnswer(String path) {
        VolumeObjectTO newVolume = new VolumeObjectTO();
        newVolume.setPath(path);
        return new CopyCmdAnswer(newVolume);
    }

    @Test
    public void moveRbdSnapshotsWithMigratedVolumeMovesThePrimarySnapshotsToTheNewPool() {
        VolumeInfo srcVolume = mockRbdVolume(1L, 10L, "image");
        VolumeInfo destVolume = mockRbdVolume(2L, 20L, null);
        StoragePoolVO srcPool = Mockito.mock(StoragePoolVO.class);
        Mockito.when(srcPool.getId()).thenReturn(10L);
        Mockito.when(srcPool.getPoolType()).thenReturn(Storage.StoragePoolType.RBD);
        Mockito.when(srcPool.getPath()).thenReturn("vm-images");
        Mockito.when(primaryDataStoreDao.findById(10L)).thenReturn(srcPool);
        StoragePoolVO destPool = Mockito.mock(StoragePoolVO.class);
        Mockito.when(destPool.getId()).thenReturn(20L);
        Mockito.when(destPool.getPath()).thenReturn("vm-images-ec");
        Mockito.when(destPool.getPoolType()).thenReturn(Storage.StoragePoolType.RBD);
        Mockito.when(primaryDataStoreDao.findById(20L)).thenReturn(destPool);
        SnapshotDataStoreVO onSourcePool = Mockito.spy(new SnapshotDataStoreVO(10L, 5L));
        Mockito.doReturn(100L).when(onSourcePool).getId();
        onSourcePool.setInstallPath("vm-images/image/snap");
        SnapshotDataStoreVO onOtherPool = new SnapshotDataStoreVO(30L, 6L);
        onOtherPool.setInstallPath("other/image/snap");
        onOtherPool.setVolumeId(1L);
        Mockito.when(snapshotDataStoreDaoMock.listAllByVolumeAndDataStore(1L, DataStoreRole.Primary)).thenReturn(Arrays.asList(onSourcePool, onOtherPool));

        volumeServiceImplSpy.moveRbdSnapshotsWithMigratedVolume(srcVolume, destVolume, copyAnswer("image"));

        Assert.assertEquals(20L, onSourcePool.getDataStoreId());
        Assert.assertEquals("vm-images-ec/image/snap", onSourcePool.getInstallPath());
        Assert.assertEquals(Long.valueOf(2L), onSourcePool.getVolumeId());
        Mockito.verify(snapshotDataStoreDaoMock).update(100L, onSourcePool);
        Assert.assertEquals(30L, onOtherPool.getDataStoreId());
        Assert.assertEquals("other/image/snap", onOtherPool.getInstallPath());
        Assert.assertEquals(Long.valueOf(1L), onOtherPool.getVolumeId());
        Mockito.verify(snapshotDataStoreDaoMock, Mockito.never()).update(Mockito.anyLong(), Mockito.eq(onOtherPool));
        Mockito.verify(snapshotDataStoreDaoMock, Mockito.never()).updateVolumeIds(Mockito.anyLong(), Mockito.anyLong());
    }

    @Test
    public void moveRbdSnapshotsWithMigratedVolumeDoesNothingWhenTheImageWasCopied() {
        volumeServiceImplSpy.moveRbdSnapshotsWithMigratedVolume(mockRbdVolume(1L, 10L, "image"), mockRbdVolume(2L, 20L, null), copyAnswer("other-image"));

        Mockito.verifyNoInteractions(snapshotDataStoreDaoMock);
    }
}
