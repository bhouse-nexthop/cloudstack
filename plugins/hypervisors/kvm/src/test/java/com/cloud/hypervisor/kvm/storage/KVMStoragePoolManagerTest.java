//
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
//

package com.cloud.hypervisor.kvm.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.ceph.rados.exceptions.RadosException;
import com.ceph.rbd.RbdException;
import com.cloud.hypervisor.kvm.resource.KVMHABase;
import com.cloud.hypervisor.kvm.resource.KVMHAMonitor;
import com.cloud.storage.Storage.StoragePoolType;
import com.cloud.utils.exception.CloudRuntimeException;

public class KVMStoragePoolManagerTest {

    private static final String POOL_UUID = "7684f6e5-fd91-3435-b04e-59ae0efb3914";
    private static final String VOLUME = "volume";

    private final KVMHAMonitor monitor = mock(KVMHAMonitor.class);
    private final StorageAdaptor adaptor = mock(StorageAdaptor.class);
    private final Map<String, String> details = new HashMap<>();
    /** The pools which the manager knows, with the information to create them again. */
    private final Map<String, Object> storagePools = new ConcurrentHashMap<>();
    private KVMStoragePoolManager manager;
    private KVMStoragePool pool;

    @Before
    public void setUp() throws Exception {
        // The real constructor scans the classpath for storage adaptors (some of which need scripts and libraries of a
        // host), which the tests do not need.
        manager = mock(KVMStoragePoolManager.class, Mockito.CALLS_REAL_METHODS);
        // every type of pool gets the adaptor of the test
        Map<String, StorageAdaptor> adaptors = new HashMap<>();
        adaptors.put("libvirt", adaptor);
        setField("_storageMapper", adaptors);
        setField("_storagePools", storagePools);
        setField("_haMonitor", monitor);
        setField("logger", LogManager.getLogger(KVMStoragePoolManager.class));
        pool = mock(KVMStoragePool.class);
        doReturn(pool).when(manager).getStoragePool(StoragePoolType.RBD, POOL_UUID);
        doNothing().when(manager).sleepBeforeRetry(anyLong());
    }

    private void setField(String name, Object value) throws Exception {
        Field field = KVMStoragePoolManager.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(manager, value);
    }

    @Test
    public void getPhysicalDiskDoesNotRetryACephTimeout() {
        CloudRuntimeException failure = new CloudRuntimeException("open failed",
                new RbdException("Failed to open image " + VOLUME, -110));
        when(pool.getPhysicalDisk(VOLUME)).thenThrow(failure);

        CloudRuntimeException thrown = assertThrows(CloudRuntimeException.class,
                () -> manager.getPhysicalDisk(StoragePoolType.RBD, POOL_UUID, VOLUME));

        assertSame(failure, thrown);
        verify(pool, times(1)).getPhysicalDisk(VOLUME);
        verify(manager, never()).sleepBeforeRetry(anyLong());
    }

    @Test
    public void getPhysicalDiskDoesNotRetryARadosConnectFailure() {
        when(pool.getPhysicalDisk(VOLUME)).thenThrow(new CloudRuntimeException("connect failed",
                new RadosException("Failed to connect", -110)));

        assertThrows(CloudRuntimeException.class, () -> manager.getPhysicalDisk(StoragePoolType.RBD, POOL_UUID, VOLUME));

        verify(pool, times(1)).getPhysicalDisk(VOLUME);
        verify(manager, never()).sleepBeforeRetry(anyLong());
    }

    @Test
    public void getPhysicalDiskDoesNotRetryAMissingPool() {
        when(pool.getPhysicalDisk(VOLUME)).thenThrow(new CloudRuntimeException("ioctx failed",
                new RadosException("Failed to create the IoCTX", -2)));

        assertThrows(CloudRuntimeException.class, () -> manager.getPhysicalDisk(StoragePoolType.RBD, POOL_UUID, VOLUME));

        verify(pool, times(1)).getPhysicalDisk(VOLUME);
        verify(manager, never()).sleepBeforeRetry(anyLong());
    }

    @Test
    public void getPhysicalDiskDoesNotRetryATimeoutBehindANotFound() {
        RbdException timeout = new RbdException("Failed to open image " + VOLUME, -110);
        RbdException notFound = new RbdException("Failed to open image " + VOLUME, -2);
        notFound.initCause(timeout);
        RuntimeException failure = new RuntimeException("open failed", notFound);
        when(pool.getPhysicalDisk(VOLUME)).thenThrow(failure);

        CloudRuntimeException thrown = assertThrows(CloudRuntimeException.class,
                () -> manager.getPhysicalDisk(StoragePoolType.RBD, POOL_UUID, VOLUME));

        assertSame(failure, thrown.getCause());
        verify(pool, times(1)).getPhysicalDisk(VOLUME);
        verify(manager, never()).sleepBeforeRetry(anyLong());
    }

    @Test
    public void getPhysicalDiskRetriesAnImageThatIsNotThereYet() {
        KVMPhysicalDisk disk = mock(KVMPhysicalDisk.class);
        CloudRuntimeException notFound = new CloudRuntimeException("open failed",
                new RbdException("Failed to open image " + VOLUME, -2));
        when(pool.getPhysicalDisk(VOLUME)).thenThrow(notFound).thenThrow(notFound).thenReturn(disk);

        assertSame(disk, manager.getPhysicalDisk(StoragePoolType.RBD, POOL_UUID, VOLUME));

        verify(pool, times(3)).getPhysicalDisk(VOLUME);
        verify(manager, times(2)).sleepBeforeRetry(anyLong());
    }

    @Test
    public void getPhysicalDiskKeepsRetryingAnImageThatNeverAppears() {
        when(pool.getPhysicalDisk(VOLUME)).thenThrow(new CloudRuntimeException("open failed",
                new RbdException("Failed to open image " + VOLUME, -2)));

        assertThrows(CloudRuntimeException.class, () -> manager.getPhysicalDisk(StoragePoolType.RBD, POOL_UUID, VOLUME));

        // 100 tries in the loop and a last one after it
        verify(pool, times(101)).getPhysicalDisk(VOLUME);
    }

    @Test
    public void getPhysicalDiskStillRetriesAnErrorThatIsNotFromCeph() {
        doReturn(pool).when(manager).getStoragePool(StoragePoolType.NetworkFilesystem, POOL_UUID);
        doThrow(new CloudRuntimeException("libvirt lookup failed")).when(pool).getPhysicalDisk(VOLUME);

        assertThrows(CloudRuntimeException.class,
                () -> manager.getPhysicalDisk(StoragePoolType.NetworkFilesystem, POOL_UUID, VOLUME));

        verify(pool, times(101)).getPhysicalDisk(VOLUME);
        verify(manager, times(100)).sleepBeforeRetry(anyLong());
    }

    @Test(timeout = 60000)
    public void testCreateWaitingForPoolLockDoesNotHoldManagerLock() throws Exception {
        /*
         * A teardown of a pool holds that pool's lock, and can sit in an umount for as long as the
         * storage takes to answer. A create of the same pool has to wait for it, but must do so
         * without holding the manager wide lock, or the creates of every other pool on the host wait
         * behind the umount too.
         */
        final KVMStoragePoolManager manager = Mockito.mock(KVMStoragePoolManager.class, Mockito.CALLS_REAL_METHODS);
        final KVMStoragePool pool = Mockito.mock(KVMStoragePool.class);
        final String uuid = String.valueOf(UUID.randomUUID());
        Mockito.doReturn(pool).when(manager).createStoragePoolSynchronized(eq(uuid), any(), anyInt(), any(), any(), any(), any(), anyBoolean());

        final AtomicReference<KVMStoragePool> created = new AtomicReference<>();
        final Thread create;
        synchronized (KVMStoragePoolLocks.get(uuid)) {
            // the teardown in progress
            create = new Thread(() -> created.set(manager.createStoragePool(uuid, "127.0.0.1", 0, "/export/primary",
                    null, StoragePoolType.NetworkFilesystem)));
            create.start();
            while (create.getState() != Thread.State.BLOCKED) {
                Assert.assertTrue("create finished while the pool's lock was held", create.isAlive());
                Thread.sleep(10);
            }

            final CountDownLatch managerLockTaken = new CountDownLatch(1);
            final Thread otherPool = new Thread(() -> {
                synchronized (manager) {
                    managerLockTaken.countDown();
                }
            });
            otherPool.start();
            Assert.assertTrue("the manager wide lock was held by a create waiting for its pool's lock",
                    managerLockTaken.await(30, TimeUnit.SECONDS));
            Assert.assertNull(created.get());
        }

        create.join(30000);
        Assert.assertSame(pool, created.get());
    }

    @Test
    public void testPoolsWithHaSupportAreUnregisteredFromTheMonitorWhenDeleted() {
        for (StoragePoolType type : new StoragePoolType[] {StoragePoolType.NetworkFilesystem, StoragePoolType.SharedMountPoint, StoragePoolType.RBD}) {
            when(adaptor.deleteStoragePool(POOL_UUID)).thenReturn(true);
            when(adaptor.deleteStoragePool(POOL_UUID, details)).thenReturn(true);

            assertTrue(manager.deleteStoragePool(type, POOL_UUID));
            assertTrue(manager.deleteStoragePool(type, POOL_UUID, details));
        }
        verify(monitor, times(6)).unregisterStoragePool(POOL_UUID);
    }

    @Test
    public void testNfsPoolIsDeletedWithoutDetails() {
        // the delete without the details unmounts the pool
        when(adaptor.deleteStoragePool(POOL_UUID)).thenReturn(true);

        assertTrue(manager.deleteStoragePool(StoragePoolType.NetworkFilesystem, POOL_UUID, details));

        verify(monitor).unregisterStoragePool(POOL_UUID);
        verify(adaptor).deleteStoragePool(POOL_UUID);
        verify(adaptor, never()).deleteStoragePool(eq(POOL_UUID), any());
    }

    @Test
    public void testRbdAndSharedMountPointPoolsAreDeletedWithDetailsAsBefore() {
        // the delete which takes the details does not destroy the libvirt pool, which the VMs may still use, e.g. in maintenance of the pool
        for (StoragePoolType type : new StoragePoolType[] {StoragePoolType.RBD, StoragePoolType.SharedMountPoint}) {
            reset(adaptor, monitor);
            when(adaptor.deleteStoragePool(POOL_UUID, details)).thenReturn(true);

            assertTrue(manager.deleteStoragePool(type, POOL_UUID, details));

            verify(monitor).unregisterStoragePool(POOL_UUID);
            verify(adaptor).deleteStoragePool(POOL_UUID, details);
            verify(adaptor, never()).deleteStoragePool(POOL_UUID);
        }
    }

    @Test
    public void testPoolWithoutHaSupportIsNotUnregisteredFromTheMonitor() {
        when(adaptor.deleteStoragePool(POOL_UUID, details)).thenReturn(true);

        assertTrue(manager.deleteStoragePool(StoragePoolType.CLVM, POOL_UUID, details));

        verify(monitor, never()).unregisterStoragePool(any());
        verify(adaptor, never()).deleteStoragePool(POOL_UUID);
    }

    private KVMHABase.HAStoragePool monitoredPool() {
        KVMHABase.HAStoragePool haPool = mock(KVMHABase.HAStoragePool.class);
        when(monitor.getStoragePool(POOL_UUID)).thenReturn(haPool);
        return haPool;
    }

    @Test
    public void testPoolWhichIsNotDeletedIsMonitoredForHaAgainAndStaysKnown() {
        for (StoragePoolType type : new StoragePoolType[] {StoragePoolType.NetworkFilesystem, StoragePoolType.SharedMountPoint, StoragePoolType.RBD}) {
            reset(adaptor, monitor);
            storagePools.put(POOL_UUID, new Object());
            KVMHABase.HAStoragePool haPool = monitoredPool();
            // the pool is still there, so the heartbeat of the host on it must go on, and the manager must not forget it
            when(adaptor.deleteStoragePool(POOL_UUID)).thenReturn(false);
            when(adaptor.deleteStoragePool(POOL_UUID, details)).thenReturn(false);

            assertFalse(manager.deleteStoragePool(type, POOL_UUID));
            assertFalse(manager.deleteStoragePool(type, POOL_UUID, details));

            verify(monitor, times(2)).addStoragePool(haPool);
            assertTrue(storagePools.containsKey(POOL_UUID));
        }
    }

    @Test
    public void testPoolWhichDeleteThrowsIsMonitoredForHaAgain() {
        KVMHABase.HAStoragePool haPool = monitoredPool();
        when(adaptor.deleteStoragePool(POOL_UUID, details)).thenThrow(new CloudRuntimeException("busy"));

        try {
            manager.deleteStoragePool(StoragePoolType.RBD, POOL_UUID, details);
            fail("The exception of the delete is not for the manager to hide");
        } catch (CloudRuntimeException expected) {
            assertEquals("busy", expected.getMessage());
        }

        verify(monitor).addStoragePool(haPool);
    }

    @Test
    public void testPoolWhichIsDeletedIsForgottenAndNotMonitoredForHaAgain() {
        for (StoragePoolType type : new StoragePoolType[] {StoragePoolType.NetworkFilesystem, StoragePoolType.SharedMountPoint, StoragePoolType.RBD}) {
            reset(adaptor, monitor);
            monitoredPool();
            when(adaptor.deleteStoragePool(POOL_UUID)).thenReturn(true);
            when(adaptor.deleteStoragePool(POOL_UUID, details)).thenReturn(true);

            storagePools.put(POOL_UUID, new Object());
            assertTrue(manager.deleteStoragePool(type, POOL_UUID));
            assertFalse(storagePools.containsKey(POOL_UUID));
            storagePools.put(POOL_UUID, new Object());
            assertTrue(manager.deleteStoragePool(type, POOL_UUID, details));
            assertFalse(storagePools.containsKey(POOL_UUID));

            verify(monitor, never()).addStoragePool(any());
        }
    }

    @Test
    public void testPoolWithoutHaSupportWhichAdaptorHasNoSuchPoolIsForgottenAsBefore() {
        // the adaptors of these types return false if they have no such pool: its information must not stay
        storagePools.put(POOL_UUID, new Object());
        when(adaptor.deleteStoragePool(POOL_UUID, details)).thenReturn(false);

        assertFalse(manager.deleteStoragePool(StoragePoolType.CLVM, POOL_UUID, details));

        assertFalse(storagePools.containsKey(POOL_UUID));
    }

    private KVMStoragePool mockCreatedPool(String prerequisiteError) {
        KVMStoragePool pool = mock(KVMStoragePool.class);
        when(pool.getUuid()).thenReturn(POOL_UUID);
        when(pool.isPoolSupportHA()).thenReturn(true);
        when(pool.getHeartBeatPrerequisiteError()).thenReturn(prerequisiteError);
        when(adaptor.createStoragePool(any(), any(), anyInt(), any(), any(), any(), any(), eq(true))).thenReturn(pool);
        return pool;
    }

    @Test
    public void testPoolWhichHeartBeatCanBeUsedIsMonitoredForHa() {
        mockCreatedPool(null);

        manager.createStoragePool("pool", "10.0.0.1", 6789, "rbdpool", "cephuser:cephkey", StoragePoolType.RBD, details);

        verify(monitor).addStoragePool(any());
    }

    @Test
    public void testPoolWhichHeartBeatCanNotBeUsedIsNotMonitoredForHa() {
        // e.g. the Python bindings of Ceph are not installed: the host could not write its heartbeat, and would be fenced
        mockCreatedPool("the package python3-rados is not installed");

        KVMStoragePool pool = manager.createStoragePool("pool", "10.0.0.1", 6789, "rbdpool", "cephuser:cephkey", StoragePoolType.RBD, details);

        verify(monitor, never()).addStoragePool(any());
        // the pool itself is still usable
        assertNotNull(pool);
    }
}
