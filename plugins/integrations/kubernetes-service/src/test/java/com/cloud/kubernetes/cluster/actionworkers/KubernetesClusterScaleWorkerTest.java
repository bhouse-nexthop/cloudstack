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

import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.offering.ServiceOffering;
import com.cloud.service.ServiceOfferingVO;
import com.cloud.service.dao.ServiceOfferingDao;
import com.cloud.utils.Pair;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.VirtualMachine;
import com.cloud.vm.dao.UserVmDao;
import com.cloud.vm.dao.VMInstanceDao;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.CONTROL;
import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.DEFAULT;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClusterScaleWorkerTest {

    @Mock
    private KubernetesCluster kubernetesCluster;
    @Mock
    private KubernetesClusterManagerImpl clusterManager;
    @Mock
    private ServiceOfferingDao serviceOfferingDao;
    @Mock
    private KubernetesClusterVmMapDao kubernetesClusterVmMapDao;
    @Mock
    private UserVmDao userVmDao;
    @Mock
    private VMInstanceDao vmInstanceDao;

    private KubernetesClusterScaleWorker worker;

    private static final Long defaultOfferingId = 1L;

    @Before
    public void setUp() {
        worker = new KubernetesClusterScaleWorker(kubernetesCluster, clusterManager);
        worker.serviceOfferingDao = serviceOfferingDao;
        worker.kubernetesClusterVmMapDao = kubernetesClusterVmMapDao;
        worker.userVmDao = userVmDao;
    }

    @Test
    public void testCalculateNewClusterCountAndCapacityAllNodesScaleSize() {
        long controlNodes = 3L;
        long etcdNodes = 2L;
        Mockito.when(kubernetesCluster.getControlNodeCount()).thenReturn(controlNodes);
        Mockito.when(kubernetesCluster.getEtcdNodeCount()).thenReturn(etcdNodes);

        ServiceOffering newOffering = Mockito.mock(ServiceOffering.class);
        int newCores = 4;
        int newMemory = 4096;
        Mockito.when(newOffering.getCpu()).thenReturn(newCores);
        Mockito.when(newOffering.getRamSize()).thenReturn(newMemory);

        long newWorkerSize = 4L;
        Pair<Long, Long> newClusterCapacity = worker.calculateNewClusterCountAndCapacity(newWorkerSize, DEFAULT, newOffering);

        long expectedCores = (newCores * newWorkerSize) + (newCores * controlNodes) + (newCores * etcdNodes);
        long expectedMemory = (newMemory * newWorkerSize) + (newMemory * controlNodes) + (newMemory * etcdNodes);
        Assert.assertEquals(expectedCores, newClusterCapacity.first().longValue());
        Assert.assertEquals(expectedMemory, newClusterCapacity.second().longValue());
    }

    @Test
    public void testCalculateNewClusterCountAndCapacityNodeTypeScaleControlOffering() {
        long controlNodes = 2L;
        long kubernetesClusterId = 10L;
        Mockito.when(kubernetesCluster.getId()).thenReturn(kubernetesClusterId);
        Mockito.when(kubernetesCluster.getControlNodeCount()).thenReturn(controlNodes);

        ServiceOfferingVO existingOffering = Mockito.mock(ServiceOfferingVO.class);
        int existingCores = 2;
        int existingMemory = 2048;
        Mockito.when(existingOffering.getCpu()).thenReturn(existingCores);
        Mockito.when(existingOffering.getRamSize()).thenReturn(existingMemory);
        int remainingClusterCpu = 8;
        int remainingClusterMemory = 12288;
        Mockito.when(kubernetesCluster.getCores()).thenReturn(remainingClusterCpu + (controlNodes * existingCores));
        Mockito.when(kubernetesCluster.getMemory()).thenReturn(remainingClusterMemory + (controlNodes * existingMemory));

        Mockito.when(serviceOfferingDao.findById(1L)).thenReturn(existingOffering);

        ServiceOfferingVO newOffering = Mockito.mock(ServiceOfferingVO.class);
        int newCores = 4;
        int newMemory = 2048;
        Mockito.when(newOffering.getCpu()).thenReturn(newCores);
        Mockito.when(newOffering.getRamSize()).thenReturn(newMemory);

        KubernetesClusterVmMapVO controlNodeVM1 = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(controlNodeVM1.getVmId()).thenReturn(10L);
        UserVmVO userVmVO = Mockito.mock(UserVmVO.class);
        Mockito.when(userVmVO.getServiceOfferingId()).thenReturn(defaultOfferingId);
        Mockito.when(userVmDao.findById(10L)).thenReturn(userVmVO);
        Mockito.when(kubernetesClusterVmMapDao.listByClusterIdAndVmType(kubernetesClusterId, CONTROL)).thenReturn(List.of(controlNodeVM1));
        Pair<Long, Long> newClusterCapacity = worker.calculateNewClusterCountAndCapacity(null, CONTROL, newOffering);

        long expectedCores = remainingClusterCpu + (controlNodes * newCores);
        long expectedMemory = remainingClusterMemory + (controlNodes * newMemory);
        Assert.assertEquals(expectedCores, newClusterCapacity.first().longValue());
        Assert.assertEquals(expectedMemory, newClusterCapacity.second().longValue());
    }


    @Test
    public void testGetWorkerNodesToRemoveForDownsize_singleRemoval() {
        KubernetesCluster kubernetesCluster = Mockito.mock(KubernetesCluster.class);
        KubernetesClusterManagerImpl clusterManager = Mockito.mock(KubernetesClusterManagerImpl.class);
        KubernetesClusterScaleWorker worker = new KubernetesClusterScaleWorker(kubernetesCluster, new java.util.HashMap<>(), 2L, null, false, null, null, clusterManager);
        worker.vmInstanceDao = vmInstanceDao;
        KubernetesClusterScaleWorker spyWorker = Mockito.spy(worker);

        KubernetesClusterVmMapVO vm1 = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(vm1.isExternalNode()).thenReturn(false);
        Mockito.when(vm1.isControlNode()).thenReturn(false);
        Mockito.when(vm1.isEtcdNode()).thenReturn(false);

        KubernetesClusterVmMapVO vm2 = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(vm2.isExternalNode()).thenReturn(false);
        Mockito.when(vm2.isControlNode()).thenReturn(false);
        Mockito.when(vm2.isEtcdNode()).thenReturn(false);

        KubernetesClusterVmMapVO vm3 = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(vm3.isExternalNode()).thenReturn(false);
        Mockito.when(vm3.isControlNode()).thenReturn(false);
        Mockito.when(vm3.isEtcdNode()).thenReturn(false);

        Mockito.doReturn(Arrays.asList(vm1, vm2, vm3)).when(spyWorker).getKubernetesClusterVMMaps();

        List<KubernetesClusterVmMapVO> toRemove = spyWorker.getWorkerNodesToRemove();

        Assert.assertEquals(1, toRemove.size());
        Assert.assertSame(vm3, toRemove.get(0));
    }

    @Test
    public void testGetWorkerNodesToRemoveForDownsize_noRemoval() {
        KubernetesCluster kubernetesCluster = Mockito.mock(KubernetesCluster.class);

        KubernetesClusterScaleWorker worker = new KubernetesClusterScaleWorker(kubernetesCluster, new java.util.HashMap<>(), 3L, null, false, null, null, clusterManager);
        worker.vmInstanceDao = vmInstanceDao;
        KubernetesClusterScaleWorker spyWorker = Mockito.spy(worker);

        KubernetesClusterVmMapVO vm1 = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(vm1.isExternalNode()).thenReturn(false);
        Mockito.when(vm1.isControlNode()).thenReturn(false);
        Mockito.when(vm1.isEtcdNode()).thenReturn(false);

        KubernetesClusterVmMapVO vm2 = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(vm2.isExternalNode()).thenReturn(false);
        Mockito.when(vm2.isControlNode()).thenReturn(false);
        Mockito.when(vm2.isEtcdNode()).thenReturn(false);

        KubernetesClusterVmMapVO vm3 = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(vm3.isExternalNode()).thenReturn(false);
        Mockito.when(vm3.isControlNode()).thenReturn(false);
        Mockito.when(vm3.isEtcdNode()).thenReturn(false);

        Mockito.doReturn(Arrays.asList(vm1, vm2, vm3)).when(spyWorker).getKubernetesClusterVMMaps();

        List<KubernetesClusterVmMapVO> toRemove = spyWorker.getWorkerNodesToRemove();

        Assert.assertTrue(toRemove.isEmpty());
    }

    private KubernetesClusterVmMapVO mockWorkerVmMap(long vmId, VirtualMachine.State state) {
        KubernetesClusterVmMapVO vmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.lenient().when(vmMap.isExternalNode()).thenReturn(false);
        Mockito.lenient().when(vmMap.isControlNode()).thenReturn(false);
        Mockito.lenient().when(vmMap.isEtcdNode()).thenReturn(false);
        Mockito.lenient().when(vmMap.getVmId()).thenReturn(vmId);
        VMInstanceVO vm = Mockito.mock(VMInstanceVO.class);
        Mockito.lenient().when(vm.getState()).thenReturn(state);
        Mockito.lenient().when(vmInstanceDao.findById(vmId)).thenReturn(vm);
        return vmMap;
    }

    @Test
    public void testGetWorkerNodesToRemovePrefersNodesDown() {
        KubernetesClusterScaleWorker worker = new KubernetesClusterScaleWorker(kubernetesCluster, new java.util.HashMap<>(), 2L, null, false, null, null, clusterManager);
        worker.vmInstanceDao = vmInstanceDao;
        KubernetesClusterScaleWorker spyWorker = Mockito.spy(worker);

        KubernetesClusterVmMapVO vm1 = mockWorkerVmMap(1L, VirtualMachine.State.Running);
        KubernetesClusterVmMapVO vm2 = mockWorkerVmMap(2L, VirtualMachine.State.Stopped);
        KubernetesClusterVmMapVO vm3 = mockWorkerVmMap(3L, VirtualMachine.State.Running);
        KubernetesClusterVmMapVO vm4 = mockWorkerVmMap(4L, VirtualMachine.State.Running);
        Mockito.doReturn(Arrays.asList(vm1, vm2, vm3, vm4)).when(spyWorker).getKubernetesClusterVMMaps();

        List<KubernetesClusterVmMapVO> toRemove = spyWorker.getWorkerNodesToRemove();

        // The stopped node goes first, then the last running node as before
        Assert.assertEquals(2, toRemove.size());
        Assert.assertTrue(toRemove.contains(vm2));
        Assert.assertTrue(toRemove.contains(vm4));
    }

    @Test
    public void testGetDownNodeCountIgnoresEtcdNodes() {
        KubernetesClusterScaleWorker worker = new KubernetesClusterScaleWorker(kubernetesCluster, new java.util.HashMap<>(), 2L, null, false, null, null, clusterManager);
        worker.vmInstanceDao = vmInstanceDao;
        KubernetesClusterScaleWorker spyWorker = Mockito.spy(worker);

        KubernetesClusterVmMapVO running = mockWorkerVmMap(1L, VirtualMachine.State.Running);
        KubernetesClusterVmMapVO stopped = mockWorkerVmMap(2L, VirtualMachine.State.Stopped);
        KubernetesClusterVmMapVO etcd = mockWorkerVmMap(3L, VirtualMachine.State.Stopped);
        Mockito.when(etcd.isEtcdNode()).thenReturn(true);
        Mockito.doReturn(Arrays.asList(running, stopped, etcd)).when(spyWorker).getKubernetesClusterVMMaps();

        Assert.assertEquals(1L, spyWorker.getDownNodeCount(Collections.emptySet()));
        // Newly provisioned VMs aren't left out of the ready nodes
        Assert.assertEquals(0L, spyWorker.getDownNodeCount(Set.of(2L)));
    }

    @Test
    public void testIsVmDown() {
        Assert.assertTrue(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Stopped));
        Assert.assertTrue(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Shutdown));
        Assert.assertTrue(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Error));
        Assert.assertTrue(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Destroyed));
        Assert.assertTrue(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Expunging));
        // The VM may still be running in these
        Assert.assertFalse(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Running));
        Assert.assertFalse(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Migrating));
        Assert.assertFalse(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Unknown));
        Assert.assertFalse(KubernetesClusterScaleWorker.isVmDown(VirtualMachine.State.Starting));
    }

    private UserVmVO mockNodeVm(String hostName) {
        UserVmVO vm = Mockito.mock(UserVmVO.class);
        Mockito.when(vm.getHostName()).thenReturn(hostName);
        return vm;
    }

    @Test
    public void testDrainNodesToRemoveFailureLeavesClusterUnchanged() throws Exception {
        Mockito.when(kubernetesCluster.getId()).thenReturn(10L);
        worker.publicIpAddress = "10.0.0.1";
        worker.sshPort = 2222;
        worker.scaleTimeoutTime = Long.MAX_VALUE;
        KubernetesClusterScaleWorker spy = Mockito.spy(worker);
        UserVmVO node1 = mockNodeVm("node-1");
        UserVmVO node2 = mockNodeVm("node-2");
        Mockito.doReturn(new Pair<>(true, "")).when(spy).executeSshCommand(Mockito.eq("10.0.0.1"), Mockito.eq(2222),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(new Pair<>(true, "drained")).when(spy).drainKubernetesClusterNode(Mockito.eq("10.0.0.1"), Mockito.eq(2222),
                Mockito.eq(node1), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(new Pair<>(false, "Cannot evict pod")).when(spy).drainKubernetesClusterNode(Mockito.eq("10.0.0.1"), Mockito.eq(2222),
                Mockito.eq(node2), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(true).when(spy).uncordonKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
        Mockito.doReturn(true).when(spy).stateTransitTo(Mockito.anyLong(), Mockito.any());

        try {
            spy.drainNodesToRemove(Arrays.asList(node1, node2));
            Assert.fail("Expected the scale-down to stop");
        } catch (CloudRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("node-2"));
            Assert.assertTrue(e.getMessage().contains("Cannot evict pod"));
        }
        Mockito.verify(spy).uncordonKubernetesClusterNode("10.0.0.1", 2222, node1);
        Mockito.verify(spy).uncordonKubernetesClusterNode("10.0.0.1", 2222, node2);
        Mockito.verify(spy).stateTransitTo(10L, KubernetesCluster.Event.OperationSucceeded);
        Mockito.verify(spy, Mockito.never()).stateTransitTo(10L, KubernetesCluster.Event.OperationFailed);
    }

    @Test
    public void testDrainNodesToRemoveCordonsAllBeforeDraining() throws Exception {
        worker.publicIpAddress = "10.0.0.1";
        worker.sshPort = 2222;
        worker.scaleTimeoutTime = Long.MAX_VALUE;
        KubernetesClusterScaleWorker spy = Mockito.spy(worker);
        UserVmVO node1 = mockNodeVm("node-1");
        UserVmVO node2 = mockNodeVm("node-2");
        Mockito.doReturn(new Pair<>(true, "")).when(spy).executeSshCommand(Mockito.anyString(), Mockito.anyInt(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(new Pair<>(true, "drained")).when(spy).drainKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(),
                Mockito.any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());

        spy.drainNodesToRemove(Arrays.asList(node1, node2));

        org.mockito.InOrder inOrder = Mockito.inOrder(spy);
        inOrder.verify(spy).executeSshCommand("10.0.0.1", 2222, "sudo /opt/bin/kubectl cordon node-1", 10000, 10000, 30000);
        inOrder.verify(spy).executeSshCommand("10.0.0.1", 2222, "sudo /opt/bin/kubectl cordon node-2", 10000, 10000, 30000);
        inOrder.verify(spy).drainKubernetesClusterNode(Mockito.eq("10.0.0.1"), Mockito.eq(2222), Mockito.eq(node1), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());
        inOrder.verify(spy).drainKubernetesClusterNode(Mockito.eq("10.0.0.1"), Mockito.eq(2222), Mockito.eq(node2), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.verify(spy, Mockito.never()).uncordonKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
        Mockito.verify(spy, Mockito.never()).stateTransitTo(Mockito.anyLong(), Mockito.any());
    }

    @Test
    public void testDrainNodesToRemoveFailureKeepsAlertCluster() throws Exception {
        Mockito.when(kubernetesCluster.getId()).thenReturn(10L);
        Mockito.when(kubernetesCluster.getState()).thenReturn(KubernetesCluster.State.Alert);
        KubernetesClusterScaleWorker alertWorker = new KubernetesClusterScaleWorker(kubernetesCluster, null, 1L, null, null, null, null, clusterManager);
        alertWorker.publicIpAddress = "10.0.0.1";
        alertWorker.sshPort = 2222;
        KubernetesClusterScaleWorker spy = Mockito.spy(alertWorker);
        UserVmVO node1 = mockNodeVm("node-1");
        Mockito.doReturn(new Pair<>(false, "connection refused")).when(spy).executeSshCommand(Mockito.anyString(), Mockito.anyInt(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(true).when(spy).stateTransitTo(Mockito.anyLong(), Mockito.any());

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> spy.drainNodesToRemove(List.of(node1)));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("unable to cordon Kubernetes node: node-1"));
        Mockito.verify(spy).stateTransitTo(10L, KubernetesCluster.Event.OperationFailed);
        Mockito.verify(spy, Mockito.never()).uncordonKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void testDrainNodesToRemoveStopsAtDeadline() throws Exception {
        Mockito.when(kubernetesCluster.getId()).thenReturn(10L);
        worker.publicIpAddress = "10.0.0.1";
        worker.sshPort = 2222;
        worker.scaleTimeoutTime = System.currentTimeMillis() - 1;
        KubernetesClusterScaleWorker spy = Mockito.spy(worker);
        UserVmVO node1 = mockNodeVm("node-1");
        Mockito.doReturn(new Pair<>(true, "")).when(spy).executeSshCommand(Mockito.anyString(), Mockito.anyInt(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(true).when(spy).uncordonKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
        Mockito.doReturn(true).when(spy).stateTransitTo(Mockito.anyLong(), Mockito.any());

        CloudRuntimeException e = Assert.assertThrows(CloudRuntimeException.class, () -> spy.drainNodesToRemove(List.of(node1)));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
        Mockito.verify(spy, Mockito.never()).drainKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.verify(spy).uncordonKubernetesClusterNode("10.0.0.1", 2222, node1);
        Mockito.verify(spy).stateTransitTo(10L, KubernetesCluster.Event.OperationSucceeded);
    }

    @Test
    public void testDrainNodesToRemoveKeepsNodeCordonedBeforehand() throws Exception {
        Mockito.when(kubernetesCluster.getId()).thenReturn(10L);
        worker.publicIpAddress = "10.0.0.1";
        worker.sshPort = 2222;
        worker.scaleTimeoutTime = Long.MAX_VALUE;
        KubernetesClusterScaleWorker spy = Mockito.spy(worker);
        UserVmVO node1 = mockNodeVm("node-1");
        UserVmVO node2 = mockNodeVm("node-2");
        Mockito.doReturn(new Pair<>(true, "")).when(spy).executeSshCommand(Mockito.anyString(), Mockito.anyInt(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(new Pair<>(true, "sudo: unable to resolve host node-1\ntrue")).when(spy).executeSshCommand(Mockito.anyString(), Mockito.anyInt(),
                Mockito.eq("sudo /opt/bin/kubectl get node node-1 -o jsonpath='{.spec.unschedulable}'"), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(new Pair<>(false, "Cannot evict pod")).when(spy).drainKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(),
                Mockito.any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(true).when(spy).uncordonKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
        Mockito.doReturn(true).when(spy).stateTransitTo(Mockito.anyLong(), Mockito.any());

        Assert.assertThrows(CloudRuntimeException.class, () -> spy.drainNodesToRemove(Arrays.asList(node1, node2)));
        Mockito.verify(spy, Mockito.never()).uncordonKubernetesClusterNode("10.0.0.1", 2222, node1);
        Mockito.verify(spy).uncordonKubernetesClusterNode("10.0.0.1", 2222, node2);
    }

    @Test
    public void testDrainNodesToRemoveTakesUnknownCordonAsCordoned() throws Exception {
        Mockito.when(kubernetesCluster.getId()).thenReturn(10L);
        worker.publicIpAddress = "10.0.0.1";
        worker.sshPort = 2222;
        worker.scaleTimeoutTime = Long.MAX_VALUE;
        KubernetesClusterScaleWorker spy = Mockito.spy(worker);
        UserVmVO node1 = mockNodeVm("node-1");
        Mockito.doReturn(new Pair<>(true, "")).when(spy).executeSshCommand(Mockito.anyString(), Mockito.anyInt(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doThrow(new RuntimeException("timed out")).when(spy).executeSshCommand(Mockito.anyString(), Mockito.anyInt(),
                Mockito.startsWith("sudo /opt/bin/kubectl get node"), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(new Pair<>(false, "Cannot evict pod")).when(spy).drainKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(),
                Mockito.any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());
        Mockito.doReturn(true).when(spy).stateTransitTo(Mockito.anyLong(), Mockito.any());

        Assert.assertThrows(CloudRuntimeException.class, () -> spy.drainNodesToRemove(List.of(node1)));
        Mockito.verify(spy).executeSshCommand("10.0.0.1", 2222, "sudo /opt/bin/kubectl cordon node-1", 10000, 10000, 30000);
        Mockito.verify(spy, Mockito.never()).uncordonKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
    }

    @Test
    public void testDrainNodesToRemoveSkipsDownVm() throws Exception {
        worker.publicIpAddress = "10.0.0.1";
        worker.sshPort = 2222;
        worker.scaleTimeoutTime = Long.MAX_VALUE;
        KubernetesClusterScaleWorker spy = Mockito.spy(worker);
        UserVmVO down = mockNodeVm("node-1");
        Mockito.when(down.getState()).thenReturn(VirtualMachine.State.Stopped);
        UserVmVO up = mockNodeVm("node-2");
        Mockito.when(up.getState()).thenReturn(VirtualMachine.State.Running);
        Mockito.doReturn(new Pair<>(true, "")).when(spy).executeSshCommand(Mockito.anyString(), Mockito.anyInt(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.doReturn(new Pair<>(true, "drained")).when(spy).drainKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(),
                Mockito.any(), Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());

        Assert.assertEquals(List.of(up), spy.drainNodesToRemove(Arrays.asList(down, up)));
        Mockito.verify(spy, Mockito.never()).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.contains("node-1"),
                Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
        Mockito.verify(spy, Mockito.never()).drainKubernetesClusterNode(Mockito.anyString(), Mockito.anyInt(), Mockito.eq(down),
                Mockito.anyInt(), Mockito.anyLong(), Mockito.anyLong());
    }
}
