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
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.kubernetes.version.KubernetesSupportedVersion;
import com.cloud.kubernetes.cluster.utils.KubernetesClusterUtil;
import com.cloud.uservm.UserVm;
import com.cloud.utils.Pair;
import com.cloud.utils.ssh.SshHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClusterUpgradeWorkerTest {

    @Mock
    private KubernetesCluster kubernetesCluster;
    @Mock
    private KubernetesSupportedVersion kubernetesSupportedVersion;
    @Mock
    private KubernetesClusterManagerImpl clusterManager;
    @Mock
    private KubernetesClusterVmMapDao kubernetesClusterVmMapDao;

    private KubernetesClusterUpgradeWorker worker;

    @Before
    public void setUp() {
        String[] keys = {};
        worker = new KubernetesClusterUpgradeWorker(kubernetesCluster, kubernetesSupportedVersion, clusterManager, keys);
        worker.kubernetesClusterVmMapDao = kubernetesClusterVmMapDao;
    }

    @Test
    public void testFilterOutManualUpgradeNodesFromClusterUpgrade() {
        long controlNodeId = 1L;
        long workerNode1Id = 2L;
        long workerNode2Id = 3L;
        UserVm controlNode = Mockito.mock(UserVm.class);
        Mockito.when(controlNode.getId()).thenReturn(controlNodeId);
        UserVm workerNode1 = Mockito.mock(UserVm.class);
        Mockito.when(workerNode1.getId()).thenReturn(workerNode1Id);
        UserVm workerNode2 = Mockito.mock(UserVm.class);
        Mockito.when(workerNode2.getId()).thenReturn(workerNode2Id);
        KubernetesClusterVmMapVO controlNodeMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        KubernetesClusterVmMapVO workerNode1Map = Mockito.mock(KubernetesClusterVmMapVO.class);
        KubernetesClusterVmMapVO workerNode2Map = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(workerNode2Map.isManualUpgrade()).thenReturn(true);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(controlNodeId)).thenReturn(controlNodeMap);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(workerNode1Id)).thenReturn(workerNode1Map);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(workerNode2Id)).thenReturn(workerNode2Map);
        worker.clusterVMs = Arrays.asList(controlNode, workerNode1, workerNode2);
        worker.filterOutManualUpgradeNodesFromClusterUpgrade();
        Assert.assertEquals(2, worker.clusterVMs.size());
        List<Long> ids = worker.clusterVMs.stream().map(UserVm::getId).collect(Collectors.toList());
        Assert.assertTrue(ids.contains(controlNodeId) && ids.contains(workerNode1Id));
        Assert.assertFalse(ids.contains(workerNode2Id));
    }

    private static final String VERSION_COMMAND = String.format(KubernetesClusterUtil.CLUSTER_NODE_VERSION_COMMAND, "node-1");
    private static final String READY_COMMAND = String.format(KubernetesClusterUpgradeWorker.NODE_STATUS_COMMAND, "node-1");

    private KubernetesClusterUpgradeWorker mockNodeAtVersion(String storedVersion) {
        Mockito.when(kubernetesSupportedVersion.getSemanticVersion()).thenReturn("1.29.2");
        KubernetesClusterVmMapVO vmMap = Mockito.mock(KubernetesClusterVmMapVO.class);
        Mockito.when(vmMap.getNodeVersion()).thenReturn(storedVersion);
        Mockito.when(kubernetesClusterVmMapDao.getClusterMapFromVmId(5L)).thenReturn(vmMap);
        worker.publicIpAddress = "10.0.0.1";
        worker.sshPort = 2222;
        return Mockito.spy(worker);
    }

    private UserVm mockVm() {
        UserVm vm = Mockito.mock(UserVm.class);
        Mockito.when(vm.getId()).thenReturn(5L);
        return vm;
    }

    @Test
    public void testIsNodeUpgradedWhenRecordedAndReported() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.2");
        Mockito.doReturn(new Pair<>(true, "v1.29.2\n")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Mockito.doReturn(new Pair<>(true, "Ready\n")).when(spy).executeSshCommand("10.0.0.1", 2222, READY_COMMAND, 10000, 10000, 20000);
        Assert.assertTrue(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedWhenCordonedAfterUpgrade() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.2");
        Mockito.doReturn(new Pair<>(true, "v1.29.2\n")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Mockito.doReturn(new Pair<>(true, "Ready,SchedulingDisabled\n")).when(spy).executeSshCommand("10.0.0.1", 2222, READY_COMMAND, 10000, 10000, 20000);
        Assert.assertTrue(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedNotWhenNodeNotReady() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.2");
        Mockito.doReturn(new Pair<>(true, "v1.29.2\n")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Mockito.doReturn(new Pair<>(true, "NotReady")).when(spy).executeSshCommand("10.0.0.1", 2222, READY_COMMAND, 10000, 10000, 20000);
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedNotTrustingRecordedVersionAlone() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.2");
        Mockito.doReturn(new Pair<>(true, "v1.28.9")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedWhenNodeUnreachable() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("1.29.2");
        Mockito.doThrow(new RuntimeException("connection refused")).when(spy).executeSshCommand("10.0.0.1", 2222, VERSION_COMMAND, 10000, 10000, 20000);
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    @Test
    public void testIsNodeUpgradedWhenRecordedAtOldVersion() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.28.9");
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
        Mockito.verify(spy, Mockito.never()).executeSshCommand(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt());
    }

    @Test
    public void testIsNodeUpgradedComparesWholeVersion() throws Exception {
        KubernetesClusterUpgradeWorker spy = mockNodeAtVersion("v1.29.20");
        Assert.assertFalse(spy.isNodeUpgraded(mockVm(), "node-1"));
    }

    private MockedStatic<SshHelper> mockAutoscalerSsh(KubernetesClusterUpgradeWorker spyWorker, Pair<Boolean, String> result) {
        Mockito.doReturn("cloud").when(spyWorker).getControlNodeLoginUser();
        MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class);
        sshHelper.when(() -> SshHelper.sshExecute(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyString(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(),
                ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenReturn(result);
        return sshHelper;
    }

    @Test
    public void testUpdateAutoscalerSkippedWhenAutoscalingDisabled() {
        Mockito.when(kubernetesCluster.getAutoscalingEnabled()).thenReturn(false);
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            worker.updateAutoscaler();
            sshHelper.verifyNoInteractions();
        }
    }

    @Test
    public void testUpdateAutoscalerSkippedWithoutMinMaxSize() {
        Mockito.when(kubernetesCluster.getAutoscalingEnabled()).thenReturn(true);
        Mockito.when(kubernetesCluster.getMinSize()).thenReturn(null);
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            worker.updateAutoscaler();
            sshHelper.verifyNoInteractions();
        }
    }

    @Test
    public void testUpdateAutoscalerRedeploysWithClusterSettings() {
        Mockito.when(kubernetesCluster.getAutoscalingEnabled()).thenReturn(true);
        Mockito.when(kubernetesCluster.getUuid()).thenReturn("cluster-uuid");
        Mockito.when(kubernetesCluster.getMinSize()).thenReturn(1L);
        Mockito.when(kubernetesCluster.getMaxSize()).thenReturn(5L);
        KubernetesClusterUpgradeWorker spyWorker = Mockito.spy(worker);
        try (MockedStatic<SshHelper> sshHelper = mockAutoscalerSsh(spyWorker, new Pair<>(true, ""))) {
            spyWorker.updateAutoscaler();
            sshHelper.verify(() -> SshHelper.sshExecute(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.eq("cloud"),
                    ArgumentMatchers.any(), ArgumentMatchers.any(),
                    ArgumentMatchers.eq("sudo /opt/bin/autoscale-kube-cluster -i cluster-uuid -e -M 5 -m 1"),
                    ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()));
        }
    }

    @Test
    public void testUpdateAutoscalerDoesNotThrowOnFailure() {
        Mockito.when(kubernetesCluster.getAutoscalingEnabled()).thenReturn(true);
        Mockito.when(kubernetesCluster.getUuid()).thenReturn("cluster-uuid");
        Mockito.when(kubernetesCluster.getMinSize()).thenReturn(1L);
        Mockito.when(kubernetesCluster.getMaxSize()).thenReturn(5L);
        KubernetesClusterUpgradeWorker spyWorker = Mockito.spy(worker);
        try (MockedStatic<SshHelper> sshHelper = mockAutoscalerSsh(spyWorker, new Pair<>(false, "autoscale-kube-cluster: not found"))) {
            spyWorker.updateAutoscaler();
            sshHelper.when(() -> SshHelper.sshExecute(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyString(),
                    ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(),
                    ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenThrow(new RuntimeException("ssh failed"));
            spyWorker.updateAutoscaler();
            sshHelper.verify(() -> SshHelper.sshExecute(ArgumentMatchers.any(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyString(),
                    ArgumentMatchers.any(), ArgumentMatchers.any(),
                    ArgumentMatchers.eq("sudo /opt/bin/autoscale-kube-cluster -i cluster-uuid -e -M 5 -m 1"),
                    ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()), Mockito.times(2));
        }
    }
}
