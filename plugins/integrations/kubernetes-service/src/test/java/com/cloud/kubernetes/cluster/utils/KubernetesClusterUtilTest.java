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
package com.cloud.kubernetes.cluster.utils;

import java.io.File;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.dao.KubernetesClusterVmMapDao;
import com.cloud.utils.Pair;
import com.cloud.utils.ssh.SshHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClusterUtilTest {

    private void executeThrowAndTestVersionMatch() {
        Pair<Boolean, String> resultPair = null;
        Pair<Boolean, String> result = KubernetesClusterUtil.clusterNodeVersionMatches(resultPair, "1.24.0");
        Assert.assertFalse(result.first());
    }

    private void executeAndTestVersionMatch(boolean status, String response, boolean expectedResult) {
        Pair<Boolean, String> resultPair = new Pair<>(status, response);
        Pair<Boolean, String> result = KubernetesClusterUtil.clusterNodeVersionMatches(resultPair, "1.24.0");
        Assert.assertEquals(expectedResult, result.first());
    }

    @Test
    public void testClusterNodeVersionMatches() {
        String v1233WorkerNodeOutput = "v1.23.3";
        String v1240WorkerNodeOutput = "v1.24.0";

        executeAndTestVersionMatch(true, v1240WorkerNodeOutput, true);

        executeAndTestVersionMatch(true, v1233WorkerNodeOutput, false);

        executeAndTestVersionMatch(true, "v1.24.0\n", true);

        executeAndTestVersionMatch(true, "v1.24.01", false);

        executeAndTestVersionMatch(true, "sudo: unable to resolve host node-1\nv1.24.0\n", true);

        executeAndTestVersionMatch(true, "v1.24.0+vendor.1", true);

        executeAndTestVersionMatch(false, v1240WorkerNodeOutput, false);

        executeAndTestVersionMatch(false, v1233WorkerNodeOutput, false);

        executeThrowAndTestVersionMatch();
    }

    @Test
    public void testClusterNodeVersionMatchesRecordsVersion() {
        KubernetesClusterVmMapDao vmMapDao = Mockito.mock(KubernetesClusterVmMapDao.class);
        KubernetesClusterVmMapVO vmMapVO = new KubernetesClusterVmMapVO(1L, 2L, false);
        vmMapVO.setNodeVersion("1.24.0");
        Mockito.when(vmMapDao.getClusterMapFromVmId(2L)).thenReturn(vmMapVO);
        try (MockedStatic<SshHelper> sshHelper = Mockito.mockStatic(SshHelper.class)) {
            sshHelper.when(() -> SshHelper.sshExecute(Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.any(),
                    Mockito.any(), Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyInt()))
                    .thenReturn(new Pair<>(true, "v1.25.1\n"));

            Assert.assertTrue(KubernetesClusterUtil.clusterNodeVersionMatches("1.25.1", "10.1.1.1", 2222, "cloud",
                    new File("id_rsa"), "node-1", System.currentTimeMillis() + 60000, 1, 2L, vmMapDao));
        }
        // in the same form as for a new node, not the kubectl output
        Assert.assertEquals("1.25.1", vmMapVO.getNodeVersion());
        Mockito.verify(vmMapDao).update(vmMapVO.getId(), vmMapVO);
    }

    @Test
    public void testGenerateBootstrapToken() {
        String token = KubernetesClusterUtil.generateBootstrapToken();
        Assert.assertTrue(token.matches("[a-z0-9]{6}\\.[a-z0-9]{16}"));
        Assert.assertNotEquals(token, KubernetesClusterUtil.generateBootstrapToken());
    }

    @Test
    public void testGenerateCertificateKey() {
        String key = KubernetesClusterUtil.generateCertificateKey();
        Assert.assertTrue(key.matches("[0-9a-f]{64}"));
        Assert.assertNotEquals(key, KubernetesClusterUtil.generateCertificateKey());
    }

    @Test
    public void testGetCaCertHash() throws Exception {
        byte[] der = Base64.getDecoder().decode("MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAET4KHpN8Rsx35juCq20Wi0WJyBRRF" +
                "Yl0p4YCFF6xxCY02Gelg7EYwHdPA/dbs+HrNwBBb/wuUNnEZiw1OQJXIRg==");
        PublicKey key = KeyFactory.getInstance("EC").generatePublic(new X509EncodedKeySpec(der));
        Assert.assertEquals("sha256:9826a516d7c2f8974dd0a09a64048c5cba6ff67780b01568b13cd3acf92a409f",
                KubernetesClusterUtil.getCaCertHash(key));
    }

    @Test
    public void testParseJoinCommand() {
        String hash = "sha256:9826a516d7c2f8974dd0a09a64048c5cba6ff67780b01568b13cd3acf92a409f";
        Pair<String, String> result = KubernetesClusterUtil.parseJoinCommand(
                "kubeadm join 10.1.1.10:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash " + hash + " \n");
        Assert.assertEquals("abcdef.0123456789abcdef", result.first());
        Assert.assertEquals(hash, result.second());
    }

    @Test
    public void testParseJoinCommandInvalid() {
        Assert.assertNull(KubernetesClusterUtil.parseJoinCommand(null));
        Assert.assertNull(KubernetesClusterUtil.parseJoinCommand("failed to load admin kubeconfig"));
        Assert.assertNull(KubernetesClusterUtil.parseJoinCommand("kubeadm join 10.1.1.10:6443 --token abcdef.0123456789abcdef"));
        Assert.assertNull(KubernetesClusterUtil.parseJoinCommand(
                "kubeadm join 10.1.1.10:6443 --token $(reboot) --discovery-token-ca-cert-hash sha256:9826a516d7c2f8974dd0a09a64048c5cba6ff67780b01568b13cd3acf92a409f"));
        Assert.assertNull(KubernetesClusterUtil.parseJoinCommand(
                "kubeadm join 10.1.1.10:6443 --token abcdef.0123456789abcdef;id --discovery-token-ca-cert-hash sha256:9826a516d7c2f8974dd0a09a64048c5cba6ff67780b01568b13cd3acf92a409f"));
    }

    @Test
    public void testGetJoinTokenTtl() {
        Assert.assertEquals("3600s", KubernetesClusterUtil.getJoinTokenTtl(3600));
        Assert.assertEquals("1s", KubernetesClusterUtil.getJoinTokenTtl(0));
    }
}
