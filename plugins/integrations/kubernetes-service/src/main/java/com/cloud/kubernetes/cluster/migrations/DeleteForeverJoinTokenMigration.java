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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;

import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;
import com.cloud.utils.exception.CloudRuntimeException;

/**
 * Deletes the bootstrap token that clusters created by older versions of CloudStack were given: derived from the
 * cluster UUID, which the API shows, and never expiring. Nodes now join with a token created for them.
 */
public class DeleteForeverJoinTokenMigration extends KubernetesClusterMigration {

    public static final String ID = "delete-forever-join-token";
    protected static final String KUBECTL = KubernetesClusterRollingUpdateWorker.KUBECTL;
    protected static final String TOKEN_SECRET_COMMAND = KUBECTL + "-n kube-system get secret bootstrap-token-%s --ignore-not-found -o jsonpath=secret={.data.token-secret}.";
    protected static final String DELETE_TOKEN_COMMAND = KUBECTL + "-n kube-system delete secret bootstrap-token-%s --ignore-not-found";
    private static final Pattern TOKEN_SECRET = Pattern.compile("secret=([A-Za-z0-9+/=]*)\\.");

    public DeleteForeverJoinTokenMigration(KubernetesClusterManagerImpl manager) {
        super(manager);
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public String getDescription() {
        return "Deletes the never expiring bootstrap token, derived from the cluster UUID, that clusters created by older versions of CloudStack " +
                "were given. Nodes added since join with a token created for them";
    }

    @Override
    public Scope getScope() {
        return Scope.CLUSTER;
    }

    /** The token older versions of CloudStack gave the cluster: its ID and its secret */
    protected static String[] getUuidToken(KubernetesCluster cluster) {
        String token = cluster.getUuid().replace("-", "").substring(0, 22);
        return new String[] {token.substring(0, 6), token.substring(6)};
    }

    /** Whether the cluster has the token, with both its ID and its secret, so that a token of the same ID made by someone else is left alone */
    protected boolean hasUuidToken(KubernetesClusterRollingUpdateWorker worker) {
        String[] token = getUuidToken(worker.getKubernetesCluster());
        String output = worker.runKubectl(String.format(TOKEN_SECRET_COMMAND, token[0]));
        if (output == null) {
            throw new CloudRuntimeException("unable to run kubectl on the cluster to tell whether it has the bootstrap token derived from its UUID");
        }
        Matcher matcher = TOKEN_SECRET.matcher(output);
        return matcher.find() && token[1].equals(new String(Base64.decodeBase64(matcher.group(1)), StandardCharsets.UTF_8));
    }

    @Override
    public void applyToCluster(KubernetesClusterRollingUpdateWorker worker) {
        if (!hasUuidToken(worker)) {
            return;
        }
        // a node that has not joined yet may still be trying to with the token
        List<String> unregistered = worker.getUnregisteredNodes();
        if (!unregistered.isEmpty()) {
            throw new CloudRuntimeException(String.format("nodes %s have not registered with the cluster, and may still need the bootstrap token derived from the cluster UUID to join it",
                    unregistered));
        }
        if (worker.runKubectl(String.format(DELETE_TOKEN_COMMAND, getUuidToken(worker.getKubernetesCluster())[0])) == null) {
            throw new CloudRuntimeException("unable to delete the bootstrap token derived from the cluster UUID");
        }
    }

    @Override
    public boolean verify(KubernetesCluster cluster, KubernetesClusterRollingUpdateWorker worker) {
        if (worker == null) {
            return false;
        }
        try {
            return !hasUuidToken(worker);
        } catch (CloudRuntimeException e) {
            return false;
        }
    }
}
