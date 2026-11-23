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

import java.util.List;

import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;

/** Gives a cluster created with a single service offering (gen 1) settings of its own for each node type (gen 2) */
public class NodeTypeSettingsMigration extends KubernetesClusterMigration {

    public static final String ID = "node-type-settings";

    public NodeTypeSettingsMigration(KubernetesClusterManagerImpl manager) {
        super(manager);
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public String getDescription() {
        return "Copies the service offering of the cluster to each node type, and records the affinity groups the nodes of a type all have " +
                "as those of the type, so that the settings of each node type can be changed on their own";
    }

    @Override
    public Scope getScope() {
        return Scope.DATABASE;
    }

    @Override
    public String checkPrecondition(KubernetesCluster cluster) {
        return cluster.getServiceOfferingId() == null && !verify(cluster, null) ? "the cluster has no service offering to copy to its node types" : null;
    }

    @Override
    public void applyToDatabase(KubernetesClusterVO cluster) {
        manager.convertToNodeTypeSettings(cluster);
    }

    @Override
    public boolean verify(KubernetesCluster cluster, KubernetesClusterRollingUpdateWorker worker) {
        boolean hasEtcdNodes = cluster.getEtcdNodeCount() != null && cluster.getEtcdNodeCount() > 0;
        List<KubernetesClusterNodeType> types = hasEtcdNodes ? List.of(CONTROL, WORKER, ETCD) : List.of(CONTROL, WORKER);
        return types.stream().allMatch(type -> manager.getNodeTypeServiceOfferingIdIfSet(cluster, type) != null);
    }
}
