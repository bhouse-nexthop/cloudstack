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

import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterVO;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;
import com.cloud.vm.UserVmVO;

/**
 * A change that brings a Kubernetes cluster created by an older version of CloudStack to what the current one sets
 * up. Each cluster records the migrations applied to it. Database migrations are applied on their own when the
 * management server starts; the others are listed as pending and applied on request, with the node ones in a single
 * rolling pass, so that each node is drained and restarted at most once.
 */
public abstract class KubernetesClusterMigration {

    public enum Scope {
        /** Changes only what CloudStack stores about the cluster */
        DATABASE,
        /** Runs kubectl on the cluster */
        CLUSTER,
        /**
         * Changes each node: its VM, which the rolling pass stops and starts again, or, for one that
         * {@link #appliesToRunningNode}, the running node, which is only cordoned
         */
        NODE
    }

    protected final KubernetesClusterManagerImpl manager;

    protected KubernetesClusterMigration(KubernetesClusterManagerImpl manager) {
        this.manager = manager;
    }

    /** What the clusters record; never changes once released */
    public abstract String getId();

    public abstract String getDescription();

    public abstract Scope getScope();

    /** Whether applying it interrupts something that runs on the cluster */
    public boolean isDisruptive() {
        return false;
    }

    /** What keeps the migration from being applied to the cluster, from what CloudStack stores, or null if nothing does */
    public String checkPrecondition(KubernetesCluster cluster) {
        return null;
    }

    /** DATABASE scope: applies the migration to the cluster, locked by the caller, which may have it in part already */
    public void applyToDatabase(KubernetesClusterVO cluster) {
    }

    /**
     * CLUSTER scope: applies the migration through the worker of the rolling pass, before it restarts any node.
     * NODE scope: readies the cluster for it, before any node is drained.
     */
    public void applyToCluster(KubernetesClusterRollingUpdateWorker worker) {
    }

    /** NODE scope: whether a node is still to be migrated */
    public boolean isPendingOnNode(KubernetesCluster cluster, UserVmVO vm) {
        return false;
    }

    /** NODE scope: migrates the stopped VM of a node, before the rolling pass starts it again */
    public void applyToStoppedNode(KubernetesCluster cluster, UserVmVO vm) {
    }

    /** NODE scope: whether it migrates a node with {@link #applyToRunningNode}, so that the rolling pass does not restart the VM for it */
    public boolean appliesToRunningNode() {
        return false;
    }

    /**
     * NODE scope: migrates a node that is running and cordoned: as it is left once its VM is back, if another migration
     * restarts it, or else neither drained nor restarted
     */
    public void applyToRunningNode(KubernetesClusterRollingUpdateWorker worker, UserVmVO vm) {
    }

    /** Whether the cluster is migrated. The worker is that of the rolling pass, or null outside of one, for a DATABASE or NODE migration */
    public abstract boolean verify(KubernetesCluster cluster, KubernetesClusterRollingUpdateWorker worker);
}
