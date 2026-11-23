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

import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;
import com.cloud.utils.exception.CloudRuntimeException;

/**
 * Removes the deprecated Kubernetes dashboard from a cluster that runs Headlamp, which a cluster gets when it is
 * created or upgraded with a binaries ISO that has it
 */
public class HeadlampMigration extends KubernetesClusterMigration {

    public static final String ID = "headlamp";
    protected static final String KUBECTL = KubernetesClusterRollingUpdateWorker.KUBECTL;
    protected static final String HEADLAMP_COMMAND = KUBECTL + "-n kube-system get deployment headlamp --ignore-not-found -o jsonpath=replicas={.status.availableReplicas}.";
    private static final Pattern REPLICAS = Pattern.compile("replicas=(\\d+)\\.");
    protected static final String DASHBOARD_PHASE_COMMAND = KUBECTL + "get namespace kubernetes-dashboard --ignore-not-found -o jsonpath={.status.phase}";
    protected static final String DASHBOARD_RBAC_COMMAND = KUBECTL + "get clusterrolebinding/kubernetes-dashboard clusterrolebinding/kubernetes-dashboard-ui "
            + "clusterrolebinding/kubernetes-dashboard-admin-user clusterrole/kubernetes-dashboard --ignore-not-found -o name";
    /** The namespace, and the cluster wide objects that give its service accounts access: those of the manifest and those CKS and its docs created */
    protected static final String DELETE_DASHBOARD_COMMAND = KUBECTL + "delete namespace kubernetes-dashboard --ignore-not-found --wait=false && "
            + KUBECTL + "delete clusterrolebinding kubernetes-dashboard kubernetes-dashboard-ui kubernetes-dashboard-admin-user --ignore-not-found && "
            + KUBECTL + "delete clusterrole kubernetes-dashboard --ignore-not-found";

    public HeadlampMigration(KubernetesClusterManagerImpl manager) {
        super(manager);
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public String getDescription() {
        return "Deletes the deprecated Kubernetes dashboard, once the cluster runs Headlamp instead, which an upgrade to a Kubernetes version " +
                "whose binaries ISO has Headlamp installs";
    }

    @Override
    public Scope getScope() {
        return Scope.CLUSTER;
    }

    @Override
    public boolean isDisruptive() {
        return true;
    }

    /** Whether Headlamp has a replica available, from the output, in which sudo or kubectl may print anything else */
    protected boolean isHeadlampRunning(KubernetesClusterRollingUpdateWorker worker) {
        String output = worker.runKubectl(HEADLAMP_COMMAND);
        if (output == null) {
            throw new CloudRuntimeException("unable to run kubectl on the cluster to tell whether Headlamp is running");
        }
        Matcher matcher = REPLICAS.matcher(output);
        return matcher.find() && Integer.parseInt(matcher.group(1)) > 0;
    }

    @Override
    public void applyToCluster(KubernetesClusterRollingUpdateWorker worker) {
        if (verify(worker.getKubernetesCluster(), worker)) {
            return;
        }
        if (!isHeadlampRunning(worker)) {
            throw new CloudRuntimeException("Headlamp is not running on the cluster. Upgrade it to a Kubernetes version whose binaries ISO has Headlamp first");
        }
        if (worker.runKubectl(DELETE_DASHBOARD_COMMAND) == null) {
            throw new CloudRuntimeException("unable to delete the Kubernetes dashboard");
        }
    }

    @Override
    public boolean verify(KubernetesCluster cluster, KubernetesClusterRollingUpdateWorker worker) {
        // Headlamp was running when the dashboard was deleted; a rollout of the nodes may have moved it since
        if (worker == null) {
            return false;
        }
        // a namespace that is being deleted, Terminating, is gone once what it holds is
        String phase = worker.runKubectl(DASHBOARD_PHASE_COMMAND);
        String rbac = phase == null || phase.contains("Active") ? null : worker.runKubectl(DASHBOARD_RBAC_COMMAND);
        return rbac != null && !rbac.contains("clusterrole");
    }
}
