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

import static com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType.WORKER;

import java.util.List;

import javax.inject.Inject;

import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterManagerImpl;
import com.cloud.kubernetes.cluster.KubernetesClusterVmMapVO;
import com.cloud.kubernetes.cluster.KubernetesServiceHelper.KubernetesClusterNodeType;
import com.cloud.kubernetes.cluster.actionworkers.KubernetesClusterRollingUpdateWorker;
import com.cloud.kubernetes.cluster.utils.KubernetesClusterUtil;
import com.cloud.service.ServiceOfferingVO;
import com.cloud.service.dao.ServiceOfferingDao;
import com.cloud.storage.VMTemplateVO;
import com.cloud.storage.dao.VMTemplateDao;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.UserVmManager;
import com.cloud.vm.UserVmVO;
import com.cloud.vm.dao.UserVmDao;

/**
 * Makes the node VMs dynamically scalable, so that a change of service offering scales a running node instead of
 * restarting it. On KVM a VM only gets the room to grow when it starts, so each node is restarted.
 */
public class DynamicScalingMigration extends KubernetesClusterMigration {

    public static final String ID = "dynamic-scaling";

    @Inject
    protected UserVmDao userVmDao;
    @Inject
    protected ServiceOfferingDao serviceOfferingDao;
    @Inject
    protected VMTemplateDao templateDao;

    public DynamicScalingMigration(KubernetesClusterManagerImpl manager) {
        super(manager);
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public String getDescription() {
        return "Makes the node VMs dynamically scalable, restarting each node, so that their service offering can be changed while they run. " +
                "Needs " + UserVmManager.EnableDynamicallyScaleVmCK + " in the zone, and dynamic scaling allowed by the service offerings and templates of the nodes";
    }

    @Override
    public Scope getScope() {
        return Scope.NODE;
    }

    @Override
    public boolean isDisruptive() {
        return true;
    }

    protected boolean isDynamicScalingEnabled(long zoneId) {
        return Boolean.TRUE.equals(UserVmManager.EnableDynamicallyScaleVm.valueIn(zoneId));
    }

    @Override
    public String checkPrecondition(KubernetesCluster cluster) {
        if (!isDynamicScalingEnabled(cluster.getZoneId()) && !verify(cluster, null)) {
            return String.format("%s is false in the zone of the cluster", UserVmManager.EnableDynamicallyScaleVmCK);
        }
        List<KubernetesClusterVmMapVO> vmMaps = manager.kubernetesClusterVmMapDao.listByClusterId(cluster.getId());
        boolean etcdOnOwnNodes = cluster.getEtcdNodeCount() != null && cluster.getEtcdNodeCount() > 0;
        for (KubernetesClusterVmMapVO vmMap : vmMaps) {
            UserVmVO vm = vmMap.isExternalNode() ? null : userVmDao.findById(vmMap.getVmId());
            if (vm == null || !isPendingOnNode(cluster, vm)) {
                continue;
            }
            KubernetesClusterNodeType type = KubernetesClusterUtil.getNodeType(vmMap);
            int members = KubernetesClusterUtil.countManagedNodes(vmMaps, type);
            if (!KubernetesClusterRollingUpdateWorker.canRestartOneAtATime(type, members, etcdOnOwnNodes)) {
                return String.format("the %d %s node(s) can not be restarted one at a time, as the cluster can not keep running without one",
                        members, type.name().toLowerCase());
            }
            Long templateId = type == WORKER ? manager.getNodeTypeTemplateId(cluster, type) : null;
            String reason = getUnscalableReason(vm, manager.getNodeTypeServiceOfferingId(cluster, type), templateId == null ? vm.getTemplateId() : templateId);
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }

    protected String getUnscalableReason(UserVmVO vm, Long offeringId, long templateId) {
        ServiceOfferingVO offering = serviceOfferingDao.findByIdIncludingRemoved(offeringId == null ? vm.getServiceOfferingId() : offeringId);
        if (offering == null || !offering.isDynamicScalingEnabled()) {
            return String.format("the service offering %s of node %s does not allow dynamic scaling", offering == null ? offeringId : offering.getName(), vm.getHostName());
        }
        VMTemplateVO template = templateDao.findByIdIncludingRemoved(templateId);
        if (template == null || !template.isDynamicallyScalable()) {
            return String.format("the template %s of node %s is not dynamically scalable", template == null ? templateId : template.getName(), vm.getHostName());
        }
        return null;
    }

    @Override
    public boolean isPendingOnNode(KubernetesCluster cluster, UserVmVO vm) {
        return !vm.isDynamicallyScalable();
    }

    @Override
    public void applyToStoppedNode(KubernetesCluster cluster, UserVmVO vm) {
        String reason = isDynamicScalingEnabled(cluster.getZoneId()) ? getUnscalableReason(vm, vm.getServiceOfferingId(), vm.getTemplateId())
                : String.format("%s is false in the zone of the cluster", UserVmManager.EnableDynamicallyScaleVmCK);
        if (reason != null) {
            throw new CloudRuntimeException(String.format("unable to make node %s dynamically scalable: %s", vm.getHostName(), reason));
        }
        vm.setDynamicallyScalable(true);
        userVmDao.update(vm.getId(), vm);
    }

    @Override
    public boolean verify(KubernetesCluster cluster, KubernetesClusterRollingUpdateWorker worker) {
        for (KubernetesClusterVmMapVO vmMap : manager.kubernetesClusterVmMapDao.listByClusterId(cluster.getId())) {
            UserVmVO vm = vmMap.isExternalNode() ? null : userVmDao.findById(vmMap.getVmId());
            if (vm != null && isPendingOnNode(cluster, vm)) {
                return false;
            }
        }
        return true;
    }
}
