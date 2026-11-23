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
package org.apache.cloudstack.api.command.user.kubernetes.cluster;

import java.util.List;

import javax.inject.Inject;

import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiCommandResourceType;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.BaseAsyncCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.response.KubernetesClusterResponse;
import org.apache.cloudstack.context.CallContext;

import com.cloud.exception.InvalidParameterValueException;
import com.cloud.kubernetes.cluster.KubernetesCluster;
import com.cloud.kubernetes.cluster.KubernetesClusterEventTypes;
import com.cloud.kubernetes.cluster.KubernetesClusterService;
import com.cloud.utils.exception.CloudRuntimeException;

@APICommand(name = "applyKubernetesClusterMigrations",
        description = "Applies pending migrations to a CloudManaged Kubernetes cluster. The pending database migrations are applied first, " +
                "whatever the state of the cluster, unless an operation is in progress on it. The cluster must be Running or Alert for the others: " +
                "the cluster ones, then the node ones in a single rollout that restarts each node that needs " +
                "one once, as updateKubernetesCluster does. A rollout that fails is resumed by calling this API again",
        responseObject = KubernetesClusterResponse.class,
        responseView = ResponseObject.ResponseView.Restricted,
        entityType = {KubernetesCluster.class},
        requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = true,
        since = "24.0.0",
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User})
public class ApplyKubernetesClusterMigrationsCmd extends BaseAsyncCmd {

    @Inject
    public KubernetesClusterService kubernetesClusterService;

    @Parameter(name = ApiConstants.ID, type = CommandType.UUID, required = true,
            entityType = KubernetesClusterResponse.class,
            description = "The ID of the Kubernetes cluster")
    private Long id;

    @Parameter(name = ApiConstants.MIGRATIONS, type = CommandType.LIST, collectionType = CommandType.STRING,
            description = "The IDs of the migrations to apply, as listKubernetesClusterMigrations lists them. All the pending ones when not given")
    private List<String> migrations;

    @Parameter(name = ApiConstants.MAX_UNAVAILABLE, type = CommandType.INTEGER,
            description = "The number of worker nodes restarted at a time, 1 by default, as for updateKubernetesCluster")
    private Integer maxUnavailable;

    public Long getId() {
        return id;
    }

    public List<String> getMigrations() {
        return migrations;
    }

    public Integer getMaxUnavailable() {
        return maxUnavailable;
    }

    @Override
    public String getEventType() {
        return KubernetesClusterEventTypes.EVENT_KUBERNETES_CLUSTER_MIGRATE;
    }

    @Override
    public String getEventDescription() {
        KubernetesCluster cluster = _entityMgr.findById(KubernetesCluster.class, getId());
        return String.format("Applying migrations to Kubernetes cluster ID: %s", cluster != null ? cluster.getUuid() : getId());
    }

    @Override
    public long getEntityOwnerId() {
        return CallContext.current().getCallingAccount().getId();
    }

    @Override
    public ApiCommandResourceType getApiResourceType() {
        return ApiCommandResourceType.KubernetesCluster;
    }

    @Override
    public Long getApiResourceId() {
        return getId();
    }

    @Override
    public void execute() throws ServerApiException {
        try {
            kubernetesClusterService.applyKubernetesClusterMigrations(this);
            final KubernetesClusterResponse response = kubernetesClusterService.createKubernetesClusterResponse(getId());
            response.setResponseName(getCommandName());
            setResponseObject(response);
        } catch (InvalidParameterValueException ex) {
            throw new ServerApiException(ApiErrorCode.PARAM_ERROR, ex.getMessage());
        } catch (CloudRuntimeException ex) {
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, ex.getMessage());
        }
    }
}
