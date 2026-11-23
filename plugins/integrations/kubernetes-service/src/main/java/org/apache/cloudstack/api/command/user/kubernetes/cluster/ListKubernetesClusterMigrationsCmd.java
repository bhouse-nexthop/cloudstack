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

import javax.inject.Inject;

import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;
import org.apache.cloudstack.api.response.KubernetesClusterMigrationResponse;
import org.apache.cloudstack.api.response.KubernetesClusterResponse;
import org.apache.cloudstack.api.response.ListResponse;
import org.apache.cloudstack.context.CallContext;

import com.cloud.kubernetes.cluster.KubernetesClusterService;

@APICommand(name = "listKubernetesClusterMigrations",
        description = "Lists the migrations of a CloudManaged Kubernetes cluster, which bring a cluster created by an older version of CloudStack " +
                "to what the current one sets up, with whether each is applied or pending",
        responseObject = KubernetesClusterMigrationResponse.class,
        responseView = ResponseObject.ResponseView.Restricted,
        requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = false,
        since = "24.0.0",
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User})
public class ListKubernetesClusterMigrationsCmd extends BaseCmd {

    @Inject
    public KubernetesClusterService kubernetesClusterService;

    @Parameter(name = ApiConstants.ID, type = CommandType.UUID, required = true,
            entityType = KubernetesClusterResponse.class,
            description = "The ID of the Kubernetes cluster")
    private Long id;

    public Long getId() {
        return id;
    }

    @Override
    public long getEntityOwnerId() {
        return CallContext.current().getCallingAccount().getId();
    }

    @Override
    public void execute() {
        ListResponse<KubernetesClusterMigrationResponse> response = kubernetesClusterService.listKubernetesClusterMigrations(this);
        response.setResponseName(getCommandName());
        setResponseObject(response);
    }
}
