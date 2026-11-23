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
package org.apache.cloudstack.api.response;

import java.util.Date;

import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseResponse;

import com.cloud.serializer.Param;
import com.google.gson.annotations.SerializedName;

public class KubernetesClusterMigrationResponse extends BaseResponse {
    @SerializedName(ApiConstants.ID)
    @Param(description = "The ID of the migration")
    private String id;

    @SerializedName(ApiConstants.DESCRIPTION)
    @Param(description = "What the migration changes")
    private String description;

    @SerializedName(ApiConstants.SCOPE)
    @Param(description = "What the migration changes: DATABASE, what CloudStack stores, which is migrated when the management server starts; " +
            "CLUSTER, what runs on the cluster; NODE, the node VMs, which are restarted")
    private String scope;

    @SerializedName(ApiConstants.DISRUPTIVE)
    @Param(description = "Whether applying the migration interrupts something that runs on the cluster")
    private Boolean disruptive;

    @SerializedName(ApiConstants.STATE)
    @Param(description = "Applied or Pending")
    private String state;

    @SerializedName(ApiConstants.APPLIED)
    @Param(description = "The date the migration was applied to the cluster")
    private Date applied;

    @SerializedName(ApiConstants.UNMET_PRECONDITION)
    @Param(description = "What keeps a pending migration from being applied to the cluster")
    private String unmetPrecondition;

    public KubernetesClusterMigrationResponse() {
        setObjectName("kubernetesclustermigration");
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public void setDisruptive(Boolean disruptive) {
        this.disruptive = disruptive;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setApplied(Date applied) {
        this.applied = applied;
    }

    public void setUnmetPrecondition(String unmetPrecondition) {
        this.unmetPrecondition = unmetPrecondition;
    }

    public String getId() {
        return id;
    }

    public String getState() {
        return state;
    }

    public String getUnmetPrecondition() {
        return unmetPrecondition;
    }
}
