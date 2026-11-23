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
package com.cloud.kubernetes.cluster;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.cloudstack.api.InternalIdentity;

/** A migration applied to a Kubernetes cluster */
@Entity
@Table(name = "kubernetes_cluster_migrations")
public class KubernetesClusterMigrationVO implements InternalIdentity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "cluster_id")
    private long clusterId;

    @Column(name = "migration_id")
    private String migrationId;

    @Column(name = "applied")
    @Temporal(value = TemporalType.TIMESTAMP)
    private Date applied;

    public KubernetesClusterMigrationVO() {
    }

    public KubernetesClusterMigrationVO(long clusterId, String migrationId) {
        this.clusterId = clusterId;
        this.migrationId = migrationId;
        this.applied = new Date();
    }

    @Override
    public long getId() {
        return id;
    }

    public long getClusterId() {
        return clusterId;
    }

    public String getMigrationId() {
        return migrationId;
    }

    public Date getApplied() {
        return applied;
    }
}
