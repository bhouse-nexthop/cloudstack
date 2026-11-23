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
package com.cloud.kubernetes.cluster.dao;

import java.util.List;

import org.springframework.stereotype.Component;

import com.cloud.kubernetes.cluster.KubernetesClusterMigrationVO;
import com.cloud.utils.db.GenericDaoBase;
import com.cloud.utils.db.SearchBuilder;
import com.cloud.utils.db.SearchCriteria;

@Component
public class KubernetesClusterMigrationDaoImpl extends GenericDaoBase<KubernetesClusterMigrationVO, Long> implements KubernetesClusterMigrationDao {

    private final SearchBuilder<KubernetesClusterMigrationVO> clusterIdSearch;
    private final SearchBuilder<KubernetesClusterMigrationVO> clusterIdAndMigrationIdSearch;

    public KubernetesClusterMigrationDaoImpl() {
        clusterIdSearch = createSearchBuilder();
        clusterIdSearch.and("clusterId", clusterIdSearch.entity().getClusterId(), SearchCriteria.Op.EQ);
        clusterIdSearch.done();

        clusterIdAndMigrationIdSearch = createSearchBuilder();
        clusterIdAndMigrationIdSearch.and("clusterId", clusterIdAndMigrationIdSearch.entity().getClusterId(), SearchCriteria.Op.EQ);
        clusterIdAndMigrationIdSearch.and("migrationId", clusterIdAndMigrationIdSearch.entity().getMigrationId(), SearchCriteria.Op.EQ);
        clusterIdAndMigrationIdSearch.done();
    }

    @Override
    public List<KubernetesClusterMigrationVO> listByClusterId(long clusterId) {
        SearchCriteria<KubernetesClusterMigrationVO> sc = clusterIdSearch.create();
        sc.setParameters("clusterId", clusterId);
        return listBy(sc);
    }

    @Override
    public KubernetesClusterMigrationVO findByClusterIdAndMigrationId(long clusterId, String migrationId) {
        SearchCriteria<KubernetesClusterMigrationVO> sc = clusterIdAndMigrationIdSearch.create();
        sc.setParameters("clusterId", clusterId);
        sc.setParameters("migrationId", migrationId);
        return findOneBy(sc);
    }
}
