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
package com.cloud.capacity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.cloud.service.dao.ServiceOfferingDetailsDao;
import com.cloud.utils.Pair;
import com.cloud.vm.VmDetailConstants;

/**
 * Capacity is counted in cluster-overcommitted units, so a VM whose offering sets its own
 * overcommit ratio has to be scaled before it is charged.
 */
public class CapacityManagerScaleToClusterUnitsTest {

    private final CapacityManagerImpl capacityManager = new CapacityManagerImpl();

    private ServiceOfferingDetailsDao serviceOfferingDetailsDao;

    @Before
    public void setUp() {
        serviceOfferingDetailsDao = Mockito.mock(ServiceOfferingDetailsDao.class);
        ReflectionTestUtils.setField(capacityManager, "_serviceOfferingDetailsDao", serviceOfferingDetailsDao);
    }

    private long scale(long requested, Float offeringRatio, float clusterRatio) {
        return capacityManager.scaleToClusterUnits(requested, offeringRatio, clusterRatio);
    }

    @Test
    public void testVmInheritingTheClusterRatioIsChargedAsRequested() {
        assertEquals(4000L, scale(4000L, null, 10.0f));
        assertEquals(4000L, scale(4000L, null, 1.0f));
    }

    @Test
    public void testOfferingMatchingItsClusterIsChargedAsRequested() {
        assertEquals(4000L, scale(4000L, 10.0f, 10.0f));
    }

    @Test
    public void testUnovercommittedVmInAnOvercommittedClusterIsChargedItsFullShare() {
        // ratio 1 in a cluster overcommitted 10 times: the VM holds its whole request, so it must
        // occupy ten times the units of an equally sized neighbour running at the cluster ratio
        assertEquals(40000L, scale(4000L, 1.0f, 10.0f));
    }

    @Test
    public void testVmMoreOvercommittedThanItsClusterIsChargedLess() {
        assertEquals(2000L, scale(4000L, 20.0f, 10.0f));
    }

    @Test
    public void testChargeFollowsTheClusterOfTheHost() {
        // the same VM is charged by the cluster of the host it is on, so releasing the host it
        // left after a migration undoes exactly what was charged there
        assertEquals(8000L, scale(4000L, 1.0f, 2.0f));
        assertEquals(16000L, scale(4000L, 1.0f, 4.0f));
    }

    private void offeringDetails(Map<String, String> details) {
        when(serviceOfferingDetailsDao.listDetailsKeyPairs(eq(10L), anyList())).thenReturn(details);
    }

    private void offeringDetails(String cpu, String memory) {
        Map<String, String> details = new HashMap<>();
        if (cpu != null) {
            details.put(VmDetailConstants.CPU_OVER_COMMIT_RATIO, cpu);
        }
        if (memory != null) {
            details.put(VmDetailConstants.MEMORY_OVER_COMMIT_RATIO, memory);
        }
        offeringDetails(details);
    }

    @Test
    public void testGetOfferingOvercommitRatiosIgnoresUnusableValues() {
        for (String value : List.of("NaN", "Infinity", "0.5", "0", "-1", "none")) {
            offeringDetails(value, value);
            Pair<Float, Float> ratios = capacityManager.getOfferingOvercommitRatios(10L);
            assertNull(value, ratios.first());
            assertNull(value, ratios.second());
        }
    }

    @Test
    public void testGetOfferingOvercommitRatiosReadsBoth() {
        offeringDetails("1", null);
        Pair<Float, Float> ratios = capacityManager.getOfferingOvercommitRatios(10L);
        assertEquals(1.0f, ratios.first(), 1e-6);
        assertNull(ratios.second());
    }

    @Test
    public void testGetOfferingOvercommitRatiosReadsKeysStoredInAnotherCase() {
        // offerings created before the keys were stored under their canonical name
        Map<String, String> details = new HashMap<>();
        details.put("CPUOVERCOMMITRATIO", "1");
        details.put("MemoryOvercommitRatio", "2");
        offeringDetails(details);
        Pair<Float, Float> ratios = capacityManager.getOfferingOvercommitRatios(10L);
        assertEquals(1.0f, ratios.first(), 1e-6);
        assertEquals(2.0f, ratios.second(), 1e-6);
    }

    @Test
    public void testGetOfferingOvercommitRatiosWithoutDetails() {
        offeringDetails(null);
        Pair<Float, Float> ratios = capacityManager.getOfferingOvercommitRatios(10L);
        assertNull(ratios.first());
        assertNull(ratios.second());
    }
}
