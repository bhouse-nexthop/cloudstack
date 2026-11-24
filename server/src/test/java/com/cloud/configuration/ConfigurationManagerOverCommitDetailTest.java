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
package com.cloud.configuration;

import java.util.Collections;
import java.util.Map;

import org.apache.cloudstack.context.CallContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.cloud.exception.InvalidParameterValueException;
import com.cloud.user.Account;
import com.cloud.user.AccountManager;
import com.cloud.vm.VmDetailConstants;

public class ConfigurationManagerOverCommitDetailTest {

    private final ConfigurationManagerImpl configurationManager = new ConfigurationManagerImpl();

    private void validate(String value) {
        configurationManager.validateOverCommitRatioInServiceOfferingDetail(
                VmDetailConstants.CPU_OVER_COMMIT_RATIO, value);
    }

    @Test
    public void testRatiosAreAccepted() {
        validate("1");
        validate("1.0");
        validate("4");
        validate("10.5");
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testUndercommitIsRejected() {
        validate("0.1");
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testJustBelowOneIsRejected() {
        validate("0.99");
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testZeroIsRejected() {
        validate("0");
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testNegativeIsRejected() {
        validate("-1");
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testNonNumericIsRejected() {
        validate("none");
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testNaNIsRejected() {
        validate("NaN");
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testInfinityIsRejected() {
        validate("Infinity");
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testNullIsRejected() {
        validate(null);
    }

    @Test
    public void testDetailNameIsCanonicalisedInAnyCase() {
        Assert.assertEquals(VmDetailConstants.CPU_OVER_COMMIT_RATIO, ConfigurationManagerImpl.overCommitRatioDetailName("CPUOVERCOMMITRATIO"));
        Assert.assertEquals(VmDetailConstants.MEMORY_OVER_COMMIT_RATIO, ConfigurationManagerImpl.overCommitRatioDetailName("MemoryOvercommitRatio"));
        Assert.assertNull(ConfigurationManagerImpl.overCommitRatioDetailName("rootdisksize"));
    }

    private void checkOverCommitRatiosAs(Account.Type type, Map<String, String> requested, Map<String, String> source) {
        Account caller = Mockito.mock(Account.class);
        Mockito.when(caller.getId()).thenReturn(42L);
        AccountManager accountManager = Mockito.mock(AccountManager.class);
        Mockito.when(accountManager.isRootAdmin(42L)).thenReturn(type == Account.Type.ADMIN);
        ReflectionTestUtils.setField(configurationManager, "_accountMgr", accountManager);
        CallContext callContext = Mockito.mock(CallContext.class);
        Mockito.when(callContext.getCallingAccount()).thenReturn(caller);
        try (MockedStatic<CallContext> callContextMock = Mockito.mockStatic(CallContext.class)) {
            callContextMock.when(CallContext::current).thenReturn(callContext);
            configurationManager.checkCallerMaySetOverCommitRatios(requested, source);
        }
    }

    @Test
    public void testRootAdminMaySetTheRatios() {
        checkOverCommitRatiosAs(Account.Type.ADMIN, Map.of("cpuOvercommitRatio", "1"), Collections.emptyMap());
    }

    @Test(expected = InvalidParameterValueException.class)
    public void testDomainAdminMayNotSetTheRatios() {
        checkOverCommitRatiosAs(Account.Type.DOMAIN_ADMIN, Map.of("MemoryOvercommitRatio", "1"), Collections.emptyMap());
    }

    @Test
    public void testDomainAdminMaySetOtherDetails() {
        checkOverCommitRatiosAs(Account.Type.DOMAIN_ADMIN, Map.of("rootdisksize", "10"), Collections.emptyMap());
    }
}
