package com.oodrive.nuage.utils;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2015 Oodrive
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.junit.Assert;
import org.junit.Test;

/**
 * Test the class {@link IpAddressValidator}.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class TestIpAddressValidator {

    @Test
    public void testIPv4ValidAddresses() {
        Assert.assertTrue(IpAddressValidator.validateIPv4("0.0.0.0"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("255.255.255.255"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("255.255.255.0"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("255.255.0.0"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("255.0.0.0"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("1.2.3.4"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("5.6.7.8"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("9.10.11.12"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("01.02.03.04"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("005.006.007.008"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("10.20.30.40"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("40.50.60.70"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("80.90.100.110"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("010.020.030.040"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("100.100.100.100"));
        Assert.assertTrue(IpAddressValidator.validateIPv4("200.200.200.200"));
    }

    @Test
    public void testIPv4InvalidAddresses() {
        Assert.assertFalse(IpAddressValidator.validateIPv4(null));
        Assert.assertFalse(IpAddressValidator.validateIPv4("0.0.0"));
        Assert.assertFalse(IpAddressValidator.validateIPv4("255.255.255.256"));
        Assert.assertFalse(IpAddressValidator.validateIPv4("1.2.3.4.5"));
        Assert.assertFalse(IpAddressValidator.validateIPv4("4.5 .6.7"));
        Assert.assertFalse(IpAddressValidator.validateIPv4("8.-9.10.11"));
        Assert.assertFalse(IpAddressValidator.validateIPv4("10.20.30.40."));
        /* false but might be accepted by some people... */
        Assert.assertFalse(IpAddressValidator.validateIPv4("40..60.70"));
        Assert.assertFalse(IpAddressValidator.validateIPv4("100.300.100.100"));
        Assert.assertFalse(IpAddressValidator.validateIPv4("200.200.200.2000"));
    }
}
