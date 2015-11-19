package io.eguan.utils;

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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to validate a string representation of an IP address.
 * 
 * @author oodrive
 * @author llambert
 */
public final class IpAddressValidator {
    /** Regexp that matches one IPv4 address digit */
    private static final String IPV4_DIGIT = "([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
    /** Regexp that matches an IPv4 address */
    private static final String IPV4_PATTERN = "^" + IPV4_DIGIT + "\\." + IPV4_DIGIT + "\\." + IPV4_DIGIT + "\\."
            + IPV4_DIGIT + "$";

    /**
     * No instance.
     */
    private IpAddressValidator() {
        throw new AssertionError("No instance");
    }

    /**
     * Simple IPv4 addresses validator.
     * <p>
     * The wildcard address (0.0.0.0) and the world broadcast address (255.255.255.255) are valid. The implicit zero
     * notation is not considered to be valid (ie <code>validateIPv4(192.168..1)</code> returns false.)
     * 
     * 
     * @param ip
     *            The string that will be checked
     * @return false if the input is either not a valid representation of an IP address or null and true if the input
     *         string represents a valid IPv4 address
     */
    public static boolean validateIPv4(final String ip) {
        if (ip == null) {
            return false;
        }
        else {
            final Pattern pattern = Pattern.compile(IPV4_PATTERN);
            final Matcher matcher = pattern.matcher(ip);
            return matcher.matches();
        }
    }
}
