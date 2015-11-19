package io.eguan.rest.container;

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

import io.eguan.configuration.BooleanConfigKey;

/**
 * Key defining whether to register a shutdown hook making the server stop on JVM shutdown.
 * 
 * <table border='1'>
 * <tr>
 * <th>NAME</th>
 * <th>DESCRIPTION</th>
 * <th>REQUIRED</th>
 * <th>UNIT</th>
 * <th>TYPE</th>
 * <th>DEFAULT</th>
 * <th>MIN</th>
 * <th>MAX</th>
 * </tr>
 * <tr>
 * <td>{@value #NAME}</td>
 * <td>Whether to register a shutdown hook making the server stop on JVM shutdown</td>
 * <td>FALSE</td>
 * <td>either "true", "yes" or "false", "no" (case insensitive)</td>
 * <td>{@link Boolean}</td>
 * <td>true</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class JettyStopAtShutdownConfigKey extends BooleanConfigKey {

    protected static final String NAME = "shutdownOnExit";

    private static final Boolean DEFAULT_VALUE = Boolean.TRUE;

    private static final JettyStopAtShutdownConfigKey INSTANCE = new JettyStopAtShutdownConfigKey();

    /**
     * Gets the predefined singleton instance.
     * 
     * @return the singleton instance constructed by {@link #JettyShutdownOnExitConfigKey()}
     */
    public static final JettyStopAtShutdownConfigKey getInstance() {
        return INSTANCE;
    }

    private JettyStopAtShutdownConfigKey() {
        super(NAME);
    }

    @Override
    protected final Boolean getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
