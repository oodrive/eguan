package io.eguan.vvr.configuration.keys;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

import io.eguan.configuration.EnumConfigKey;
import io.eguan.configuration.MetaConfiguration;

/**
 * Key defining how verbose logging should be, mapped to {@value #IBS_CONFIG_KEY} in the IBS configuration file.
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
 * <td>Defines how verbose logging should be, mapped to {@value #IBS_CONFIG_KEY} in the IBS configuration file.</td>
 * <td>FALSE</td>
 * <td>{@link IbsLogLevel} constants, one of: fatal, error, warn, info, debug, trace, off</td>
 * <td>{@link String}</td>
 * <td>warn</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class IbsLogLevelConfigKey extends EnumConfigKey<IbsLogLevel> implements IbsConfigKey {

    private static final String NAME = "loglevel";

    private static final String IBS_CONFIG_KEY = "loglevel";

    private static final IbsLogLevel DEFAULT_VALUE = IbsLogLevel.warn;

    private static final IbsLogLevelConfigKey INSTANCE = new IbsLogLevelConfigKey();

    public static IbsLogLevelConfigKey getInstance() {
        return INSTANCE;
    }

    private IbsLogLevelConfigKey() throws NullPointerException {
        super(NAME, IbsLogLevel.class);
    }

    @Override
    public final String getBackendConfigKey() {
        return IBS_CONFIG_KEY;
    }

    @Override
    public final String getBackendConfigValue(final MetaConfiguration configuration) {
        return valueToString(getTypedValue(configuration));
    }

    @Override
    protected final IbsLogLevel getDefaultValue() {
        return DEFAULT_VALUE;
    }

}
