package io.eguan.vvr.configuration.keys;

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

import io.eguan.configuration.EnumConfigKey;
import io.eguan.configuration.MetaConfiguration;

/**
 * Key holding the value for the internal compression option of the IBS.
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
 * <td>The value for the internal compression option of the IBS, mapped to {@value #IBS_CONFIG_KEY} in the IBS
 * configuration file.</td>
 * <td>FALSE</td>
 * <td>constants defined by {@link IbsCompressionValue}</td>
 * <td>{@link String}</td>
 * <td>{@value #DEFAULT_VALUE}</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * @author jmcaba
 * 
 */
public final class IbsCompressionConfigKey extends EnumConfigKey<IbsCompressionValue> implements IbsConfigKey {

    private static final String NAME = "compression";

    private static final String IBS_CONFIG_KEY = "compression";

    private static final IbsCompressionValue DEFAULT_VALUE = IbsCompressionValue.back;

    private static final IbsCompressionConfigKey INSTANCE = new IbsCompressionConfigKey();

    public static IbsCompressionConfigKey getInstance() {
        return INSTANCE;
    }

    private IbsCompressionConfigKey() throws NullPointerException {
        super(NAME, IbsCompressionValue.class);
    }

    @Override
    protected final IbsCompressionValue getDefaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public final String getBackendConfigKey() {
        return IBS_CONFIG_KEY;
    }

    @Override
    public final String getBackendConfigValue(final MetaConfiguration configuration) {
        return valueToString(getTypedValue(configuration));
    }

}
