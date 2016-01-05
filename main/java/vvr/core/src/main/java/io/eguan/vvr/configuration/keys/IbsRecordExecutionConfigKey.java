package io.eguan.vvr.configuration.keys;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import io.eguan.configuration.FileConfigKey;
import io.eguan.configuration.MetaConfiguration;

import java.io.File;

/**
 * Key holding the path to the record execution file of an Ibs as a {@link FileConfigKey}.
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
 * <td>Storage path for the ibs recorder file to replay execution, mapped to {@value #IBS_CONFIG_KEY} in the IBS
 * configuration file.</td>
 * <td>FALSE</td>
 * <td>file path</td>
 * <td>{@link String}</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author jmcaba
 * @author ebredzinski
 * 
 */
public final class IbsRecordExecutionConfigKey extends FileConfigKey implements IbsConfigKey {

    private static final String NAME = "record.execution";

    private static final String IBS_CONFIG_KEY = "record_execution";

    private static final IbsRecordExecutionConfigKey INSTANCE = new IbsRecordExecutionConfigKey();

    public static IbsRecordExecutionConfigKey getInstance() {
        return INSTANCE;
    }

    private IbsRecordExecutionConfigKey() {
        super(NAME, false, false, false);
    }

    @Override
    protected final File getDefaultValue() {
        return null;
    }

    @Override
    public boolean isRequired() {
        return false;
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
