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
 * Key holding the path to store the IbpGen instance in as a {@link FileConfigKey}.
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
 * <td>Storage path for hot data and configuration persistence, mapped to {@value #IBS_CONFIG_KEY} in the IBS
 * configuration file.</td>
 * <td>TRUE</td>
 * <td>directory path</td>
 * <td>{@link String}</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>N/A</td>
 * </tr>
 * </table>
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class IbsIbpGenPathConfigKey extends FileConfigKey implements IbsConfigKey {

    private static final String NAME = "ibpgen.path";

    private static final String IBS_CONFIG_KEY = "ibpgen_path";

    private static final IbsIbpGenPathConfigKey INSTANCE = new IbsIbpGenPathConfigKey();

    public static IbsIbpGenPathConfigKey getInstance() {
        return INSTANCE;
    }

    private IbsIbpGenPathConfigKey() {
        super(NAME, true, true, true);
    }

    @Override
    protected final File getDefaultValue() {
        return null;
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
