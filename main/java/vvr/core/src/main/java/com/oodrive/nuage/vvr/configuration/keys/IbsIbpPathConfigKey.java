package com.oodrive.nuage.vvr.configuration.keys;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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

import java.io.File;
import java.util.ArrayList;

import com.oodrive.nuage.configuration.FileListConfigKey;
import com.oodrive.nuage.configuration.MetaConfiguration;

/**
 * Key holding the list of distinct paths to store Ibp instances in as a {@link FileListConfigKey}.
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
 * <td>List of existing paths to IBP storage directories mapped to {@value #IBS_CONFIG_KEY} in the IBS configuration
 * file.</td>
 * <td>FALSE</td>
 * <td>comma-separated directory paths</td>
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
public final class IbsIbpPathConfigKey extends FileListConfigKey implements IbsConfigKey {

    private static final String NAME = "ibp.path";

    private static final String IBS_CONFIG_KEY = "ibp_path";

    private static final IbsIbpPathConfigKey INSTANCE = new IbsIbpPathConfigKey();

    public static IbsIbpPathConfigKey getInstance() {
        return INSTANCE;
    }

    private IbsIbpPathConfigKey() {
        super(NAME, ",", true, true, true);
    }

    @Override
    protected final ArrayList<File> getDefaultValue() {
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
