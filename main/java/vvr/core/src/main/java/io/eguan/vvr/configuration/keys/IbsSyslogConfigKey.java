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

import io.eguan.configuration.BooleanConfigKey;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.utils.LogUtils;

public class IbsSyslogConfigKey extends BooleanConfigKey implements IbsConfigKey {

    private static final String NAME = "syslog";

    private static final String IBS_VALUE_TRUE = "yes";

    private static final String IBS_VALUE_FALSE = "no";

    private static final String IBS_SYSLOG_CONFIG_KEY = "syslog";

    private static final IbsSyslogConfigKey INSTANCE = new IbsSyslogConfigKey();

    public static IbsSyslogConfigKey getInstance() {
        return INSTANCE;
    }

    protected IbsSyslogConfigKey() {
        super(NAME);
    }

    @Override
    public String getBackendConfigKey() {
        return IBS_SYSLOG_CONFIG_KEY;
    }

    @Override
    public String getBackendConfigValue(final MetaConfiguration configuration) {
        if (LogUtils.getSyslogEnable()) {
            return IBS_VALUE_TRUE;
        }
        else {
            return IBS_VALUE_FALSE;
        }
    }

    @Override
    protected Boolean getDefaultValue() {
        return LogUtils.getSyslogEnable();
    }
}
