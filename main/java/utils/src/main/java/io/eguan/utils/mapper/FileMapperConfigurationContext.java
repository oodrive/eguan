package io.eguan.utils.mapper;

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

import io.eguan.configuration.AbstractConfigurationContext;
import io.eguan.configuration.ConfigKey;
import io.eguan.configuration.ConfigurationContext;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.ValidationError;
import io.eguan.configuration.ValidationError.ErrorType;

import java.util.List;

/**
 * Context for configuration keys specific to the file mapping.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class FileMapperConfigurationContext extends AbstractConfigurationContext {

    protected static final String NAME = "io.eguan.filemapping";

    private static final FileMapperConfigurationContext INSTANCE = new FileMapperConfigurationContext();

    /**
     * Gets the singleton instance of this context.
     * 
     * @return the instance constructed by {@link #FileMapperConfigurationContext()}
     */
    public static final FileMapperConfigurationContext getInstance() {
        return INSTANCE;
    }

    /**
     * Constructs an instance with the given {@link #NAME} and all keys references by this context.
     */
    private FileMapperConfigurationContext() {
        super(NAME, DirPrefixLengthConfigKey.getInstance(), DirStructureDepthConfigKey.getInstance(),
                FileMapperConfigKey.getInstance());
    }

    @Override
    public final List<ValidationError> validateConfiguration(final MetaConfiguration configuration) {
        final List<ValidationError> result = super.validateConfiguration(configuration);
        final DirPrefixLengthConfigKey dirPrefixKey = DirPrefixLengthConfigKey.getInstance();
        final DirStructureDepthConfigKey dirStructureKey = DirStructureDepthConfigKey.getInstance();

        final Integer dirPrefixLengthValue = dirPrefixKey.getTypedValue(configuration);
        final Integer dirStructureDepthValue = dirStructureKey.getTypedValue(configuration);

        if (com.google.common.math.IntMath.checkedMultiply(dirPrefixLengthValue.intValue(),
                dirStructureDepthValue.intValue()) > 31) {
            result.add(new ValidationError(ErrorType.VALUE_INVALID, new ConfigurationContext[] { this },
                    new ConfigKey[] { dirPrefixKey, dirStructureKey }, dirPrefixLengthValue + " * "
                            + dirStructureDepthValue, "product is greater than maximum of 31"));
        }

        return result;
    }
}
