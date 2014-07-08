package com.oodrive.nuage.configuration;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import com.oodrive.nuage.configuration.ValidConfigurationContext.ContextTestHelper;

/**
 * Implementation of {@link TestAbstractConfigKeys} for testing {@link FileConfigKey}s.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class TestFileListConfigKey extends TestAbstractConfigKeys {

    private static final class TestableFileListConfigKey extends FileListConfigKey {

        private final boolean hasDefault;

        private final boolean required;

        protected TestableFileListConfigKey(final boolean required, final boolean hasDefault,
                final boolean checkDirectory, final boolean checkExistence, final boolean checkWritable) {
            super("test.filelist.key", ":", checkDirectory, checkExistence, checkWritable);
            this.hasDefault = hasDefault;
            this.required = required;
        }

        @Override
        protected ArrayList<File> getDefaultValue() {
            return hasDefault ? new ArrayList<File>(Arrays.asList(new File[] { new File("/tmp") })) : null;
        }

        @Override
        public boolean isRequired() {
            return required;
        }

    }

    @Override
    protected final AbstractConfigKey getTestKey(final boolean required, final boolean hasDefault) {
        return new TestableFileListConfigKey(required, hasDefault, false, false, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public final void testParseValueFailEmptyElement() {
        final AbstractConfigKey target = getTestKey(false, false);

        target.parseValue("/tmp/: :/opt");
    }

    /**
     * Tests selective failure of additional checks for existence, directory and writable state.
     */
    @Test
    public final void testCheckValueFailOptionalChecks() throws IOException {
        TestableFileListConfigKey checkingKey = null;

        Path validFile = null;
        Path invalidFile = null;

        final String tmpFilePrefix = this.getClass().getSimpleName();
        final Set<PosixFilePermission> writablePermissions = PosixFilePermissions.fromString("rwxr-x---");
        final Set<PosixFilePermission> unwritablePermissions = PosixFilePermissions.fromString("r-xr-x---");

        for (int i = 1; i < 8; i++) {
            final boolean isDirectory = (i & 1) != 0;
            final boolean exists = (i & 2) != 0;
            final boolean isWritable = (i & 4) != 0;

            checkingKey = new TestableFileListConfigKey(false, false, isDirectory, exists, isWritable);

            try {
                validFile = isDirectory ? Files.createTempDirectory(tmpFilePrefix) : Files.createTempFile(
                        tmpFilePrefix, null);
                assertTrue(Files.exists(validFile));

                invalidFile = !isDirectory ? Files.createTempDirectory(tmpFilePrefix) : Files.createTempFile(
                        tmpFilePrefix, null);
                assertTrue(Files.exists(invalidFile));

                Files.setPosixFilePermissions(validFile, (isWritable ? writablePermissions : unwritablePermissions));
                Files.setPosixFilePermissions(invalidFile, (!isWritable ? writablePermissions : unwritablePermissions));

                if (exists) {
                    Files.delete(invalidFile);
                }

                final ValidationError reportInvalid = checkingKey.checkValue(new ArrayList<File>(Arrays
                        .asList(new File[] { invalidFile.toFile() })));

                assertEquals(ValidationError.ErrorType.VALUE_INVALID, reportInvalid.getType());

                final ValidationError reportValid = checkingKey.checkValue(new ArrayList<File>(Arrays
                        .asList(new File[] { validFile.toFile() })));

                assertEquals(ValidationError.NO_ERROR, reportValid);
            }
            finally {
                if (Files.exists(invalidFile)) {
                    Files.setPosixFilePermissions(invalidFile, writablePermissions);
                    Files.delete(invalidFile);
                }
                if (Files.exists(validFile)) {
                    Files.setPosixFilePermissions(validFile, writablePermissions);
                    Files.delete(validFile);
                }
            }
        }
    }

    /**
     * Test failure to modify a value within a {@link MetaConfiguration} for a {@link FileListConfigKey}.
     */
    @Test
    public final void testImmutableConfigurationValue() throws NullPointerException, IllegalArgumentException,
            IOException, ConfigValidationException {
        final TestableFileListConfigKey targetKey = (TestableFileListConfigKey) getTestKey(false, true);
        final AbstractConfigurationContext targetContext = new AbstractConfigurationContext("test.context", targetKey) {
        };

        final ArrayList<File> defaultValue = targetKey.getDefaultValue();
        final Properties configProps = new Properties();
        configProps.setProperty(targetContext.getPropertyKey(targetKey), targetKey.valueToString(defaultValue));

        final MetaConfiguration targetConfig = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(configProps), targetContext);

        final ArrayList<File> modifiedValue = targetKey.getTypedValue(targetConfig);

        // alters the read value (a little bit)
        modifiedValue.clear();

        // re-reads the value from config
        final ArrayList<File> originalValue = targetKey.getTypedValue(targetConfig);

        for (final File currValue : defaultValue) {
            assertEquals(currValue, originalValue.get(defaultValue.indexOf(currValue)));
        }

    }
}
