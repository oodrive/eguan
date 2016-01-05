package io.eguan.vvr.configuration;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import io.eguan.configuration.AbstractConfigKey;
import io.eguan.configuration.AbstractConfigurationContext;
import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.configuration.StringConfigKey;
import io.eguan.configuration.ValidConfigurationContext;
import io.eguan.ibs.Ibs;
import io.eguan.ibs.IbsFactory;
import io.eguan.vvr.configuration.IbsConfigurationContext;
import io.eguan.vvr.configuration.keys.IbsAutoConfRamSize;
import io.eguan.vvr.configuration.keys.IbsBufferRotationDelay;
import io.eguan.vvr.configuration.keys.IbsBufferRotationThreshold;
import io.eguan.vvr.configuration.keys.IbsBufferWriteDelayIncrement;
import io.eguan.vvr.configuration.keys.IbsBufferWriteDelayLevelSize;
import io.eguan.vvr.configuration.keys.IbsBufferWriteDelayThreshold;
import io.eguan.vvr.configuration.keys.IbsCompressionConfigKey;
import io.eguan.vvr.configuration.keys.IbsCompressionValue;
import io.eguan.vvr.configuration.keys.IbsConfigKey;
import io.eguan.vvr.configuration.keys.IbsDisableBackgroundCompactionForIbpgenConfigKey;
import io.eguan.vvr.configuration.keys.IbsDumpAtStopBestEffortDelayConfigKey;
import io.eguan.vvr.configuration.keys.IbsHotDataConfigKey;
import io.eguan.vvr.configuration.keys.IbsIbpGenPathConfigKey;
import io.eguan.vvr.configuration.keys.IbsIbpPathConfigKey;
import io.eguan.vvr.configuration.keys.IbsLdbBlockRestartIntervalConfigKey;
import io.eguan.vvr.configuration.keys.IbsLdbBlockSizeConfigKey;
import io.eguan.vvr.configuration.keys.IbsLogLevel;
import io.eguan.vvr.configuration.keys.IbsLogLevelConfigKey;
import io.eguan.vvr.configuration.keys.IbsOwnerUuidConfigKey;
import io.eguan.vvr.configuration.keys.IbsRecordExecutionConfigKey;
import io.eguan.vvr.configuration.keys.IbsSyslogConfigKey;
import io.eguan.vvr.configuration.keys.IbsUuidConfigKey;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.model.InitializationError;

/**
 * {@link ValidConfigurationContext} implementation with {@link IbsConfigurationContext}-specific tests.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * @author jmcaba
 * @author ebredzinski
 * 
 */
public final class TestValidIbsConfigurationContext extends ValidConfigurationContext {

    private static class ContextTestHelperIbs extends ContextTestHelper<IbsConfigurationContext> {

        protected File ibsIbpGenDir;
        protected ArrayList<File> ibsIbpDirList;

        protected ContextTestHelperIbs() {
            super(IbsConfigurationContext.getInstance());
        }

        @Override
        public void setUp() throws InitializationError {
            final String tmpFilePrefix = TestValidIbsConfigurationContext.class.getSimpleName();

            try {
                ibsIbpGenDir = Files.createTempDirectory(tmpFilePrefix).toFile();

                ibsIbpDirList = new ArrayList<File>();
                for (int i = 0; i < 4; i++) {
                    ibsIbpDirList.add(Files.createTempDirectory(tmpFilePrefix).toFile());
                }
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
        }

        @Override
        public void tearDown() throws InitializationError {
            try {
                io.eguan.utils.Files.deleteRecursive(ibsIbpGenDir.toPath());

                for (final File currFile : ibsIbpDirList) {
                    io.eguan.utils.Files.deleteRecursive(currFile.toPath());
                }
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
        }

        @Override
        public Properties getConfig() {
            final Properties result = new Properties();

            result.setProperty(getPropertyKey(IbsIbpPathConfigKey.getInstance()),
                    ibsIbpDirList.toString().replaceAll("[\\[\\]]", ""));

            result.setProperty(getPropertyKey(IbsIbpGenPathConfigKey.getInstance()), ibsIbpGenDir.toString());

            result.setProperty(getPropertyKey(IbsHotDataConfigKey.getInstance()), "TRUE");

            result.setProperty(getPropertyKey(IbsDisableBackgroundCompactionForIbpgenConfigKey.getInstance()), "TRUE");

            result.setProperty(getPropertyKey(IbsCompressionConfigKey.getInstance()),
                    IbsCompressionValue.valueOf("front").toString());

            result.setProperty(getPropertyKey(IbsUuidConfigKey.getInstance()), UUID.randomUUID().toString());

            result.setProperty(getPropertyKey(IbsOwnerUuidConfigKey.getInstance()), UUID.randomUUID().toString());

            result.setProperty(getPropertyKey(IbsLogLevelConfigKey.getInstance()), IbsLogLevel.valueOf("info")
                    .toString());

            result.setProperty(getPropertyKey(IbsLdbBlockSizeConfigKey.getInstance()), Integer.valueOf(4096).toString());

            result.setProperty(getPropertyKey(IbsLdbBlockRestartIntervalConfigKey.getInstance()), Integer
                    .valueOf(32768).toString());

            result.setProperty(getPropertyKey(IbsBufferRotationThreshold.getInstance()), Integer.valueOf(1048576)
                    .toString());
            result.setProperty(getPropertyKey(IbsBufferRotationDelay.getInstance()), Integer.valueOf(20).toString());

            result.setProperty(getPropertyKey(IbsBufferWriteDelayThreshold.getInstance()), Integer.valueOf(20)
                    .toString());

            result.setProperty(getPropertyKey(IbsBufferWriteDelayLevelSize.getInstance()), Integer.valueOf(7)
                    .toString());

            result.setProperty(getPropertyKey(IbsBufferWriteDelayIncrement.getInstance()), Integer.valueOf(10)
                    .toString());

            result.setProperty(getPropertyKey(IbsAutoConfRamSize.getInstance()), Integer.valueOf(4 << 30).toString());

            result.setProperty(getPropertyKey(IbsRecordExecutionConfigKey.getInstance()), "");

            result.setProperty(getPropertyKey(IbsDumpAtStopBestEffortDelayConfigKey.getInstance()), Integer.valueOf(10)
                    .toString());

            result.setProperty(getPropertyKey(IbsSyslogConfigKey.getInstance()), Boolean.FALSE.toString());
            return result;
        }

    }

    /**
     * Generate errors every 10 {@link Ibs} IO operations.
     * 
     */
    private static final class ContextTestHelperIbsError extends ContextTestHelperIbs {

        private File ibpDirParent;
        private File ibpDir;

        @Override
        public void setUp() throws InitializationError {
            final String tmpFilePrefix = TestValidIbsConfigurationContext.class.getSimpleName();

            try {
                ibsIbpGenDir = Files.createTempDirectory(tmpFilePrefix).toFile();

                ibsIbpDirList = new ArrayList<File>(1);
                ibpDirParent = Files.createTempDirectory(tmpFilePrefix).toFile();
                ibpDir = new File(ibpDirParent, Ibs.UNIT_TEST_IBS_HEADER + "10");
                ibpDir.mkdir();
                ibsIbpDirList.add(ibpDir);
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
        }

        @Override
        public void tearDown() throws InitializationError {
            try {
                io.eguan.utils.Files.deleteRecursive(ibsIbpGenDir.toPath());
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
            try {
                io.eguan.utils.Files.deleteRecursive(ibpDirParent.toPath());
            }
            catch (final IOException e) {
                throw new InitializationError(e);
            }
            // Destroy Ibs
            try {
                final Ibs toDel = IbsFactory.openIbs(ibpDir);
                toDel.destroy();
            }
            catch (final Exception e) {
                throw new InitializationError(e);
            }
        }

    }

    private static final ContextTestHelperIbs testHelper = new ContextTestHelperIbs();
    private static final ContextTestHelperIbs testErrHelper = new ContextTestHelperIbsError();

    private static ArrayList<File> tmpDirectories = new ArrayList<File>();

    @BeforeClass
    public static void setUpClass() throws InitializationError {
        testHelper.setUp();
    }

    @AfterClass
    public static void tearDownClass() throws InitializationError {
        testHelper.tearDown();
        final ArrayList<Throwable> exceptionList = new ArrayList<Throwable>();
        for (final File currFile : tmpDirectories) {
            try {
                Files.deleteIfExists(currFile.toPath());
            }
            catch (final IOException e) {
                exceptionList.add(e);
            }
        }
        if (!exceptionList.isEmpty()) {
            throw new InitializationError(exceptionList);
        }
    }

    @Test
    public final void testMetaConfigurationIbsConfigurationContextAcceptSimilarDirs() throws RuntimeException,
            IOException, ConfigValidationException {

        final Properties config = testHelper.getConfig();

        MetaConfiguration preConfig = null;
        try {
            preConfig = MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                    IbsConfigurationContext.getInstance());
        }
        catch (final ConfigValidationException ce) {
            throw new IllegalStateException(ce);
        }
        final ArrayList<File> ibpPaths = IbsIbpPathConfigKey.getInstance().getTypedValue(preConfig);

        // this directory is valid, but would fail a simple substring test
        final File validIbpPath = new File(ibpPaths.get(0).getAbsolutePath() + "addIbp");
        validIbpPath.mkdirs();
        tmpDirectories.add(validIbpPath);

        final String ibpPropertyKey = testHelper.getPropertyKey(IbsIbpPathConfigKey.getInstance());
        config.setProperty(ibpPropertyKey, validIbpPath.getAbsolutePath() + "," + config.getProperty(ibpPropertyKey));

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                IbsConfigurationContext.getInstance());

    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link IbsConfigurationContext} as context due to a path
     * whose a child of another at the beginning of the Ibp path list.
     * 
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationIbsConfigurationContextFailIbpFirstChildPath() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();

        MetaConfiguration preConfig = null;
        try {
            preConfig = MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                    IbsConfigurationContext.getInstance());
        }
        catch (final ConfigValidationException ce) {
            throw new IllegalStateException(ce);
        }
        final ArrayList<File> ibpPaths = IbsIbpPathConfigKey.getInstance().getTypedValue(preConfig);
        final File badIbpChildPath = new File(ibpPaths.get(0), "badIbpPath");
        badIbpChildPath.mkdirs();
        tmpDirectories.add(badIbpChildPath);

        final String ibpPropertyKey = testHelper.getPropertyKey(IbsIbpPathConfigKey.getInstance());
        config.setProperty(ibpPropertyKey, badIbpChildPath.getAbsolutePath() + "," + config.getProperty(ibpPropertyKey));

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                IbsConfigurationContext.getInstance());
    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link IbsConfigurationContext} as context due to a path
     * whose a child of another at the end of the Ibp path list.
     * 
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationIbsConfigurationContextFailIbpEndChildPath() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();

        MetaConfiguration preConfig = null;
        try {
            preConfig = MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                    IbsConfigurationContext.getInstance());
        }
        catch (final ConfigValidationException ce) {
            throw new IllegalStateException(ce);
        }
        final ArrayList<File> ibpPaths = IbsIbpPathConfigKey.getInstance().getTypedValue(preConfig);
        final File badIbpChildPath = new File(ibpPaths.get(0), "badIbpPath");
        badIbpChildPath.mkdirs();
        tmpDirectories.add(badIbpChildPath);

        final String ibpPropertyKey = testHelper.getPropertyKey(IbsIbpPathConfigKey.getInstance());
        config.setProperty(ibpPropertyKey, config.getProperty(ibpPropertyKey) + "," + badIbpChildPath.getAbsolutePath());

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                IbsConfigurationContext.getInstance());
    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link IbsConfigurationContext} as context due to a path
     * whose a parent of another at the beginning of the Ibp path list.
     * 
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationIbsConfigurationContextFailIbpFirstParentPath() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();

        MetaConfiguration preConfig = null;
        try {
            preConfig = MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                    IbsConfigurationContext.getInstance());
        }
        catch (final ConfigValidationException ce) {
            throw new IllegalStateException(ce);
        }
        final ArrayList<File> ibpPaths = IbsIbpPathConfigKey.getInstance().getTypedValue(preConfig);

        final File badIbpParentPath = ibpPaths.get(0).getParentFile();
        assertNotNull(badIbpParentPath);

        final String ibpPropertyKey = testHelper.getPropertyKey(IbsIbpPathConfigKey.getInstance());
        config.setProperty(ibpPropertyKey,
                badIbpParentPath.getAbsolutePath() + "," + config.getProperty(ibpPropertyKey));

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                IbsConfigurationContext.getInstance());
    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link IbsConfigurationContext} as context due to a path
     * whose a parent of another at the end of the Ibp path list.
     * 
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationIbsConfigurationContextFailIbpEndParentPath() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();

        MetaConfiguration preConfig = null;
        try {
            preConfig = MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                    IbsConfigurationContext.getInstance());
        }
        catch (final ConfigValidationException ce) {
            throw new IllegalStateException(ce);
        }
        final ArrayList<File> ibpPaths = IbsIbpPathConfigKey.getInstance().getTypedValue(preConfig);

        final File badIbpParentPath = ibpPaths.get(0).getParentFile();
        assertNotNull(badIbpParentPath);

        final String ibpPropertyKey = testHelper.getPropertyKey(IbsIbpPathConfigKey.getInstance());
        config.setProperty(ibpPropertyKey,
                config.getProperty(ibpPropertyKey) + "," + badIbpParentPath.getAbsolutePath());

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                IbsConfigurationContext.getInstance());
    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link IbsConfigurationContext} as context due to an
     * IbpGen path overlapping Ibp paths.
     * 
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationIbsConfigurationContextFailIbpGenPath() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();

        MetaConfiguration preConfig = null;
        try {
            preConfig = MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                    IbsConfigurationContext.getInstance());
        }
        catch (final ConfigValidationException ce) {
            throw new IllegalStateException(ce);
        }
        final ArrayList<File> ibpPaths = IbsIbpPathConfigKey.getInstance().getTypedValue(preConfig);
        final File badIbpGenPath = new File(ibpPaths.get(0), "badIbpGenPath");
        badIbpGenPath.mkdirs();
        tmpDirectories.add(badIbpGenPath);
        System.out.println(ibpPaths + ": " + badIbpGenPath);

        final String ibpGenPropertyKey = testHelper.getPropertyKey(IbsIbpGenPathConfigKey.getInstance());
        config.setProperty(ibpGenPropertyKey, badIbpGenPath.getAbsolutePath());

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                IbsConfigurationContext.getInstance());
    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link IbsConfigurationContext} as context due to a
     * <code>null</code> Ibp path list.
     * 
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationIbsConfigurationContextFailIbpPathsNull() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();

        config.remove(testHelper.getPropertyKey(IbsIbpPathConfigKey.getInstance()));

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                IbsConfigurationContext.getInstance());
    }

    /**
     * Test failure to create a {@link MetaConfiguration} with {@link IbsConfigurationContext} as context due to a
     * <code>null</code> IbpGen path.
     * 
     * @throws ConfigValidationException
     *             if the prepared configuration is invalid. Expected for this test.
     */
    @Test(expected = ConfigValidationException.class)
    public final void testMetaConfigurationIbsConfigurationContextFailIbpGenPathNull() throws RuntimeException,
            IOException, ConfigValidationException {
        final Properties config = testHelper.getConfig();

        config.remove(testHelper.getPropertyKey(IbsIbpGenPathConfigKey.getInstance()));

        MetaConfiguration.newConfiguration(ContextTestHelper.getPropertiesAsInputStream(config),
                IbsConfigurationContext.getInstance());
    }

    /**
     * Tests successful execution of {@link IbsConfigurationContext#storeIbsConfig(MetaConfiguration, OutputStream)}.
     */
    @Test
    public final void testStoreIbsConfig() throws RuntimeException, IOException, ConfigValidationException {
        final Properties configProps = testHelper.getConfig();

        final IbsConfigurationContext targetContext = IbsConfigurationContext.getInstance();

        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(configProps), targetContext);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        targetContext.storeIbsConfig(configuration, outputStream);

        final Properties resultProps = new Properties();
        resultProps.load(new ByteArrayInputStream(outputStream.toByteArray()));

        for (final AbstractConfigKey currKey : targetContext.getConfigKeys()) {
            if (!(currKey instanceof IbsConfigKey)) {
                continue;
            }
            final String resultValue = resultProps.getProperty(((IbsConfigKey) currKey).getBackendConfigKey());
            assertNotNull("key saved to Ibs config", resultValue);
            assertEquals("value saved to Ibs config", ContextTestHelper.getStringValue(configuration, currKey),
                    resultValue);
        }
    }

    /**
     * Tests failure of {@link IbsConfigurationContext#storeIbsConfig(MetaConfiguration, OutputStream)} due to a faulty
     * configuration.
     * 
     * @throws IllegalStateException
     *             if the configuration does not manage {@link IbsConfigurationContext}'s keys. Expected for this test.
     */
    @Test(expected = IllegalStateException.class)
    public final void testStoreIbsConfigFailBadConfiguration() throws IllegalStateException, RuntimeException,
            IOException, ConfigValidationException {

        final AbstractConfigurationContext DummyContext = new AbstractConfigurationContext("com.example.test",
                new StringConfigKey("no.key") {

                    @Override
                    protected String getDefaultValue() {
                        return "default";
                    }
                }) {
        };

        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(testHelper.getConfig()), DummyContext);

        IbsConfigurationContext.getInstance().storeIbsConfig(configuration, new ByteArrayOutputStream());

    }

    /**
     * Tests failure of {@link IbsConfigurationContext#storeIbsConfig(MetaConfiguration, OutputStream)} due to a
     * <code>null</code> configuration.
     * 
     * @throws NullPointerException
     *             Expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public final void testStoreIbsConfigFailNullConfiguration() throws NullPointerException, RuntimeException,
            IOException, ConfigValidationException {

        IbsConfigurationContext.getInstance().storeIbsConfig(null, new ByteArrayOutputStream());

    }

    /**
     * Tests failure of {@link IbsConfigurationContext#storeIbsConfig(MetaConfiguration, OutputStream)} due to an
     * unwritable {@link OutputStream}.
     * 
     * @throws IOException
     *             if writing to the {@link OutputStream} fails. Expected for this test.
     */
    @Test(expected = IOException.class)
    public final void testStoreIbsConfigFailBadOutputStream() throws RuntimeException, IOException,
            ConfigValidationException {
        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(testHelper.getConfig()),
                IbsConfigurationContext.getInstance());

        final OutputStream badOutputStream = new OutputStream() {

            @Override
            public final void write(final int b) throws IOException {
                throw new IOException();
            }
        };

        IbsConfigurationContext.getInstance().storeIbsConfig(configuration, badOutputStream);

    }

    /**
     * Tests failure of {@link IbsConfigurationContext#storeIbsConfig(MetaConfiguration, OutputStream)} due to a
     * <code>null</code> {@link OutputStream}.
     * 
     * @throws NullPointerException
     *             Expected for this test.
     */
    @Test(expected = NullPointerException.class)
    public final void testStoreIbsConfigFailNullOutputStream() throws NullPointerException, RuntimeException,
            IOException, ConfigValidationException {
        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(
                ContextTestHelper.getPropertiesAsInputStream(testHelper.getConfig()),
                IbsConfigurationContext.getInstance());

        IbsConfigurationContext.getInstance().storeIbsConfig(configuration, null);

    }

    @Override
    public final ContextTestHelper<?> getTestHelper() {
        return testHelper;
    }

    public final ContextTestHelper<?> getTestErrHelper() {
        return testErrHelper;
    }
}
