package io.eguan.utils.mapper;

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

import io.eguan.configuration.ConfigValidationException;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.utils.UuidCharSequence;
import io.eguan.utils.mapper.DeepFileMapper;
import io.eguan.utils.mapper.DirPrefixLengthConfigKey;
import io.eguan.utils.mapper.DirStructureDepthConfigKey;
import io.eguan.utils.mapper.FileMapper;
import io.eguan.utils.mapper.FileMapperConfigurationContext;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

/**
 * Tests for {@link FileMapper} implementations.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class TestFileMappers {

    /** Base dir */
    private File tempBaseDir;
    /** Default configuration */
    private MetaConfiguration defaultConfiguration;

    @Before
    public void createConfig() throws Exception {
        // Default config file mapper
        tempBaseDir = Files.createTempDir();
        defaultConfiguration = MetaConfiguration.newConfiguration(new ByteArrayInputStream(new byte[0]),
                FileMapperConfigurationContext.getInstance());
    }

    @After
    public void cleanConfig() throws Exception {
        if (tempBaseDir != null) {
            io.eguan.utils.Files.deleteRecursive(tempBaseDir.toPath());
        }
    }

    @Test
    public void testFlatMapper() {
        final FileMapper fileMapper = FileMapper.Type.FLAT.newInstance(tempBaseDir, 32, defaultConfiguration);
        final UUID uuid = new UUID(2378821352914714605L, 3607550935519964895L);
        final CharSequence id = new UuidCharSequence(uuid);
        Assert.assertEquals(tempBaseDir.getAbsolutePath() + File.separator + "2103456abc987fed3210987654cbaedf",
                fileMapper.mapIdToFile(id).getAbsolutePath());
    }

    @Test
    public void testDefaultDeepMapper() {
        final FileMapper fileMapper = FileMapper.Type.DEEP.newInstance(tempBaseDir, 32, defaultConfiguration);
        final UUID uuid = new UUID(2378821352914714605L, 3607550935519964895L);
        final CharSequence id = new UuidCharSequence(uuid);
        final MetaConfiguration config = defaultConfiguration;

        // compute the expected result from config parameters
        final String uuidString = uuid.toString().replaceAll("-", "");
        final int prefixLength = DirPrefixLengthConfigKey.getInstance().getTypedValue(config).intValue();
        final int dirDepth = DirStructureDepthConfigKey.getInstance().getTypedValue(config).intValue();

        final String sub = uuidString.substring(0, prefixLength * dirDepth);
        final String rep = sub.replaceAll("([0-9a-fA-F]{" + prefixLength + "})", "$1" + File.separator);

        Assert.assertEquals(tempBaseDir.getAbsolutePath() + File.separator + uuidString.replaceFirst(sub, rep),
                fileMapper.mapIdToFile(id).getAbsolutePath());
    }

    @Test
    public void testDeepMapper6x5() {
        final FileMapper fileMapper = new DeepFileMapper(tempBaseDir, 32, 6, 5);
        final UUID uuid = new UUID(2378821352914714605L, 3607550935519964895L);
        final CharSequence id = new UuidCharSequence(uuid);
        Assert.assertEquals(tempBaseDir.getAbsolutePath() + File.separator + "210345" + File.separator + "6abc98"
                + File.separator + "7fed32" + File.separator + "109876" + File.separator + "54cbae" + File.separator
                + "df", fileMapper.mapIdToFile(id).getAbsolutePath());
    }

    @Test
    public void testDeepMapper1x5() {
        final FileMapper fileMapper = new DeepFileMapper(tempBaseDir, 32, 1, 5);
        final UUID uuid = new UUID(2378821352914714605L, 3607550935519964895L);
        final CharSequence id = new UuidCharSequence(uuid);
        Assert.assertEquals(tempBaseDir.getAbsolutePath() + File.separator + "2" + File.separator + "1"
                + File.separator + "0" + File.separator + "3" + File.separator + "4" + File.separator
                + "56abc987fed3210987654cbaedf", fileMapper.mapIdToFile(id).getAbsolutePath());
    }

    @Test
    public void testDeepMapper5x1() {
        final FileMapper fileMapper = new DeepFileMapper(tempBaseDir, 32, 5, 1);
        final UUID uuid = new UUID(2378821352914714605L, 3607550935519964895L);
        final CharSequence id = new UuidCharSequence(uuid);
        Assert.assertEquals(tempBaseDir.getAbsolutePath() + File.separator + "21034" + File.separator
                + "56abc987fed3210987654cbaedf", fileMapper.mapIdToFile(id).getAbsolutePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeepMapper4x8() {
        new DeepFileMapper(tempBaseDir, 32, 4, 8);
    }

    @Test
    public void testConfig6x5() throws Exception {
        final String config = "io.eguan.filemapping.dir.prefix.length=6\nio.eguan.filemapping.dir.structure.depth=5";
        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(
                new ByteArrayInputStream(config.getBytes()), FileMapperConfigurationContext.getInstance());
        final FileMapper fileMapper = FileMapper.Type.DEEP.newInstance(tempBaseDir, 32, configuration);

        final UUID uuid = new UUID(2378821352914714605L, 3607550935519964895L);
        final CharSequence id = new UuidCharSequence(uuid);
        Assert.assertEquals(tempBaseDir.getAbsolutePath() + File.separator + "210345" + File.separator + "6abc98"
                + File.separator + "7fed32" + File.separator + "109876" + File.separator + "54cbae" + File.separator
                + "df", fileMapper.mapIdToFile(id).getAbsolutePath());
    }

    @Test
    public void testConfig1x5() throws Exception {
        final String config = "io.eguan.filemapping.dir.prefix.length=1\nio.eguan.filemapping.dir.structure.depth=5";
        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(
                new ByteArrayInputStream(config.getBytes()), FileMapperConfigurationContext.getInstance());
        final FileMapper fileMapper = FileMapper.Type.DEEP.newInstance(tempBaseDir, 32, configuration);

        final UUID uuid = new UUID(2378821352914714605L, 3607550935519964895L);
        final CharSequence id = new UuidCharSequence(uuid);
        Assert.assertEquals(tempBaseDir.getAbsolutePath() + File.separator + "2" + File.separator + "1"
                + File.separator + "0" + File.separator + "3" + File.separator + "4" + File.separator
                + "56abc987fed3210987654cbaedf", fileMapper.mapIdToFile(id).getAbsolutePath());
    }

    @Test
    public void testConfig5x1() throws Exception {
        final String config = "io.eguan.filemapping.dir.prefix.length=5\nio.eguan.filemapping.dir.structure.depth=1";
        final MetaConfiguration configuration = MetaConfiguration.newConfiguration(
                new ByteArrayInputStream(config.getBytes()), FileMapperConfigurationContext.getInstance());
        final FileMapper fileMapper = FileMapper.Type.DEEP.newInstance(tempBaseDir, 32, configuration);

        final UUID uuid = new UUID(2378821352914714605L, 3607550935519964895L);
        final CharSequence id = new UuidCharSequence(uuid);
        Assert.assertEquals(tempBaseDir.getAbsolutePath() + File.separator + "21034" + File.separator
                + "56abc987fed3210987654cbaedf", fileMapper.mapIdToFile(id).getAbsolutePath());
    }

    @Test(expected = ConfigValidationException.class)
    public void testConfig4x8() throws Exception {
        final String config = "io.eguan.filemapping.dir.prefix.length=4\nio.eguan.filemapping.dir.structure.depth=8";
        MetaConfiguration.newConfiguration(new ByteArrayInputStream(config.getBytes()),
                FileMapperConfigurationContext.getInstance());
    }
}
