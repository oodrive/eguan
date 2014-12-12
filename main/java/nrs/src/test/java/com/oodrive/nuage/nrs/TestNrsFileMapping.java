package com.oodrive.nuage.nrs;

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

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.oodrive.nuage.configuration.MetaConfiguration;
import com.oodrive.nuage.hash.HashAlgorithm;
import com.oodrive.nuage.proto.Common.OpCode;
import com.oodrive.nuage.proto.Common.ProtocolVersion;
import com.oodrive.nuage.proto.Common.Type;
import com.oodrive.nuage.proto.nrs.NrsRemote.NrsFileMapping;
import com.oodrive.nuage.proto.nrs.NrsRemote.NrsFileMapping.NrsClusterHash;
import com.oodrive.nuage.proto.vvr.VvrRemote.RemoteOperation;
import com.oodrive.nuage.utils.Strings;

public final class TestNrsFileMapping extends TestNrsFileJanitorAbstract {

    @Test(expected = IllegalStateException.class)
    public void testNrsFileMappingNotOpened() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);

        janitor.init();
        try {
            final NrsFile nrsFile = createTestNrsFile(janitor, config, false, false, false);
            nrsFile.getFileMapping(HashAlgorithm.MD5);
        }
        finally {
            janitor.fini();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testNrsFileMappingRoot() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);

        janitor.init();
        try {
            final NrsFile nrsFile = createTestNrsFile(janitor, config, false, true, false);
            final NrsFile nrsFileOpened = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), true);
            try {
                nrsFileOpened.getFileMapping(HashAlgorithm.MD5);
            }
            finally {
                janitor.unlockNrsFile(nrsFileOpened);
            }
        }
        finally {
            janitor.fini();
        }
    }

    @Test
    public void testNrsFileMapping() throws IOException {
        final MetaConfiguration config = getConfiguration();
        final NrsFileJanitor janitor = new NrsFileJanitor(config);

        janitor.init();
        try {
            final NrsFile nrsFile = createTestNrsFile(janitor, config, false, false, false);
            final NrsFile nrsFileOpened = janitor.openNrsFile(nrsFile.getDescriptor().getFileId(), false);

            try {
                // Check empty file mapping
                {
                    final RemoteOperation.Builder builder = nrsFileOpened.getFileMapping(HashAlgorithm.MD5);
                    nrsFileOpened.resetUpdate(); // Reset update state (update not tested here)

                    // Read file mapping
                    builder.setVersion(ProtocolVersion.VERSION_1);
                    builder.setType(Type.NRS);
                    builder.setOp(OpCode.LIST);
                    final RemoteOperation operation = builder.build();
                    Assert.assertTrue(operation.hasNrsFileMapping());
                    final NrsFileMapping nrsFileMapping = operation.getNrsFileMapping();

                    Assert.assertTrue(nrsFileMapping.hasClusterSize());
                    Assert.assertEquals(NrsClusterSizeConfigKey.getInstance().getTypedValue(config).intValue(),
                            nrsFileMapping.getClusterSize());
                    Assert.assertTrue(nrsFileMapping.hasVersion());
                    Assert.assertEquals(0L, nrsFileMapping.getVersion());

                    final int expectedCount = 0;
                    Assert.assertEquals(expectedCount, nrsFileMapping.getClustersCount());
                }

                // Write some hashes
                {
                    final byte[] hash = new byte[HASH_SIZE];
                    nextHash(hash);
                    nrsFileOpened.write(4, hash);
                    nextHash(hash);
                    nrsFileOpened.write(5, hash);
                    nextHash(hash);
                    nrsFileOpened.write(6, hash);
                    nextHash(hash);
                    nrsFileOpened.write(340, hash);
                    nextHash(hash);
                    nrsFileOpened.write(600, hash);
                    nextHash(hash);
                    nrsFileOpened.write(610, hash);
                }

                // Check filled file mapping
                {
                    final RemoteOperation.Builder builder = nrsFileOpened.getFileMapping(HashAlgorithm.MD5);

                    // Read file mapping
                    builder.setVersion(ProtocolVersion.VERSION_1);
                    builder.setType(Type.NRS);
                    builder.setOp(OpCode.LIST);
                    final RemoteOperation operation = builder.build();
                    Assert.assertTrue(operation.hasNrsFileMapping());
                    final NrsFileMapping nrsFileMapping = operation.getNrsFileMapping();

                    Assert.assertTrue(nrsFileMapping.hasClusterSize());
                    Assert.assertEquals(NrsClusterSizeConfigKey.getInstance().getTypedValue(config).intValue(),
                            nrsFileMapping.getClusterSize());
                    Assert.assertTrue(nrsFileMapping.hasVersion());
                    Assert.assertEquals(6L, nrsFileMapping.getVersion());

                    final int firstClusterIndex = 2;
                    final int expectedCount = 3;
                    Assert.assertEquals(expectedCount, nrsFileMapping.getClustersCount());
                    final List<NrsClusterHash> clusterHashs = nrsFileMapping.getClustersList();

                    final boolean[] clusterFound = new boolean[expectedCount + firstClusterIndex];
                    for (int i = 0; i < firstClusterIndex; i++) {
                        clusterFound[i] = true;
                    }
                    final String[] clusterHash = new String[] { "", "", "40a4a6dd4e0f36498055323ed25f303d669e",
                            "404f01bdf56128a2f95f3aa745eb63da1d9c", "409484418babb2e450663c5d81474960a778" };
                    for (final NrsClusterHash nrsClusterHash : clusterHashs) {
                        final int index = (int) nrsClusterHash.getIndex();
                        clusterFound[index] = true;
                        Assert.assertEquals(clusterHash[index],
                                Strings.toHexString(nrsClusterHash.getHash().toByteArray()));
                    }
                    // Check result
                    for (final boolean b : clusterFound) {
                        Assert.assertTrue(b);
                    }
                }
            }
            finally {
                janitor.unlockNrsFile(nrsFileOpened);
            }

        }
        finally {
            janitor.fini();
        }
    }

    private final void nextHash(final byte[] hash) {
        for (int i = 0; i < HASH_SIZE; i++) {
            hash[i] = (byte) ((i + hash[i] * 31) & 0xFF);
        }
    }
}
