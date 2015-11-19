package io.eguan.nrs;

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

import io.eguan.configuration.MetaConfiguration;
import io.eguan.nrs.NrsClusterSizeConfigKey;
import io.eguan.nrs.NrsException;
import io.eguan.nrs.NrsFile;
import io.eguan.nrs.NrsFileFlag;
import io.eguan.nrs.NrsFileHeader;
import io.eguan.nrs.NrsFileJanitor;
import io.eguan.utils.SimpleIdentifierProvider;
import io.eguan.utils.UuidT;

import java.util.UUID;

import org.junit.Assert;

/**
 * Unit tests for {@link NrsFileJanitor}.
 *
 * @author oodrive
 * @author llambert
 *
 */
public abstract class TestNrsFileJanitorAbstract extends AbstractNrsTestFixture {

    protected static final int HASH_SIZE = 123;
    protected UuidT<NrsFile> parent;
    protected UUID device;
    protected UUID node;
    protected UuidT<NrsFile> file;
    protected int hashSize;
    protected int blockSize;
    protected long size;
    protected long now;

    protected final NrsFile createTestNrsFile(final NrsFileJanitor janitor, final MetaConfiguration config,
            final boolean flagBlock) throws NrsException {
        return createTestNrsFile(janitor, config, true, false, flagBlock);
    }

    protected final NrsFile createTestNrsFile(final NrsFileJanitor janitor, final MetaConfiguration config,
            final boolean random, final boolean flagRoot, final boolean flagBlock) throws NrsException {
        // Check cluster size
        final int clusterSize = janitor.newNrsFileHeaderBuilder().clusterSize();
        Assert.assertEquals(NrsClusterSizeConfigKey.getInstance().getTypedValue(config), Integer.valueOf(clusterSize));

        if (random) {
            parent = SimpleIdentifierProvider.newId();
        }
        else {
            parent = SimpleIdentifierProvider.fromString("1e21db8e-fe9c-11e2-b701-180373e17308");
        }

        device = random ? UUID.randomUUID() : UUID.fromString("3d34a36c-feab-11f2-a050-180373e17308");
        node = random ? UUID.randomUUID() : UUID.fromString("37307734-fa9b-12e2-9dee-180373e17308");
        if (random) {
            file = SimpleIdentifierProvider.newId();
        }
        else {
            file = SimpleIdentifierProvider.fromString("3189416c-ff9b-41e2-98d9-180373e17308");
        }

        hashSize = HASH_SIZE;
        blockSize = 4000;
        size = flagRoot ? 0L : blockSize * 56370;
        now = random ? System.currentTimeMillis() : 1265400333;

        final NrsFileHeader.Builder<NrsFile> headerBuilder = janitor.newNrsFileHeaderBuilder();
        headerBuilder.parent(parent);
        headerBuilder.device(device);
        headerBuilder.node(node);
        headerBuilder.file(file);
        headerBuilder.size(size);
        headerBuilder.blockSize(blockSize);
        headerBuilder.hashSize(hashSize);
        headerBuilder.timestamp(now);
        headerBuilder.addFlags(flagRoot ? NrsFileFlag.ROOT : NrsFileFlag.PARTIAL);
        if (flagBlock) {
            headerBuilder.addFlags(NrsFileFlag.BLOCKS);
        }

        final NrsFileHeader<NrsFile> header = headerBuilder.build();
        return janitor.createNrsFile(header);
    }

}
