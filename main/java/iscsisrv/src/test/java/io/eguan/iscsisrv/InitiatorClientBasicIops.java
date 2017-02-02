package io.eguan.iscsisrv;

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

import io.eguan.srv.ClientBasicIops;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.jscsi.exception.ConfigurationException;
import org.jscsi.exception.NoSuchSessionException;
import org.jscsi.exception.TaskExecutionException;
import org.jscsi.initiator.Configuration;
import org.jscsi.initiator.Initiator;
import org.junit.Assert;

public class InitiatorClientBasicIops implements ClientBasicIops {

    private final Initiator initiator;

    public InitiatorClientBasicIops(final String xsdFile, final URL xmlFile) {
        // jSCSI
        final Configuration configuration;
        try {
            configuration = Configuration.create(Initiator.class.getResource(xsdFile), xmlFile);
        }
        catch (final ConfigurationException e) {
            throw new RuntimeException(e);
        }
        initiator = new Initiator(configuration);
    }

    @Override
    public void write(final String targetName, final ByteBuffer src, final int logicalBlockAddress,
            final long transferLength, final int blockSize) throws NoSuchSessionException, TaskExecutionException {
        // blockSize parameter is useless for jScsi
        initiator.write(targetName, src, logicalBlockAddress, transferLength);
    }

    public Future<Void> multiThreadedWrite(final String targetName, final ByteBuffer src,
            final int logicalBlockAddress, final long transferLength, final int blockSize)
            throws NoSuchSessionException, TaskExecutionException {
        return initiator.multiThreadedWrite(targetName, src, logicalBlockAddress, transferLength);
    }

    @Override
    public void read(final String targetName, final ByteBuffer dst, final int logicalBlockAddress,
            final long transferLength, final int blockSize) throws NoSuchSessionException, TaskExecutionException {
        // blockSize parameter is useless for jScsi
        initiator.read(targetName, dst, logicalBlockAddress, transferLength);
    }

    @Override
    public void createSession(final String targetName) throws NoSuchSessionException {
        initiator.createSession(targetName);
    }

    @Override
    public void closeSession(final String targetName) throws NoSuchSessionException, TaskExecutionException {
        initiator.closeSession(targetName);

    }

    @Override
    public void checkCapacity(final String target, final long size) throws NoSuchSessionException {

        /* Initiator.getCapacity() == last block address */
        final long capacity = initiator.getCapacity(target);
        final long blockSize = initiator.getBlockSize(target);
        Assert.assertEquals("Error: capacity=" + capacity + ", blockSize=" + blockSize, size, (capacity + 1)
                * blockSize);
    }

}
