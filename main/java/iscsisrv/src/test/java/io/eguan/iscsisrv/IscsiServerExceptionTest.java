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

import io.eguan.iscsisrv.IscsiDevice;
import io.eguan.iscsisrv.IscsiServer;
import io.eguan.iscsisrv.IscsiTarget;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.jscsi.initiator.Configuration;
import org.jscsi.initiator.Initiator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test error on read and write request. When an initiator requests read or write on a jSCSI target, if an error
 * occurred on the Target side, the Target does not handle the error properly and breaks the connection
 * 
 * TODO: Fix read request and finish test TODO: Add a test with a write request with a failing write operation on the
 * device
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@RunWith(value = Parameterized.class)
public class IscsiServerExceptionTest {

    /** TargetName (server) for the initiator (see unit test initiator config file) */
    private static String TARGET_NAME = "test-eguan";
    /** TargetName (device) for the initiator (see unit test initiator config file) */
    private static String TARGET_DEVICE_IQN = "iqn.2000-06.com.oodrive:test-target";

    private static int BLOCK_SIZE = 512;
    private static int BLOCK_COUNT = 1024;

    private static int FAIL_VAL_IO_EXCEPTION = 2 * BLOCK_SIZE;
    private static int FAIL_VAL_RUNTIME_EXCEPTION = 4 * BLOCK_SIZE;

    private final int testFailValue;

    IscsiServer server;

    final class TestIscsiDevice implements IscsiDevice {

        @Override
        public void close() throws IOException {
            // NOP
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public long getSize() {
            return BLOCK_COUNT * BLOCK_SIZE;
        }

        @Override
        public int getBlockSize() {
            return BLOCK_SIZE;
        }

        @Override
        public void read(final ByteBuffer bytes, final int length, final long storageIndex) throws IOException {
            if (storageIndex == FAIL_VAL_IO_EXCEPTION) {
                throw new IOException("Test IOException");
            }
            else if (storageIndex == FAIL_VAL_RUNTIME_EXCEPTION) {
                throw new RuntimeException("Test IOException");
            }
        }

        @Override
        public void write(final ByteBuffer bytes, final int length, final long storageIndex) throws IOException {

        }

    }

    public IscsiServerExceptionTest(final Integer param) {
        this.testFailValue = param.intValue();
    }

    @Parameters
    public static Collection<Object[]> data() {
        final Object[][] data = new Object[][] { { Integer.valueOf(FAIL_VAL_IO_EXCEPTION) },
                { Integer.valueOf(FAIL_VAL_RUNTIME_EXCEPTION) } };
        return Arrays.asList(data);
    }

    @Before
    public void initServerMBean() throws Exception {
        server = new IscsiServer(InetAddress.getLoopbackAddress());
    }

    @Test
    public void testTargetReadDevice() throws Exception {
        Assert.assertFalse(server.isStarted());

        // create a mock on the device
        final IscsiDevice mIscsiDevice = new TestIscsiDevice();

        // Add target device
        final IscsiTarget target = IscsiTarget.newIscsiTarget(TARGET_DEVICE_IQN, null, mIscsiDevice);
        Assert.assertNull(server.addTarget(target));

        server.start();
        try {
            Assert.assertTrue(server.isStarted());

            final Configuration configuration = Configuration.create(Initiator.class.getResource("/jscsi.xsd"),
                    getClass().getResource("/iscsisrv-tst-config.xml"));
            final Initiator initiator = new Initiator(configuration);

            boolean closeSession = false;

            initiator.createSession(TARGET_NAME);
            try {
                closeSession = true;

                final ByteBuffer dst = ByteBuffer.wrap(new byte[BLOCK_SIZE]);
                final int logicalBlockAddress = testFailValue / BLOCK_SIZE;
                final long transferLength = BLOCK_SIZE;

                initiator.read(TARGET_NAME, dst, logicalBlockAddress, transferLength);

            }
            catch (final Throwable t) {
                closeSession = false;
                // TODO: fail if an exception is thrown
                // AssertionFailedError afe = new AssertionFailedError("Unexpected exception");
                // afe.initCause(t);
                // throw afe;
            }
            finally {
                if (closeSession) {
                    initiator.closeSession(TARGET_NAME);
                }
            }
            Assert.assertSame(target, server.removeTarget(TARGET_DEVICE_IQN));
            Assert.assertNull(server.removeTarget(TARGET_DEVICE_IQN));
        }
        finally {
            server.stop();
        }
    }
}
