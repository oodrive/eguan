package io.eguan.hash;

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

import io.eguan.hash.TigerDigest;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the Tiger digest.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 * 
 */
public class TestTigerDigest {

    private final String[][] TEST_VALUES = {
            // data, md
            // ......................
            { "", "3293AC630C13F0245F92BBB1766E16167A4E58492DDE73F3" },
            { "abc", "2AAB1484E8C158F2BFB8C5FF41B57A525129131C957B5F93" },
            { "Tiger", "DD00230799F5009FEC6DEBC838BB6A27DF2B9D6F110C7937" },
            { "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-",
                    "F71C8583902AFB879EDFE610F82C0D4786A3A534504486B5" },
            { "ABCDEFGHIJKLMNOPQRSTUVWXYZ=abcdefghijklmnopqrstuvwxyz+0123456789",
                    "48CEEB6308B87D46E95D656112CDF18D97915F9765658957" },
            { "Tiger - A Fast New Hash Function, by Ross Anderson and Eli Biham",
                    "8A866829040A410C729AD23F5ADA711603B3CDD357E4C15E" },
            {
                    "Tiger - A Fast New Hash Function, by Ross Anderson and Eli Biham, "
                            + "proceedings of Fast Software Encryption 3, Cambridge.",
                    "CE55A6AFD591F5EBAC547FF84F89227F9331DAB0B611C889" },
            {
                    "Tiger - A Fast New Hash Function, by Ross Anderson and Eli Biham, "
                            + "proceedings of Fast Software Encryption 3, Cambridge, 1996.",
                    "631ABDD103EB9A3D245B6DFD4D77B257FC7439501D1568DD" },
            {
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-"
                            + "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-",
                    "C54034E5B43EB8005848A7E0AE6AAC76E4FF590AE715FD25" } };

    @Test
    public void testTiger() {
        for (int i = 0; i < TEST_VALUES.length; i++) {
            final String[] testVal = TEST_VALUES[i];
            final byte[] src = testVal[0].getBytes();
            final byte[] digBouncy = digestBouncy(src, 0, src.length);
            final byte[] digLocal = digestLocal(src, 0, src.length);
            final StringBuilder hexString = new StringBuilder(digBouncy.length * 2);
            for (int j = 0; j < digBouncy.length; j++) {
                hexString.append(String.format("%02X", Byte.valueOf(digBouncy[j])));
            }
            Assert.assertEquals(testVal[1], hexString.toString());
            Assert.assertTrue(Arrays.equals(digBouncy, digLocal));
        }
    }

    @Test
    public void testTiger4k() {
        final byte[] src = new byte[4096];
        new SecureRandom().nextBytes(src);
        for (int i = 0; i <= 4096; i++) {
            for (int j = 0; j < i; j += 30) {
                final byte[] digBouncy = digestBouncy(src, j, i);
                final byte[] digLocal = digestLocal(src, j, i);
                Assert.assertTrue(Arrays.equals(digBouncy, digLocal));
            }
        }
    }

    private byte[] digestBouncy(final byte[] src, final int offsetStart, final int offsetEnd) {
        final org.bouncycastle.crypto.digests.TigerDigest digest = new org.bouncycastle.crypto.digests.TigerDigest();
        digest.update(src, offsetStart, offsetEnd - offsetStart);
        final byte[] result = new byte[24];
        digest.doFinal(result, 0);
        return result;
    }

    private byte[] digestLocal(final byte[] src, final int offsetStart, final int offsetEnd) {
        // Do not call ByteBuffer.wrap(src, offset, len) to check that digest will start from position and not cross the
        // limit
        final ByteBuffer buffer = ByteBuffer.wrap(src);
        buffer.position(offsetStart).limit(offsetEnd);

        final TigerDigest digest = new TigerDigest(buffer);
        final byte[] result = new byte[digest.getDigestSize()];
        digest.doFinal(result, 0);
        return result;
    }

    @Test
    public void testTigerPerf() {
        testTigerPerf(true);
    }

    /**
     * Real world usage of local hash function. Base code for a performance comparison campaign.
     * 
     * @param callAssert
     */
    private void testTigerPerf(final boolean callAssert) {
        // Prepare buffer
        final int len = 4096;
        final int loopCount = 40960;

        final byte[] src = new byte[len];
        new SecureRandom().nextBytes(src);
        final ByteBuffer bufBouncy = ByteBuffer.allocateDirect(len);
        bufBouncy.put(src).rewind();
        final ByteBuffer bufLocal = ByteBuffer.allocateDirect(len);
        bufLocal.put(src).rewind();

        // Count bouncy castle implementation time
        final byte[] digBouncy = new byte[24];
        {
            final long startBouncy = System.nanoTime();
            for (int i = 0; i < loopCount; i++) {
                final org.bouncycastle.crypto.digests.TigerDigest digest = new org.bouncycastle.crypto.digests.TigerDigest();
                // Get data from ByteBuffer
                final byte[] blockCopy = new byte[len];
                bufBouncy.position(0);
                bufBouncy.limit(len);
                bufBouncy.get(blockCopy);
                digest.update(blockCopy, 0, len);
                digest.doFinal(digBouncy, 0);

                // Avoid loop suppression: use value
                bufBouncy.rewind();
                bufBouncy.put(digBouncy[12]);
                // System.out.print(" " + digBouncy[12]);
            }
            final long endBouncy = System.nanoTime();
            System.out.println("Bouncy Castle impl "
                    + ((float) loopCount / ((float) (endBouncy - startBouncy)) * 1000000000) + " hash/seconds");
        }

        // Count local implementation time
        final byte[] digLocal = new byte[24];
        final long startLocal = System.nanoTime();
        for (int i = 0; i < loopCount; i++) {
            final TigerDigest digest = new TigerDigest(bufLocal);
            digest.doFinal(digLocal, 0);

            // Avoid loop suppression: use value
            bufLocal.rewind();
            bufLocal.put(digLocal[12]).rewind();
            // System.out.print(" " + digLocal[12]);
        }
        final long endLocal = System.nanoTime();
        System.out.println("Local impl         " + ((float) loopCount / ((float) (endLocal - startLocal)) * 1000000000)
                + " hash/seconds");

        if (callAssert) {
            Assert.assertTrue(Arrays.equals(digBouncy, digLocal));
        }
    }

    /**
     * Call perf test.
     * 
     * @param ignored
     */
    public static void main(final String[] ignored) {
        final TestTigerDigest t = new TestTigerDigest();
        t.testTiger();
        t.testTigerPerf(false);
    }
}
