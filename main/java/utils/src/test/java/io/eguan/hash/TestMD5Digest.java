package io.eguan.hash;

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

import io.eguan.hash.MD5Digest;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the MD5 digest.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestMD5Digest {

    @Test
    public void testMD54k() throws NoSuchAlgorithmException {
        final byte[] src = new byte[4096];
        new SecureRandom().nextBytes(src);
        for (int i = 0; i <= 4096; i++) {
            for (int j = 0; j < i; j += 30) {
                final byte[] digBouncy = digestBouncy(src, j, i);
                final byte[] digLocal = digestLocal(src, j, i);
                final byte[] digJvm = digestJvm(src, j, i);
                Assert.assertTrue(Arrays.equals(digBouncy, digLocal));
                Assert.assertTrue(Arrays.equals(digJvm, digLocal));
            }
        }
    }

    private byte[] digestBouncy(final byte[] src, final int offsetStart, final int offsetEnd) {
        final org.bouncycastle.crypto.digests.MD5Digest digest = new org.bouncycastle.crypto.digests.MD5Digest();
        digest.update(src, offsetStart, offsetEnd - offsetStart);
        final byte[] result = new byte[digest.getDigestSize()];
        digest.doFinal(result, 0);
        return result;
    }

    private byte[] digestLocal(final byte[] src, final int offsetStart, final int offsetEnd) {
        // Do not call ByteBuffer.wrap(src, offset, len) to check that digest will start from position and not cross the
        // limit
        final ByteBuffer buffer = ByteBuffer.wrap(src);
        buffer.position(offsetStart).limit(offsetEnd);

        final MD5Digest digest = new MD5Digest(buffer);
        final byte[] result = new byte[digest.getDigestSize()];
        digest.doFinal(result, 0);
        return result;
    }

    private byte[] digestJvm(final byte[] src, final int offsetStart, final int offsetEnd)
            throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(src, offsetStart, offsetEnd - offsetStart);
        return digest.digest();
    }

    @Test
    public void testMD5Perf() throws Exception {
        testMD5Perf(true);
    }

    /**
     * Real world usage of local hash function. Base code for a performance comparison campaign.
     * 
     * @param callAssert
     * @throws DigestException
     * @throws NoSuchAlgorithmException
     */
    private void testMD5Perf(final boolean callAssert) throws DigestException, NoSuchAlgorithmException {
        // Prepare buffer
        final int len = 4096;
        final int loopCount = 40960;

        final byte[] src = new byte[len];
        new SecureRandom().nextBytes(src);
        final ByteBuffer bufJvm = ByteBuffer.allocateDirect(len);
        bufJvm.put(src).rewind();
        final ByteBuffer bufBouncy = ByteBuffer.allocateDirect(len);
        bufBouncy.put(src).rewind();
        final ByteBuffer bufLocal = ByteBuffer.allocateDirect(len);
        bufLocal.put(src).rewind();

        // Count JVM implementation time
        final byte[] digJvm = new byte[16];
        {
            final long startJvm = System.nanoTime();
            for (int i = 0; i < loopCount; i++) {
                final MessageDigest digest = MessageDigest.getInstance("MD5");
                // Get data from ByteBuffer
                final byte[] blockCopy = new byte[len];
                bufJvm.position(0);
                bufJvm.limit(len);
                bufJvm.get(blockCopy);
                digest.update(blockCopy, 0, len);
                digest.digest(digJvm, 0, digJvm.length);

                // Avoid loop suppression: use value
                bufJvm.rewind();
                bufJvm.put(digJvm[12]);
                // System.out.print(" " + digJvm[12]);
            }
            final long endJvm = System.nanoTime();
            System.out.println("Jvm           impl " + ((float) loopCount / ((float) (endJvm - startJvm)) * 1000000000)
                    + " hash/seconds");
        }

        // Count bouncy castle implementation time
        final byte[] digBouncy = new byte[16];
        {
            final long startBouncy = System.nanoTime();
            for (int i = 0; i < loopCount; i++) {
                final org.bouncycastle.crypto.digests.MD5Digest digest = new org.bouncycastle.crypto.digests.MD5Digest();
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
        final byte[] digLocal = new byte[16];
        final long startLocal = System.nanoTime();
        for (int i = 0; i < loopCount; i++) {
            final MD5Digest digest = new MD5Digest(bufLocal);
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
            Assert.assertTrue(Arrays.equals(digJvm, digLocal));
        }
    }

    /**
     * Call perf test.
     * 
     * @param ignored
     */
    public static void main(final String[] ignored) {
        try {
            new TestMD5Digest().testMD5Perf(false);
        }
        catch (DigestException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
