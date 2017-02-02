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

import io.eguan.hash.SHA1Native;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestSHA1Native {

    @Test
    public void testSHA14k() throws NoSuchAlgorithmException {
        final byte[] src = new byte[4096];
        new SecureRandom().nextBytes(src);
        for (int i = 0; i <= 4096; i++) {
            for (int j = 0; j < i; j += 30) {
                final byte[] digLocalDirect = digestLocalDirect(src, j, i);
                final byte[] digLocalWrap = digestLocalWrap(src, j, i);
                final byte[] digJvm = digestJvm(src, j, i);
                Assert.assertTrue(Arrays.equals(digJvm, digLocalDirect));
                Assert.assertTrue(Arrays.equals(digJvm, digLocalWrap));
            }
        }
    }

    private byte[] digestLocalDirect(final byte[] src, final int offsetStart, final int offsetEnd) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(src.length);
        buffer.put(src).clear();
        buffer.position(offsetStart).limit(offsetEnd);

        return SHA1Native.hashToByteArray(buffer);
    }

    private byte[] digestLocalWrap(final byte[] src, final int offsetStart, final int offsetEnd) {
        final ByteBuffer buffer = ByteBuffer.wrap(src);
        buffer.position(offsetStart).limit(offsetEnd);

        return SHA1Native.hashToByteArray(buffer);
    }

    private byte[] digestJvm(final byte[] src, final int offsetStart, final int offsetEnd)
            throws NoSuchAlgorithmException {
        final MessageDigest digest = MessageDigest.getInstance("SHA-1");
        digest.update(src, offsetStart, offsetEnd - offsetStart);
        return digest.digest();
    }

    @Test
    public void testSHA1ByteString() throws NoSuchAlgorithmException {
        final byte[] src = new byte[4096];
        new SecureRandom().nextBytes(src);

        // SHA1 sum JVM
        final byte[] digJvm = digestJvm(src, 0, src.length);

        // SHA1 sum native ByteString
        final ByteString byteString = ByteString.copyFrom(src);
        final byte[] digLocal = SHA1Native.hashToByteArray(byteString);

        // Compare
        Assert.assertTrue(Arrays.equals(digJvm, digLocal));
    }

    @Test
    public void testSHA1Perf() throws Exception {
        testSHA1Perf(true);
    }

    /**
     * Real world usage of local hash function. Base code for a performance comparison campaign.
     * 
     * @param callAssert
     * @throws DigestException
     * @throws NoSuchAlgorithmException
     */
    private void testSHA1Perf(final boolean callAssert) throws DigestException, NoSuchAlgorithmException {
        // Prepare buffer
        final int len = 4096;
        final int loopCount = 40960;

        final byte[] src = new byte[len];
        new SecureRandom().nextBytes(src);
        final ByteBuffer bufJvm = ByteBuffer.allocateDirect(len);
        bufJvm.put(src).rewind();
        final ByteBuffer bufLocal = ByteBuffer.allocateDirect(len);
        bufLocal.put(src).rewind();

        // Count JVM implementation time
        final byte[] digJvm = new byte[20];
        {
            final long startJvm = System.nanoTime();
            for (int i = 0; i < loopCount; i++) {
                final MessageDigest digest = MessageDigest.getInstance("SHA-1");
                // Get data from ByteBuffer
                final byte[] blockCopy = new byte[len];
                bufJvm.position(0);
                bufJvm.limit(len);
                bufJvm.get(blockCopy);
                digest.update(blockCopy, 0, len);
                digest.digest(digJvm, 0, digJvm.length);
                bufJvm.rewind();
            }
            final long endJvm = System.nanoTime();
            System.out.println("Jvm           impl " + ((float) loopCount / ((float) (endJvm - startJvm)) * 1000000000)
                    + " hash/seconds");
        }

        // Count local implementation time
        final ByteBuffer digLocal = ByteBuffer.allocateDirect(20);
        final long startLocal = System.nanoTime();
        for (int i = 0; i < loopCount; i++) {
            SHA1Native.hash(bufLocal, digLocal);
            digLocal.rewind();
        }
        final long endLocal = System.nanoTime();
        System.out.println("Local impl         " + ((float) loopCount / ((float) (endLocal - startLocal)) * 1000000000)
                + " hash/seconds");

        if (callAssert) {
            final byte[] digLocalArray = new byte[20];
            digLocal.get(digLocalArray);
            Assert.assertTrue(Arrays.equals(digJvm, digLocalArray));
        }
    }

    /**
     * Call perf test.
     * 
     * @param ignored
     */
    public static void main(final String[] ignored) {
        try {
            new TestSHA1Native().testSHA1Perf(false);
        }
        catch (DigestException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }
}
