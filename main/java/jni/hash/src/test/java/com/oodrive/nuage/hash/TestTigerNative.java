package com.oodrive.nuage.hash;

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

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

/**
 * Unit tests of the {@link TigerNative} hash algorithm.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestTigerNative {

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
            final ByteBuffer src = ByteBuffer.wrap(testVal[0].getBytes());
            final byte[] digLocal = TigerNative.hashToByteArray(src);
            final StringBuilder hexString = new StringBuilder(digLocal.length * 2);
            for (int j = 0; j < digLocal.length; j++) {
                hexString.append(String.format("%02X", Byte.valueOf(digLocal[j])));
            }
            Assert.assertEquals("i=" + i, testVal[1], hexString.toString());
        }
    }

    @Test
    public void testTigerByteString() throws NoSuchAlgorithmException {
        for (int i = 0; i < TEST_VALUES.length; i++) {
            final String[] testVal = TEST_VALUES[i];
            final ByteString byteString = ByteString.copyFrom(testVal[0].getBytes());
            final byte[] digLocal = TigerNative.hashToByteArray(byteString);
            final StringBuilder hexString = new StringBuilder(digLocal.length * 2);
            for (int j = 0; j < digLocal.length; j++) {
                hexString.append(String.format("%02X", Byte.valueOf(digLocal[j])));
            }
            Assert.assertEquals("i=" + i, testVal[1], hexString.toString());
        }
    }

    /**
     * Real world usage of local hash function. Base code for a performance comparison campaign.
     * 
     */
    @Test
    public void testTigerPerf() {
        // Prepare buffer
        final int len = 4096;
        final int loopCount = 40960;

        final byte[] src = new byte[len];
        new SecureRandom().nextBytes(src);
        final ByteBuffer bufLocal = ByteBuffer.allocateDirect(len);
        bufLocal.put(src).rewind();

        // Count local implementation time
        final ByteBuffer digLocal = ByteBuffer.allocateDirect(24);
        final long startLocal = System.nanoTime();
        for (int i = 0; i < loopCount; i++) {
            TigerNative.hash(bufLocal, digLocal);
            digLocal.clear();
        }
        final long endLocal = System.nanoTime();
        System.out.println("Local impl         " + ((float) loopCount / ((float) (endLocal - startLocal)) * 1000000000)
                + " hash/seconds");
    }

    /**
     * Call perf test.
     * 
     * @param ignored
     */
    public static void main(final String[] ignored) {
        final TestTigerNative t = new TestTigerNative();
        t.testTiger();
        t.testTigerPerf();
    }
}
