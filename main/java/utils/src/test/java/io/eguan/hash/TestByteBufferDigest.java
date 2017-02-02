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

import io.eguan.hash.ByteBufferDigest;
import io.eguan.hash.HashAlgorithm;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;

/**
 * Unit tests for {@link ByteBufferDigest}.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public class TestByteBufferDigest {

    private static final String TEXT = "The quick brown fox jumps over the lazy dog !";
    private static final ByteBuffer REF;
    static {
        final byte[] text = TEXT.getBytes();
        REF = ByteBuffer.allocateDirect(text.length);
        REF.put(text);
        Assert.assertEquals(REF.capacity(), REF.position());
    }

    /** Reference of full hash for MD5 */
    private static final byte[] MD5_REF_FULL = new byte[] { 64, -127, 66, -23, 71, 121, -19, 64, 73, 56, -47, 51, -110,
            60, -75, -47, -84, 12 };
    /** Reference of partial hash for MD5 */
    private static final byte[] MD5_REF_PART = new byte[] { 64, -108, -110, -108, 102, -84, 69, 102, 10, 82, -29, 66,
            -65, 50, -69, 20, 35, -61 };

    /** Reference of full hash for Tiger */
    private static final byte[] TIGER_REF_FULL = new byte[] { 65, 0, -87, -75, -118, 21, 39, -117, -21, -13, -19, 112,
            -104, 23, 54, -78, -84, 63, -16, -96, 122, 18, -30, 7, 14, -96 };
    /** Reference of partial hash for Tiger */
    private static final byte[] TIGER_REF_PART = new byte[] { 65, -35, -11, -35, 111, -10, 88, 15, 14, 122, 4, -113,
            -40, 83, -16, 123, 55, 107, -63, 53, 39, 98, 32, -117, -91, 102 };

    /** Reference of full hash for SHA-1 */
    private static final byte[] SHA1_REF_FULL = new byte[] { 66, 7, -63, 118, 27, -20, 23, -31, -107, -53, -115, -115,
            -74, -70, 74, 53, 106, -10, -28, -106, 70, 118 };
    /** Reference of partial hash for SHA-1 */
    private static final byte[] SHA1_REF_PART = new byte[] { 66, 55, -61, -62, -118, 57, 93, 84, -18, -87, 55, 64, -79,
            23, 67, -127, 95, 90, 118, 111, 84, -46 };

    /** Reference of full hash for SHA-256 */
    private static final byte[] SHA256_REF_FULL = new byte[] { 67, -48, 25, 72, -106, 97, 117, 105, -1, -39, 66, -87,
            8, 9, 81, -6, -26, -93, 4, 8, -10, -73, 118, -5, 119, 115, -32, -2, 1, 23, -59, -95, -116, 47 };
    /** Reference of partial hash for SHA-256 */
    private static final byte[] SHA256_REF_PART = new byte[] { 67, 41, 126, 72, -7, 107, -38, -42, -44, -112, 23, -85,
            13, -14, 74, 9, -94, 11, -83, -14, 11, -107, -61, 18, -90, 53, -108, -8, 90, 48, 44, -118, -84, -96 };

    /** Reference of full hash for SHA-512 */
    private static final byte[] SHA512_REF_FULL = new byte[] { 68, -116, -64, -49, -12, -66, 106, -43, 53, 100, 90,
            121, -33, 16, -107, -61, 67, 35, 77, -8, 45, 44, -10, -41, -66, -40, 106, -117, 101, -43, 12, -46, 82, -41,
            -51, 65, -68, -94, 0, 52, 1, -20, 76, -119, 112, -110, -44, -95, -66, 12, -76, -64, 37, -126, 7, -42, -36,
            32, 41, 25, 39, 50, -54, 85, -70, 21 };
    /** Reference of partial hash for SHA-512 */
    private static final byte[] SHA512_REF_PART = new byte[] { 68, -123, -2, -48, 115, -39, -11, 113, -41, 72, 94, -29,
            -24, -59, -100, 34, -128, -86, -33, 12, 68, 91, -93, 75, -8, -45, 73, -78, -5, -11, 45, -48, 36, 74, 40,
            117, -119, -43, -15, 63, -36, 112, 32, -94, -67, -5, -35, -75, -107, 17, -11, -22, 21, -71, -11, -110, -47,
            -57, -65, 11, -6, 28, 56, 12, -112, 76 };

    /**
     * Prepare buffer.
     */
    @Before
    public void rewind() {
        REF.clear();
    }

    @Test
    public void testMD5() throws NoSuchAlgorithmException, IllegalArgumentException {
        // Check digest len
        Assert.assertEquals(HashAlgorithm.MD5.getStandardDigestLength(), MD5_REF_FULL.length - 2);
        Assert.assertEquals(HashAlgorithm.MD5.getPersistedDigestLength(), MD5_REF_FULL.length);

        { // Full digest
            final byte[] md5Full = ByteBufferDigest.digest(HashAlgorithm.MD5, REF);
            Assert.assertTrue(Arrays.equals(MD5_REF_FULL, md5Full));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, md5Full));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
        }

        { // Full digest - ByteString
            final byte[] md5Full = ByteBufferDigest.digest(HashAlgorithm.MD5, ByteString.copyFrom(REF));
            REF.clear();
            Assert.assertTrue(Arrays.equals(MD5_REF_FULL, md5Full));
            Assert.assertTrue(ByteBufferDigest.match(ByteString.copyFrom(REF), md5Full));
            REF.clear();
        }

        {// Partial digest
            REF.position(3).limit(7);
            final byte[] md5Part = ByteBufferDigest.digest(HashAlgorithm.MD5, REF);
            Assert.assertTrue(Arrays.equals(MD5_REF_PART, md5Part));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, md5Part));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
        }
    }

    @Test
    public void testMD5Check() throws NoSuchAlgorithmException {
        // Truncate reference
        final byte[] truncated = Arrays.copyOf(MD5_REF_FULL, 7);
        Assert.assertFalse(HashAlgorithm.MD5.checkHash(truncated));
        // Break trailer
        final byte[] trailer = Arrays.copyOf(MD5_REF_FULL, MD5_REF_FULL.length);
        trailer[trailer.length - 1]++;
        Assert.assertFalse(HashAlgorithm.MD5.checkHash(trailer));
        // Change algo
        final byte[] algo = Arrays.copyOf(MD5_REF_FULL, MD5_REF_FULL.length);
        algo[0]++;
        Assert.assertFalse(HashAlgorithm.MD5.checkHash(algo));
        // Change version
        final byte[] version = Arrays.copyOf(MD5_REF_FULL, MD5_REF_FULL.length);
        version[0] |= 0xC0;
        Assert.assertFalse(HashAlgorithm.MD5.checkHash(version));
    }

    @Test
    public void testTiger() throws NoSuchAlgorithmException, IllegalArgumentException {
        // Check digest len
        Assert.assertEquals(HashAlgorithm.TIGER.getStandardDigestLength(), TIGER_REF_FULL.length - 2);
        Assert.assertEquals(HashAlgorithm.TIGER.getPersistedDigestLength(), TIGER_REF_FULL.length);

        { // Full digest
            final byte[] tigerFull = ByteBufferDigest.digest(HashAlgorithm.TIGER, REF);
            Assert.assertTrue(Arrays.equals(TIGER_REF_FULL, tigerFull));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, tigerFull));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
        }

        { // Full digest - ByteString
            final byte[] tigerFull = ByteBufferDigest.digest(HashAlgorithm.TIGER, ByteString.copyFrom(REF));
            REF.clear();
            Assert.assertTrue(Arrays.equals(TIGER_REF_FULL, tigerFull));
            Assert.assertTrue(ByteBufferDigest.match(ByteString.copyFrom(REF), tigerFull));
            REF.clear();
        }

        {// Partial digest
            REF.position(3).limit(7);
            final byte[] tigerPart = ByteBufferDigest.digest(HashAlgorithm.TIGER, REF);
            Assert.assertTrue(Arrays.equals(TIGER_REF_PART, tigerPart));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, tigerPart));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
        }
    }

    @Test
    public void testTigerCheck() throws NoSuchAlgorithmException {
        // Truncate reference
        final byte[] truncated = Arrays.copyOf(TIGER_REF_FULL, 7);
        Assert.assertFalse(HashAlgorithm.TIGER.checkHash(truncated));
        // Break trailer
        final byte[] trailer = Arrays.copyOf(TIGER_REF_FULL, TIGER_REF_FULL.length);
        trailer[trailer.length - 1]++;
        Assert.assertFalse(HashAlgorithm.TIGER.checkHash(trailer));
        // Change algo
        final byte[] algo = Arrays.copyOf(TIGER_REF_FULL, TIGER_REF_FULL.length);
        algo[0]++;
        Assert.assertFalse(HashAlgorithm.TIGER.checkHash(algo));
        // Change version
        final byte[] version = Arrays.copyOf(TIGER_REF_FULL, TIGER_REF_FULL.length);
        version[0] |= 0xC0;
        Assert.assertFalse(HashAlgorithm.TIGER.checkHash(version));
    }

    @Test
    public void testSHA1() throws NoSuchAlgorithmException, IllegalArgumentException {
        // Check digest len
        Assert.assertEquals(HashAlgorithm.SHA1.getStandardDigestLength(), SHA1_REF_FULL.length - 2);
        Assert.assertEquals(HashAlgorithm.SHA1.getPersistedDigestLength(), SHA1_REF_FULL.length);

        { // Full digest
            final byte[] sha1Full = ByteBufferDigest.digest(HashAlgorithm.SHA1, REF);
            Assert.assertTrue(Arrays.equals(SHA1_REF_FULL, sha1Full));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, sha1Full));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
        }

        { // Full digest - ByteString
            final byte[] sha1Full = ByteBufferDigest.digest(HashAlgorithm.SHA1, ByteString.copyFrom(REF));
            REF.clear();
            Assert.assertTrue(Arrays.equals(SHA1_REF_FULL, sha1Full));
            Assert.assertTrue(ByteBufferDigest.match(ByteString.copyFrom(REF), sha1Full));
            REF.clear();
        }

        {// Partial digest
            REF.position(3).limit(7);
            final byte[] sha1Part = ByteBufferDigest.digest(HashAlgorithm.SHA1, REF);
            Assert.assertTrue(Arrays.equals(SHA1_REF_PART, sha1Part));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, sha1Part));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
        }
    }

    @Test
    public void testSHA1Check() throws NoSuchAlgorithmException {
        // Truncate reference
        final byte[] truncated = Arrays.copyOf(SHA1_REF_FULL, 7);
        Assert.assertFalse(HashAlgorithm.SHA1.checkHash(truncated));
        // Break trailer
        final byte[] trailer = Arrays.copyOf(SHA1_REF_FULL, SHA1_REF_FULL.length);
        trailer[trailer.length - 1]++;
        Assert.assertFalse(HashAlgorithm.SHA1.checkHash(trailer));
        // Change algo
        final byte[] algo = Arrays.copyOf(SHA1_REF_FULL, SHA1_REF_FULL.length);
        algo[0]++;
        Assert.assertFalse(HashAlgorithm.SHA1.checkHash(algo));
        // Change version
        final byte[] version = Arrays.copyOf(SHA1_REF_FULL, SHA1_REF_FULL.length);
        version[0] |= 0xC0;
        Assert.assertFalse(HashAlgorithm.SHA1.checkHash(version));
    }

    @Test
    public void testSHA256() throws NoSuchAlgorithmException, IllegalArgumentException {
        // Check digest len
        Assert.assertEquals(HashAlgorithm.SHA256.getStandardDigestLength(), SHA256_REF_FULL.length - 2);
        Assert.assertEquals(HashAlgorithm.SHA256.getPersistedDigestLength(), SHA256_REF_FULL.length);

        { // Full digest
            final byte[] sha256Full = ByteBufferDigest.digest(HashAlgorithm.SHA256, REF);
            Assert.assertTrue(Arrays.equals(SHA256_REF_FULL, sha256Full));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, sha256Full));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
        }

        { // Full digest - ByteString
            final byte[] sha256Full = ByteBufferDigest.digest(HashAlgorithm.SHA256, ByteString.copyFrom(REF));
            REF.clear();
            Assert.assertTrue(Arrays.equals(SHA256_REF_FULL, sha256Full));
            Assert.assertTrue(ByteBufferDigest.match(ByteString.copyFrom(REF), sha256Full));
            REF.clear();
        }

        {// Partial digest
            REF.position(3).limit(7);
            final byte[] sha256Part = ByteBufferDigest.digest(HashAlgorithm.SHA256, REF);
            Assert.assertTrue(Arrays.equals(SHA256_REF_PART, sha256Part));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, sha256Part));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
        }
    }

    @Test
    public void testSHA256Check() throws NoSuchAlgorithmException {
        // Truncate reference
        final byte[] truncated = Arrays.copyOf(SHA256_REF_FULL, 7);
        Assert.assertFalse(HashAlgorithm.SHA256.checkHash(truncated));
        // Break trailer
        final byte[] trailer = Arrays.copyOf(SHA256_REF_FULL, SHA256_REF_FULL.length);
        trailer[trailer.length - 1]++;
        Assert.assertFalse(HashAlgorithm.SHA256.checkHash(trailer));
        // Change algo
        final byte[] algo = Arrays.copyOf(SHA256_REF_FULL, SHA256_REF_FULL.length);
        algo[0]++;
        Assert.assertFalse(HashAlgorithm.SHA256.checkHash(algo));
        // Change version
        final byte[] version = Arrays.copyOf(SHA256_REF_FULL, SHA256_REF_FULL.length);
        version[0] |= 0xC0;
        Assert.assertFalse(HashAlgorithm.SHA256.checkHash(version));
    }

    @Test
    public void testSHA512() throws NoSuchAlgorithmException, IllegalArgumentException {
        // Check digest len
        Assert.assertEquals(HashAlgorithm.SHA512.getStandardDigestLength(), SHA512_REF_FULL.length - 2);
        Assert.assertEquals(HashAlgorithm.SHA512.getPersistedDigestLength(), SHA512_REF_FULL.length);

        { // Full digest
            final byte[] sha512Full = ByteBufferDigest.digest(HashAlgorithm.SHA512, REF);
            Assert.assertTrue(Arrays.equals(SHA512_REF_FULL, sha512Full));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, sha512Full));
            Assert.assertEquals(0, REF.position());
            Assert.assertEquals(REF.capacity(), REF.limit());
        }

        { // Full digest - ByteString
            final byte[] sha512Full = ByteBufferDigest.digest(HashAlgorithm.SHA512, ByteString.copyFrom(REF));
            REF.clear();
            Assert.assertTrue(Arrays.equals(SHA512_REF_FULL, sha512Full));
            Assert.assertTrue(ByteBufferDigest.match(ByteString.copyFrom(REF), sha512Full));
            REF.clear();
        }

        {// Partial digest
            REF.position(3).limit(7);
            final byte[] sha512Part = ByteBufferDigest.digest(HashAlgorithm.SHA512, REF);
            Assert.assertTrue(Arrays.equals(SHA512_REF_PART, sha512Part));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
            Assert.assertTrue(ByteBufferDigest.match(REF, sha512Part));
            Assert.assertEquals(3, REF.position());
            Assert.assertEquals(7, REF.limit());
        }
    }

    @Test
    public void testSHA512Check() throws NoSuchAlgorithmException {
        // Truncate reference
        final byte[] truncated = Arrays.copyOf(SHA512_REF_FULL, 7);
        Assert.assertFalse(HashAlgorithm.SHA512.checkHash(truncated));
        // Break trailer
        final byte[] trailer = Arrays.copyOf(SHA512_REF_FULL, SHA512_REF_FULL.length);
        trailer[trailer.length - 1]++;
        Assert.assertFalse(HashAlgorithm.SHA512.checkHash(trailer));
        // Change algo
        final byte[] algo = Arrays.copyOf(SHA512_REF_FULL, SHA512_REF_FULL.length);
        algo[0]++;
        Assert.assertFalse(HashAlgorithm.SHA512.checkHash(algo));
        // Change version
        final byte[] version = Arrays.copyOf(SHA512_REF_FULL, SHA512_REF_FULL.length);
        version[0] |= 0xC0;
        Assert.assertFalse(HashAlgorithm.SHA512.checkHash(version));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testByteBufferCheckTruncate() throws NoSuchAlgorithmException {
        // Truncate reference
        final byte[] truncated = Arrays.copyOf(MD5_REF_FULL, 7);
        ByteBufferDigest.match(REF, truncated);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testByteBufferCheckBreadkTrailer() throws NoSuchAlgorithmException {
        // Break trailer
        final byte[] trailer = Arrays.copyOf(MD5_REF_FULL, MD5_REF_FULL.length);
        trailer[trailer.length - 1]++;
        ByteBufferDigest.match(REF, trailer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testByteBufferCheckChangeAlgo() throws NoSuchAlgorithmException {
        // Change algo
        final byte[] algo = Arrays.copyOf(MD5_REF_FULL, MD5_REF_FULL.length);
        algo[0]++;
        ByteBufferDigest.match(REF, algo);
    }

    @Test(expected = NoSuchAlgorithmException.class)
    public void testByteBufferCheckChangeVersion() throws NoSuchAlgorithmException {
        // Change version
        final byte[] version = Arrays.copyOf(MD5_REF_FULL, MD5_REF_FULL.length);
        version[0] |= 0xC0;
        ByteBufferDigest.match(REF, version);
    }

    @Test
    public void testMD5Hash() throws NoSuchAlgorithmException {
        Assert.assertEquals(HashAlgorithm.MD5, HashAlgorithm.getHashHashAlgorithm(new byte[] { 0x40 }));
    }

    @Test
    public void testTigerHash() throws NoSuchAlgorithmException {
        Assert.assertEquals(HashAlgorithm.TIGER, HashAlgorithm.getHashHashAlgorithm(new byte[] { 0x41 }));
    }

    @Test
    public void testSHA1Hash() throws NoSuchAlgorithmException {
        Assert.assertEquals(HashAlgorithm.SHA1, HashAlgorithm.getHashHashAlgorithm(new byte[] { 0x42 }));
    }

    @Test
    public void testSHA256Hash() throws NoSuchAlgorithmException {
        Assert.assertEquals(HashAlgorithm.SHA256, HashAlgorithm.getHashHashAlgorithm(new byte[] { 0x43 }));
    }

    @Test
    public void testSHA512Hash() throws NoSuchAlgorithmException {
        Assert.assertEquals(HashAlgorithm.SHA512, HashAlgorithm.getHashHashAlgorithm(new byte[] { 0x44 }));
    }

    @Test(expected = NoSuchAlgorithmException.class)
    public void testHashErrAlgo() throws NoSuchAlgorithmException {
        HashAlgorithm.getHashHashAlgorithm(new byte[] { (byte) 0x45 });
    }

    @Test(expected = NoSuchAlgorithmException.class)
    public void testHashErrVersion() throws NoSuchAlgorithmException {
        HashAlgorithm.getHashHashAlgorithm(new byte[] { (byte) 0xC1 });
    }
}
