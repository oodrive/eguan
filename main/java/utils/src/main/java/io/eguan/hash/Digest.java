package io.eguan.hash;

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

/**
 * Common interface for the local digest implementations.
 * 
 * @author oodrive
 * @author llambert
 */
interface Digest {
    /**
     * Gets the length of the digest in bytes.
     * 
     * @return length of the digest in bytes.
     */
    int getDigestSize();

    /**
     * Fills the given array with the digest.
     * 
     * @param out
     *            array to fill
     * @param outOff
     *            start position in <code>out</code>
     * @return the number of bytes written in <code>out</code>
     */
    int doFinal(final byte[] out, final int outOff);
}
