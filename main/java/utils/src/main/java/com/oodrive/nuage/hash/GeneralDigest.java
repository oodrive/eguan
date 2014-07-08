package com.oodrive.nuage.hash;

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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * base implementation of MD4 family style digest as outlined in "Handbook of Applied Cryptography", pages 344 - 347.
 * 
 * @author oodrive
 * @author llambert
 * @author pwehrle
 */
abstract class GeneralDigest implements Digest {
    private final byte[] xBuf = new byte[4];
    private int xBufOff;

    private long byteCount;

    private final boolean bigendian;
    protected final ByteBuffer source;

    /**
     * Standard constructor
     */
    protected GeneralDigest(final ByteBuffer source, final boolean bigendian) {
        this.xBufOff = 0;
        this.bigendian = bigendian;
        this.source = source.duplicate();
        this.source.order(bigendian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
    }

    private final void update(final byte in) {
        xBuf[xBufOff++] = in;

        if (xBufOff == xBuf.length) {
            final int intValue;
            if (bigendian) {
                intValue = (xBuf[0] & 0xff) << 24 | (xBuf[1] & 0xff) << 16 | (xBuf[2] & 0xff) << 8 | (xBuf[3] & 0xff);
            }
            else {
                intValue = (xBuf[0] & 0xff) | (xBuf[1] & 0xff) << 8 | (xBuf[2] & 0xff) << 16 | (xBuf[3] & 0xff) << 24;
            }

            processWord(intValue);
            xBufOff = 0;
        }

        byteCount++;
    }

    protected final void finish() {

        // Process source long by long
        while (source.remaining() >= 4) {
            processWord(source.getInt());
            byteCount += 4;
        }
        // Remaining, if any
        while (source.remaining() != 0) {
            // Add remaining bytes
            update(source.get());
        }

        final long bitLength = (byteCount << 3);

        //
        // add the pad bytes.
        //
        update((byte) 128);

        while (xBufOff != 0) {
            update((byte) 0);
        }

        processLength(bitLength);

        processBlock();
    }

    protected abstract void processWord(int in);

    protected abstract void processLength(long bitLength);

    protected abstract void processBlock();

}
