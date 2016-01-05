package io.eguan.ibs;

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

import java.nio.ByteBuffer;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

abstract class IbsDBAbstract extends IbsAbstract {

    /**
     * Creates a new instance.
     * 
     * @param ibsPath
     *            path of the IBS store or to a configuration file
     */
    IbsDBAbstract(final String ibsPath) {
        super(ibsPath);
    }

    @Override
    public final boolean put(@Nonnull final byte[] key, @Nonnull final ByteBuffer data, @Nonnegative final int offset,
            @Nonnegative final int length) throws IbsException, IbsIOException, IllegalArgumentException,
            IndexOutOfBoundsException, NullPointerException {
        return doPut(0, key, data, offset, length);
    }

    @Override
    public final boolean replace(@Nonnull final byte[] oldKey, @Nonnull final byte[] newKey,
            @Nonnull final ByteBuffer data, @Nonnegative final int offset, @Nonnegative final int length)
            throws IbsException, IllegalArgumentException, IbsIOException, IndexOutOfBoundsException,
            NullPointerException {
        return doReplace(0, oldKey, newKey, data, offset, length);
    }

    @Override
    public final boolean put(@Nonnegative final int txId, final byte[] key, final ByteBuffer data, final int offset,
            final int length) throws IbsException, IbsIOException, IllegalArgumentException, IndexOutOfBoundsException,
            NullPointerException {
        // Check the transaction ID
        if (txId <= 0 && txId != Integer.MIN_VALUE) {
            throw new IllegalArgumentException("txId=" + txId);
        }
        return doPut(txId, key, data, offset, length);
    }

    @Override
    public final boolean replace(@Nonnegative final int txId, final byte[] oldKey, final byte[] newKey,
            final ByteBuffer data, final int offset, final int length) throws IbsException, IllegalArgumentException,
            IbsIOException, IndexOutOfBoundsException, NullPointerException {
        // Check the transaction ID
        if (txId <= 0 && txId != Integer.MIN_VALUE) {
            throw new IllegalArgumentException("txId=" + txId);
        }
        return doReplace(txId, oldKey, newKey, data, offset, length);
    }

    protected abstract boolean doPut(final int txId, @Nonnull final byte[] key, @Nonnull final ByteBuffer data,
            @Nonnegative final int offset, @Nonnegative final int length) throws IbsException, IbsIOException,
            IllegalArgumentException, IndexOutOfBoundsException, NullPointerException;

    protected abstract boolean doReplace(final int txId, @Nonnull final byte[] oldKey, @Nonnull final byte[] newKey,
            @Nonnull final ByteBuffer data, @Nonnegative final int offset, @Nonnegative final int length)
            throws IbsException, IllegalArgumentException, IbsIOException, IndexOutOfBoundsException,
            NullPointerException;
}
