package com.oodrive.nuage.ibs;

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

/**
 * Possible error codes when accessing an IBS. Must match the enumeration in <code>libibs.h</code> and the error codes
 * in the native code (<code>ibs_native.c</code>).
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 */
public enum IbsErrorCode {
    /** IBS does not exist */
    INVALID_IBS_ID(-1),
    /** Record not found (get) or on put (refresh) */
    NOT_FOUND(-2),
    /** The given buffer is too small for the record to get */
    BUFFER_TOO_SMALL(-3),
    /** The underlying database is corrupted */
    DATA_CORRUPTION(-4),
    /** Error during the read or the write of the datas */
    IO_ERROR(-5),
    /** Configuration error */
    CONFIG_ERROR(-6),
    /** Create on non-empty */
    CREATE_IN_NON_EMPTY_DIR(-7),
    /** Init on empty */
    INIT_FROM_EMPTY_DIR(-8),
    /** Internal error, on lock for example */
    INTERNAL_ERROR(-9),
    /** Code returned when a key is already in the database */
    KEY_ALREADY_ADDED(-10),
    /** Code returned when transaction does not exist */
    INVALID_TRANSACTION_ID(-11),
    /** Code returned when an error occurs while atomic write (disk full ...) */
    ATOMIC_WRITE_ERROR(-12),
    /** Unexpected error */
    UNKNOW_ERROR(-99),
    /** Native code error: returned when a direct {@link ByteBuffer} can not be accessed */
    DIRECT_BUFFER_UNSUPPORTED(-100);

    /** Error code defined in <code>libibs.h</code> */
    private final int code;

    private IbsErrorCode(final int code) {
        this.code = code;
    }

    /**
     * Finds the error enum matching the given code.
     * 
     * @param code
     * @return the {@link IbsErrorCode} matching <code>code</code>
     * @throws IllegalArgumentException
     *             if code is not a valid IBS error code
     */
    static final IbsErrorCode valueOf(final int code) throws IllegalArgumentException {
        final IbsErrorCode[] values = values();
        for (final IbsErrorCode ibsErrorCode : values) {
            if (ibsErrorCode.code == code)
                return ibsErrorCode;
        }
        throw new IllegalArgumentException("code=" + code);
    }
}
