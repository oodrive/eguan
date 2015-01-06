package com.oodrive.nuage.ibs;

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
 * Thrown when the buffer given to read a record in an IBS is too small. Gives the size of the record.
 * 
 * @author oodrive
 * @author llambert
 * @author jmcaba
 * 
 */
public final class IbsBufferTooSmallException extends IbsIOException {
    // Not serialized
    private static final long serialVersionUID = 1L;

    /** Length of the record */
    private final int recordLength;

    IbsBufferTooSmallException(final int recordLength) {
        super(IbsErrorCode.BUFFER_TOO_SMALL);
        this.recordLength = recordLength;
    }

    IbsBufferTooSmallException(final String message, final int recordLength) {
        super(message, IbsErrorCode.BUFFER_TOO_SMALL);
        this.recordLength = recordLength;
    }

    /**
     * Gets the length of the record to read.
     * 
     * @return the length of the record
     */
    public final int getRecordLength() {
        return recordLength;
    }

    @Override
    public String toString() {
        return super.toString() + " [recordLength=" + recordLength + "]";
    }

}
