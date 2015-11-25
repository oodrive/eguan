package io.eguan.utils;

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

import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Same as {@link UUID}, but can be typed.
 * 
 * @author oodrive
 * @author llambert
 * 
 * @param <F>
 *            class of object identified by this UUID.
 */
@Immutable
public final class UuidT<F> {

    private final UUID uuid;

    public UuidT(final long mostSigBits, final long leastSigBits) {
        super();
        this.uuid = new UUID(mostSigBits, leastSigBits);
    }

    UuidT(@Nonnull final UUID uuid) {
        super();
        this.uuid = Objects.requireNonNull(uuid);
    }

    final UUID getUuid() {
        return uuid;
    }

    /**
     * See {@link UUID#getMostSignificantBits()}.
     * 
     * @return The most significant 64 bits of this UUID's 128 bit value
     */
    public final long getMostSignificantBits() {
        return uuid.getMostSignificantBits();
    }

    /**
     * See {@link UUID#getLeastSignificantBits()}.
     * 
     * @return The least significant 64 bits of this UUID's 128 bit value
     */
    public final long getLeastSignificantBits() {
        return uuid.getLeastSignificantBits();
    }

    /**
     * See {@link UUID#hashCode()}.
     */
    @Override
    public final int hashCode() {
        return uuid.hashCode();
    }

    /**
     * See {@link UUID#equals()}.
     */
    @Override
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof UuidT)) {
            return false;
        }
        final UuidT<?> other = (UuidT<?>) obj;
        return uuid.equals(other.uuid);
    }

    /**
     * See {@link UUID#toString()}.
     */
    @Override
    public final String toString() {
        return uuid.toString();
    }

}
