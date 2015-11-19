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

import java.util.UUID;

/**
 * Basic identifier provider.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public final class SimpleIdentifierProvider {

    /**
     * No instance.
     */
    private SimpleIdentifierProvider() {
        throw new AssertionError();
    }

    /**
     * Creates a new unique id out of the blue.
     * 
     * @return a new (somehow random) unique id
     */
    public static final <F> UuidT<F> newId() {
        return new UuidT<F>(UUID.randomUUID());
    }

    /**
     * Create a new {@link UuidT} from the given {@link String}. The string format is the same as
     * {@link UUID#fromString(String)}.
     * 
     * @param uuid
     *            uuid string to parse
     * @return a new {@link UuidT}.
     */
    public static final <F> UuidT<F> fromString(final String uuid) {
        return new UuidT<F>(UUID.fromString(uuid));
    }

    /**
     * Create a new {@link UuidT} from the given {@link UUID}.
     * 
     * @param uuid
     *            reference uuid
     * @return a new {@link UuidT}.
     */
    public static final <F> UuidT<F> fromUUID(final UUID uuid) {
        return new UuidT<F>(uuid);
    }

}
