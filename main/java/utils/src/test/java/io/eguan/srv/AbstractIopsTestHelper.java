package io.eguan.srv;

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

public abstract class AbstractIopsTestHelper {

    protected final int blockSize;
    protected final int numBlocks;
    protected final int length;

    protected AbstractIopsTestHelper(final int blockSize, final int numBlocks, final int length) {
        this.blockSize = blockSize;
        this.numBlocks = numBlocks;
        this.length = length;
    }

    public final int getBlockSize() {
        return blockSize;
    }

    final int getNumBlocks() {
        return numBlocks;
    }

    final int getLength() {
        return length;
    }
}
