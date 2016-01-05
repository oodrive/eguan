package io.eguan.dtx;

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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.hazelcast.core.AtomicNumber;

/**
 * Class offering utility methods commonly used by DTX classes.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class DtxUtils {

    /**
     * Updates the given {@link AtomicLong} so it has at least the provided minimum value.
     * 
     * After successful execution, counter.longValue() >= minValue holds.
     * 
     * @param counter
     *            the target {@link AtomicLong}
     * @param minValue
     *            the target minimum value
     */
    public static final void updateAtomicLongToAtLeast(@Nonnull final AtomicLong counter, final long minValue) {
        long oldLastValue = counter.longValue();
        while (oldLastValue < minValue) {
            counter.compareAndSet(oldLastValue, minValue);
            oldLastValue = counter.longValue();
        }
    }

    /**
     * Updates the given {@link AtomicNumber} so it has at least the provided minimum value.
     * 
     * After successful execution, counter.get() >= minValue holds.
     * 
     * @param counter
     *            the target {@link AtomicNumber}
     * @param minValue
     *            the target minimum value
     */
    public static final void updateAtomicNumberToAtLeast(@Nonnull final AtomicNumber counter, final long minValue) {
        long oldLastValue = counter.get();
        while (oldLastValue < minValue) {
            counter.compareAndSet(oldLastValue, minValue);
            oldLastValue = counter.get();
        }
    }

    /**
     * Internal converter from {@link DtxNode} to the {@link String} description format accepted by hazelcast's
     * TcpIpConfig object.
     * 
     * @param dtxNodes
     *            a {@link List} of {@link DtxNode}s
     * @return a {@link List} of {@link String} representations suitable for hazelcast's TcpIpConfig initialization
     */
    static final List<String> dtxNodesToMembers(final List<DtxNode> dtxNodes) {
        final ArrayList<String> result = new ArrayList<String>(dtxNodes.size());

        for (final DtxNode currNode : dtxNodes) {
            result.add(dtxToMemberString(currNode));
        }

        return result;
    }

    /**
     * Converts a {@link DtxNode} to a hazelcast configuration compatible {@link String} form.
     * 
     * This conversion is not reversible.
     * 
     * @param dtxNode
     *            the {@link DtxNode} instance from which to convert
     * @return a {@link String} hazelcast cluster member representation
     */
    static final String dtxToMemberString(@Nonnull final DtxNode dtxNode) {
        final InetSocketAddress nodeAddr = dtxNode.getAddress();
        return nodeAddr.getAddress().getHostAddress() + ":" + nodeAddr.getPort();
    }

    private DtxUtils() {
        throw new AssertionError("Not instantiable");
    }

}
