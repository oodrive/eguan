package io.eguan.vvr.repository.core.api;

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

import io.eguan.proto.Common.OpCode;
import io.eguan.proto.vvr.VvrRemote.Item;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;

import java.util.Objects;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract implementation of repository object.
 * 
 * @author oodrive
 * @author pwehrle
 * @author llambert
 * @author jmcaba
 * 
 */
public abstract class AbstractUniqueVvrObject implements UniqueVvrObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUniqueVvrObject.class);

    /**
     * The uuid initialized at every object's creation.
     */
    private final UUID uniqueId;

    /**
     * The optional name of the object.
     */
    private String objectName;

    /**
     * The optional description of the object.
     */
    private String objectDescription;

    /**
     * Abstract constructor for builder invocation.
     * 
     * @param builder
     *            the {@link AbstractUniqueVvrObject#Builder} used to initialize instances of the extending class
     */
    protected AbstractUniqueVvrObject(final AbstractUniqueVvrObject.Builder builder) {
        this.uniqueId = builder.targetId;
        this.objectName = builder.targetName;
        this.objectDescription = builder.targetDescription;

        if (this.uniqueId == null) {
            throw new IllegalStateException("unique id is null");
        }
    }

    @Override
    public final UUID getUuid() {
        return this.uniqueId;
    }

    @Override
    public final String getName() {
        return objectName;
    }

    @Override
    public FutureVoid setName(final String name) {
        if (Objects.equals(name, objectName)) {
            // No change
            return null;
        }
        final Item.Builder itemBuilder = Item.newBuilder().setName(name);
        return submitTransaction(RemoteOperation.newBuilder().setItem(itemBuilder), OpCode.SET);
    }

    public final void setNameLocal(final String name) {
        this.objectName = name;
    }

    @Override
    public final String getDescription() {
        return objectDescription;
    }

    @Override
    public final FutureVoid setDescription(final String description) {
        if (Objects.equals(description, objectDescription)) {
            // No change
            return null;
        }
        final Item.Builder itemBuilder = Item.newBuilder().setDescription(description);
        return submitTransaction(RemoteOperation.newBuilder().setItem(itemBuilder), OpCode.SET);
    }

    public final void setDescriptionLocal(final String description) {
        LOGGER.debug("Setting description; is='" + objectDescription + "', new='" + description + "'" + ", timeStamp="
                + System.currentTimeMillis());
        this.objectDescription = description;
    }

    /**
     * Abstract submission of a distributed transaction by an implementing class.
     * 
     * @param opBuilder
     *            the {@link RemoteOperation.Builder} configured for the transaction
     * @param opCode
     *            the {@link OpCode} describing the operation
     * @return a {@link FutureVoid} representing the created task
     */
    abstract protected FutureVoid submitTransaction(final RemoteOperation.Builder opBuilder, final OpCode opCode);

    /**
     * Abstract member builder for properties common to all extending classes.
     * 
     * @see UniqueVvrObject.Builder
     */
    public abstract static class Builder implements UniqueVvrObject.Builder {

        /**
         * The unique ID used to build object instances.
         */
        private UUID targetId = null;

        /**
         * The optional name to assign the built instance.
         */
        private String targetName = null;

        /**
         * The optional description to assign the built instance.
         */
        private String targetDescription = null;

        @Override
        public final Builder uuid(@Nonnull final UUID uuid) {
            this.targetId = Objects.requireNonNull(uuid);
            return this;
        }

        protected final UUID uuid() {
            return targetId;
        }

        @Override
        public final Builder name(final String name) {
            this.targetName = name;
            return this;
        }

        @Override
        public final Builder description(final String description) {
            this.targetDescription = description;
            return this;
        }
    }

}
