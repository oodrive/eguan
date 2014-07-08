package com.oodrive.nuage.iscsisrv;

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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.jscsi.target.Target;
import org.jscsi.target.storage.IStorageModule;

import com.oodrive.nuage.srv.DeviceTarget;

/**
 * This class represents a iSCSI target. The target has a name, an optional alias and is associated to a device.
 * 
 * @author oodrive
 * @author llambert
 * @author ebredzinski
 */
@Immutable
public final class IscsiTarget implements DeviceTarget {

    /** Associated jSCSI target */
    private final Target target;

    /**
     * Create a new target.
     * 
     * @param target
     */
    private IscsiTarget(@Nonnull final Target target) {
        super();
        this.target = target;
    }

    /**
     * Gets the name of the target.
     * 
     * @return the name of the target
     */
    @Override
    public final String getTargetName() {
        return target.getTargetName();
    }

    /**
     * Gets the target alias.
     * 
     * @return the target alias. Maybe <code>null</code>
     */
    public final String getTargetAlias() {
        return target.getTargetAlias();
    }

    /**
     * Tells if the target is read only
     * 
     * @return <code>true</code> if the target is read only.
     */
    public final boolean isReadOnly() {
        return target.getStorageModule().isWriteProtected();
    }

    /**
     * Gets the size of the device in bytes. Same value as the jSCSI server: rounded to the smaller block size.
     * 
     * @return the size of the device in bytes.
     */
    final long getSize() {
        final IStorageModule device = target.getStorageModule();
        return device.getSizeInBlocks() * target.getStorageModule().getBlockSize();
    }

    /**
     * Close the device of the target.
     * 
     * @throws IOException
     */
    final void close() throws IOException {
        final IStorageModule device = target.getStorageModule();
        device.close();
    }

    /**
     * Create a new target with the current name.
     * 
     * @param name
     *            iSCSI target name
     * @param alias
     *            iSCSI target alias. May be <code>null</code>
     * @param device
     *            opened device for the new target.
     * @return a new {@link IscsiTarget} based on the given device.
     */
    public static final IscsiTarget newIscsiTarget(@Nonnull final String name, @Nullable final String alias,
            @Nonnull final IscsiDevice device) {
        final IStorageModule storageModule = new IStorageModuleImpl(device);
        final Target target = new Target(checkTargetName(name), checkTargetAlias(alias), storageModule);
        return new IscsiTarget(target);
    }

    /**
     * Checks if the target name is a valid iSCSI target name (see RFC 3720).
     * 
     * @param name
     * @return name
     * @throws IllegalArgumentException
     */
    private static final String checkTargetName(@Nonnull final String name) throws IllegalArgumentException {
        // Not null
        Objects.requireNonNull(name);

        if (name.startsWith("iqn.")) {
            // Max length in bytes: 233, including the trailing 0
            checkUTF8Length(name, 233, "name=");

            // TODO check format: iqn.<date>.<domain>:<string>
        }
        else if (name.startsWith("eui.")) {
            // 16 ASCII-encoded hexadecimal digits
            if (name.length() != 20) {
                throw new IllegalArgumentException("name=" + name);
            }
            if (!name.substring(4).matches("\\p{XDigit}+")) {
                throw new IllegalArgumentException("name=" + name);
            }
        }
        else {
            throw new IllegalArgumentException("name=" + name);
        }
        return name;
    }

    /**
     * Checks if the target alias is a valid iSCSI target alias (see RFC 3721).
     * 
     * @param alias
     * @return alias
     * @throws IllegalArgumentException
     */
    private static final String checkTargetAlias(@Nullable final String alias) throws IllegalArgumentException {
        // May be null
        if (alias == null) {
            return null;
        }
        // Max length in bytes: 255, including the trailing 0
        checkUTF8Length(alias, 255, "alias=");
        return alias;
    }

    /**
     * Checks the length of a string, coded in UTF-8 format.
     * 
     * @param str
     * @param maxLength
     *            max length, including a trailing 0
     * @param mgsHeader
     *            header of the message of the exception
     * @throws IllegalArgumentException
     */
    private static final void checkUTF8Length(@Nonnull final String str, final int maxLength, final String mgsHeader)
            throws IllegalArgumentException {
        try {
            final byte[] bytes = str.getBytes("UTF-8");
            if (bytes.length > maxLength || (bytes.length == maxLength && bytes[maxLength - 1] != 0)) {
                throw new IllegalArgumentException(mgsHeader + str);
            }
        }
        catch (final UnsupportedEncodingException e) {
            // a JVM must support utf-8
            throw new AssertionError("UTF-8 unsupported", e);
        }
    }

    /**
     * Needed by the server.
     * 
     * @return the jSCSI target.
     */
    final Target getTarget() {
        return target;
    }

    /**
     * The comparison is based only on the name of the target.
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public final boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof IscsiTarget))
            return false;
        final IscsiTarget other = (IscsiTarget) obj;
        return target.equals(other.target);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public final int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + target.hashCode();
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        return "IscsiTarget[" + target.getTargetName() + "]";
    }

}
