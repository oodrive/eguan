package io.eguan.nbdsrv;

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

import io.eguan.srv.DeviceTarget;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * Represents an NBD export.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
public final class NbdExport implements DeviceTarget {

    /** The name of the export */
    private final String name;
    /** The device corresponding to the export */
    private final NbdDevice device;

    public NbdExport(@Nonnull final String name, @Nonnull final NbdDevice device) {
        this.name = Objects.requireNonNull(name);
        this.device = Objects.requireNonNull(device);
    }

    /**
     * Gets the name of the export.
     * 
     * @return the name
     */
    @Override
    public final String getTargetName() {
        return name;
    }

    /**
     * Gets the device of the export.
     * 
     * @return the {@link NbdDevice}
     */
    final NbdDevice getDevice() {
        return device;
    }

    /**
     * Gets the size of the device in bytes.
     * 
     * @return the size of the device in bytes.
     */
    final long getSize() {
        return device.getSize();
    }

    /**
     * Tells if the export is read only.
     * 
     * @return true if the export is read only
     */
    final boolean isReadOnly() {
        return device.isReadOnly();
    }
}
