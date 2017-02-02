package io.eguan.webui;

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

import java.io.File;

import com.vaadin.server.FileResource;
import com.vaadin.server.Resource;
import com.vaadin.server.VaadinService;

/**
 * Utility class to manage resources.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class WebUiResources {

    private static final String basepath = VaadinService.getCurrent().getBaseDirectory().getAbsolutePath();

    /* Snapshot */
    private static final Resource smallsnapshot = new FileResource(new File(basepath
            + "/WEB-INF/images/smallsnapshot.png"));
    private static final Resource snapshot = new FileResource(new File(basepath + "/WEB-INF/images/snapshot.png"));

    /* Device */
    private static final Resource smalldevice = new FileResource(new File(basepath + "/WEB-INF/images/smalldevice.png"));
    private static final Resource device = new FileResource(new File(basepath + "/WEB-INF/images/device.png"));

    /* Activation/Deactivation */
    private static final Resource activateIcon = new FileResource(new File(basepath + "/WEB-INF/images/smallstart.png"));
    private static final Resource startIcon = new FileResource(new File(basepath + "/WEB-INF/images/start.png"));
    private static final Resource stopIcon = new FileResource(new File(basepath + "/WEB-INF/images/stop.png"));

    /* Settings */
    private static final Resource smallsettingsIcon = new FileResource(new File(basepath
            + "/WEB-INF/images/smallsettings.png"));
    private static final Resource settingsIcon = new FileResource(new File(basepath + "/WEB-INF/images/settings.png"));

    /* Trash */
    private static final Resource smalltrashIcon = new FileResource(new File(basepath
            + "/WEB-INF/images/smalltrash.png"));
    private static final Resource trashIcon = new FileResource(new File(basepath + "/WEB-INF/images/trash.png"));

    /**
     * Get the start icon.
     * 
     * @return the resource
     */
    public static final Resource getStartIcon() {
        return startIcon;
    }

    /**
     * Get the stop icon.
     * 
     * @return the resource
     */
    public static final Resource getStopIcon() {
        return stopIcon;
    }

    /**
     * Get the trash icon.
     * 
     * @return the resource
     */
    public static final Resource getTrashIcon() {
        return trashIcon;
    }

    /**
     * Get the settings icon.
     * 
     * @return the resource
     */
    public static final Resource getSettingsIcon() {
        return settingsIcon;
    }

    /**
     * Get the trash side icon.
     * 
     * @return the resource
     */
    public static final Resource getTrashSideIcon() {
        return smalltrashIcon;
    }

    /**
     * Get the settings side icon.
     * 
     * @return the resource
     */
    public static final Resource getSettingsSideIcon() {
        return smallsettingsIcon;
    }

    /**
     * Get the device side icon.
     * 
     * @return the resource
     */
    public static final Resource getDeviceSideIcon() {
        return smalldevice;
    }

    /**
     * Get the device tree icon.
     * 
     * @return the resource
     */
    public static final Resource getDeviceTreeIcon() {
        return device;
    }

    /**
     * Get the activate icon.
     * 
     * @return the resource
     */
    public static final Resource getActivateIcon() {
        return activateIcon;
    }

    /**
     * Get the Snapshot side icon.
     * 
     * @return the resource
     */
    public static final Resource getSnapshotSideIcon() {
        return smallsnapshot;
    }

    /**
     * Get the snapshot tree icon.
     * 
     * @return the resource
     */
    public static final Resource getSnapshotTreeIcon() {
        return snapshot;
    }

}
