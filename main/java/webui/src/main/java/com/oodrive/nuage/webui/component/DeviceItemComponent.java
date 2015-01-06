package com.oodrive.nuage.webui.component;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.webui.WebUiResources;
import com.oodrive.nuage.webui.WebUiUtils;
import com.oodrive.nuage.webui.WebUiUtils.IntegerAttributeOperation;
import com.oodrive.nuage.webui.WebUiUtils.LongAttributeOperation;
import com.oodrive.nuage.webui.WebUiUtils.StringAttributeOperation;
import com.oodrive.nuage.webui.component.WaitingComponent.Background;
import com.oodrive.nuage.webui.component.window.DeviceDeleteWindow;
import com.oodrive.nuage.webui.component.window.ErrorWindow;
import com.oodrive.nuage.webui.model.DeviceModel;
import com.vaadin.data.Property.ValueChangeEvent;
import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.ui.AbstractComponent;
import com.vaadin.ui.Accordion;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.OptionGroup;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;

/**
 * The class represents a device item which is a part of a tree items.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
final class DeviceItemComponent implements TreeItemComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeviceItemComponent.class);

    private final static String DEACTIVATE = "Deactivate";
    private final static String RWACTIVATE = "RW activate";
    private final static String ROACTIVATE = "RO activate";

    /* Device data representation */
    private final DeviceModel model;
    private final Accordion attr;

    private static final int ATTRIBUTES_INDEX = 0;
    private static final int ACTIVATE_INDEX = 1;
    private static final int TAKE_INDEX = 2;
    private static final int DELETE_INDEX = 3;

    DeviceItemComponent(final DeviceModel model) {
        super();
        this.model = model;
        this.attr = new Accordion();
    }

    @Override
    public final AbstractComponent init() {

        final AbstractComponent attrlayout = createAttributes();
        attr.addTab(attrlayout, "Attributes", WebUiResources.getSettingsSideIcon(), ATTRIBUTES_INDEX);

        final AbstractComponent actLayout = createActivate();
        attr.addTab(actLayout, "Activation", WebUiResources.getActivateIcon(), ACTIVATE_INDEX);

        final AbstractComponent takeSnapLayout = createTakeSnap();
        attr.addTab(takeSnapLayout, "Take Snapshot", WebUiResources.getSnapshotSideIcon(), TAKE_INDEX);

        final AbstractComponent deleteLayout = createDelete();
        attr.addTab(deleteLayout, "Delete", WebUiResources.getTrashSideIcon(), DELETE_INDEX);

        return attr;
    }

    /**
     * Update device attributes
     */
    protected final void updateAttributes() {
        // Tell if this the last selected tab of the accordion
        final boolean isSelected = attr.getSelectedTab().equals(attr.getTab(ATTRIBUTES_INDEX).getComponent());
        // Remove it
        attr.removeTab(attr.getTab(ATTRIBUTES_INDEX));
        // Create a new one with the new attributes value
        final AbstractComponent attrlayout = createAttributes();
        attr.addTab(attrlayout, "Attributes", WebUiResources.getSettingsSideIcon(), ATTRIBUTES_INDEX);
        // Select it if it was selected
        if (isSelected) {
            attr.setSelectedTab(attrlayout);
        }
    }

    /**
     * Create delete tab in the accordion.
     * 
     * @return the component.
     */
    @SuppressWarnings("serial")
    private final AbstractComponent createDelete() {

        /* root layout */
        final VerticalLayout layout = new VerticalLayout();
        layout.setSizeFull();
        layout.setMargin(true);
        layout.setSpacing(true);

        final Label label = new Label("Deleting a device can be done, only if it is de-activated.");
        layout.addComponent(label);
        label.setWidth(null);
        layout.setComponentAlignment(label, Alignment.MIDDLE_CENTER);

        final Button deleteButton = new Button("Delete");

        layout.addComponent(deleteButton);
        layout.setComponentAlignment(deleteButton, Alignment.BOTTOM_CENTER);

        deleteButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                try {
                    final DeviceDeleteWindow deleteWindow = new DeviceDeleteWindow(model.getItemUuid());
                    deleteWindow.add(model);
                }
                catch (final Exception e) {
                    LOGGER.error("Can not delete device: ", e);
                }
            }
        });
        return layout;
    }

    /**
     * Create the component to take a snapshot.
     * 
     * @return the component
     */
    @SuppressWarnings("serial")
    private final AbstractComponent createTakeSnap() {

        final VerticalLayout layout = new VerticalLayout();
        layout.setSpacing(true);
        layout.setMargin(true);

        final FormLayout takeSnapLayout = new FormLayout();
        takeSnapLayout.setMargin(true);
        takeSnapLayout.setImmediate(true);
        takeSnapLayout.setWidth(null);
        layout.addComponent(takeSnapLayout);
        layout.setComponentAlignment(takeSnapLayout, Alignment.MIDDLE_CENTER);

        // Enter name
        final TextField vvrName = new TextField("Name", "");
        takeSnapLayout.addComponent(vvrName);

        // take button
        final Button take = new Button("Take snapshot");
        layout.addComponent(take);
        layout.setComponentAlignment(take, Alignment.MIDDLE_CENTER);

        take.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                try {
                    model.takeDeviceSnapshot(vvrName.getValue());
                    Notification.show("New snapshot created", Notification.Type.TRAY_NOTIFICATION);
                }
                catch (final Exception e) {
                    final ErrorWindow err = new ErrorWindow("Snapshot not taken: " + e.getMessage());
                    err.add(model);
                }
            }
        });
        return layout;
    }

    /**
     * Create the component to activate/deactivate a device.
     * 
     * @return the component
     */
    @SuppressWarnings("serial")
    private final AbstractComponent createActivate() {

        final VerticalLayout rootlayout = new VerticalLayout();
        rootlayout.setMargin(true);
        rootlayout.setSpacing(true);

        final OptionGroup activate = new OptionGroup("Select an option: ");
        rootlayout.addComponent(activate);
        rootlayout.setComponentAlignment(activate, Alignment.MIDDLE_CENTER);

        activate.setNullSelectionAllowed(false);
        activate.setHtmlContentAllowed(true);
        activate.setImmediate(true);
        activate.addItem(DEACTIVATE);
        activate.addItem(RWACTIVATE);
        activate.addItem(ROACTIVATE);

        final boolean isActivated = model.isDeviceActive();
        if (isActivated) {
            final boolean isReadOnly = model.isDeviceReadOnly();
            if (isReadOnly) {
                activate.select(ROACTIVATE);
                // rw is not authorized, deactivate first
                activate.setItemEnabled(RWACTIVATE, false);
            }
            else {
                activate.select(RWACTIVATE);
                // ro is not authorized, deactivate first
                activate.setItemEnabled(ROACTIVATE, false);
            }
        }
        else {
            activate.select(DEACTIVATE);
        }

        activate.addValueChangeListener(new ValueChangeListener() {
            @Override
            public void valueChange(final ValueChangeEvent event) {
                final String valueString = String.valueOf(event.getProperty().getValue());

                if (valueString.equals(DEACTIVATE)) {
                    final String action = DEACTIVATE;

                    // Run activation in background
                    WaitingComponent.executeBackground(model, new Background() {
                        @Override
                        public void processing() {
                            model.deActivateDevice();
                        }

                        @Override
                        public void postProcessing() {
                            activate.setItemEnabled(RWACTIVATE, true);
                            activate.setItemEnabled(ROACTIVATE, true);
                            updateAttributes();
                            Notification.show("Device " + action + "d", Notification.Type.TRAY_NOTIFICATION);
                        }
                    });

                }
                else if (valueString.equals(RWACTIVATE)) {
                    final String action = RWACTIVATE;
                    WaitingComponent.executeBackground(model, new Background() {
                        @Override
                        public void processing() {
                            model.activateDeviceRW();
                        }

                        @Override
                        public void postProcessing() {
                            // ro is not authorized, deactivate first
                            activate.setItemEnabled(ROACTIVATE, false);
                            updateAttributes();
                            Notification.show("Device " + action + "d", Notification.Type.TRAY_NOTIFICATION);
                        }
                    });
                }
                else if (valueString.equals(ROACTIVATE)) {
                    final String action = ROACTIVATE;
                    WaitingComponent.executeBackground(model, new Background() {
                        @Override
                        public void processing() {
                            model.activateDeviceRO();
                        }

                        @Override
                        public void postProcessing() {
                            // rw is not authorized, deactivate first
                            activate.setItemEnabled(RWACTIVATE, false);
                            updateAttributes();
                            Notification.show("Device " + action + "d", Notification.Type.TRAY_NOTIFICATION);
                        }
                    });
                }

            }
        });

        return rootlayout;
    }

    /**
     * Create attributes component.
     * 
     * @return the component
     */
    private final AbstractComponent createAttributes() {

        final VerticalLayout layout = new VerticalLayout();

        final FormLayout deviceAttributesLayout = new FormLayout();
        deviceAttributesLayout.setMargin(true);
        deviceAttributesLayout.setWidth(null);
        deviceAttributesLayout.setImmediate(true);
        layout.addComponent(deviceAttributesLayout);
        layout.setComponentAlignment(deviceAttributesLayout, Alignment.MIDDLE_CENTER);

        // Enter NAME
        WebUiUtils.createFieldString(new StringAttributeOperation() {
            @Override
            public void setStringValue(final String value) {
                model.setDeviceName(value);
            }

            @Override
            public String getStringValue() {
                return model.getDeviceName();
            }
        }, "Name", deviceAttributesLayout, model);

        // Enter DESCRIPTION
        WebUiUtils.createFieldString(new StringAttributeOperation() {
            @Override
            public void setStringValue(final String value) {
                model.setDeviceDescription(value);
            }

            @Override
            public String getStringValue() {
                return model.getDeviceDescription();
            }
        }, "Description", deviceAttributesLayout, model);

        // Enter UUID (not editable)
        final TextField deviceUUID = new TextField("UUID", model.getItemUuid().toString());
        deviceUUID.setReadOnly(true);
        deviceUUID.setWidth("300px");
        deviceAttributesLayout.addComponent(deviceUUID);

        // Enter active
        final TextField deviceActive = new TextField("Active");
        if (model.isDeviceActive()) {
            deviceActive.setValue("yes");
        }
        else {
            deviceActive.setValue("no");
        }
        deviceActive.setReadOnly(true);
        deviceActive.setSizeFull();
        deviceAttributesLayout.addComponent(deviceActive);

        // Enter read only
        final TextField deviceReadOnly = new TextField("Read Only");
        if (model.isDeviceReadOnly()) {
            deviceReadOnly.setValue("yes");
        }
        else {
            deviceReadOnly.setValue("no");
        }
        deviceReadOnly.setReadOnly(true);
        deviceReadOnly.setSizeFull();
        deviceAttributesLayout.addComponent(deviceReadOnly);

        // Enter size
        WebUiUtils.createFieldLong(new LongAttributeOperation() {
            @Override
            public void setLongValue(final long value) {
                model.setDeviceSize(value);
            }

            @Override
            public long getLongValue() {
                return model.getDeviceSize();
            }

        }, "Size", deviceAttributesLayout, model);

        // Enter IQN
        WebUiUtils.createFieldString(new StringAttributeOperation() {
            @Override
            public void setStringValue(final String value) {
                model.setDeviceIqn(value);
            }

            @Override
            public String getStringValue() {
                return model.getDeviceIqn();
            }
        }, "IQN", deviceAttributesLayout, model);

        // Enter Alias
        WebUiUtils.createFieldString(new StringAttributeOperation() {
            @Override
            public void setStringValue(final String value) {
                model.setDeviceIscsiAlias(value);
            }

            @Override
            public String getStringValue() {
                return model.getDeviceIscsiAlias();
            }
        }, "iSCSI Alias", deviceAttributesLayout, model);

        // Enter iscsi block size
        WebUiUtils.createFieldInteger(new IntegerAttributeOperation() {
            @Override
            public void setIntegerValue(final int value) {
                model.setDeviceIscsiBlockSize(value);
            }

            @Override
            public int getIntegerValue() {
                return model.getDeviceIscsiBlockSize();
            }

        }, "iSCSI Block Size", deviceAttributesLayout, model, true);
        return layout;
    }
}
