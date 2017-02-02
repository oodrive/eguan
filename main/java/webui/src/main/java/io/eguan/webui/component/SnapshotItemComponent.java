package io.eguan.webui.component;

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

import io.eguan.webui.WebUiResources;
import io.eguan.webui.WebUiUtils;
import io.eguan.webui.WebUiUtils.StringAttributeOperation;
import io.eguan.webui.component.window.ErrorWindow;
import io.eguan.webui.component.window.SnapshotDeleteWindow;
import io.eguan.webui.model.SnapshotModel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.ui.AbstractComponent;
import com.vaadin.ui.Accordion;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;

/**
 * The class represents a snapshot item which is a part of a tree items.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
final class SnapshotItemComponent implements TreeItemComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotItemComponent.class);

    private final SnapshotModel model;

    private final Accordion attr;

    private static final int ATTRIBUTES_INDEX = 0;

    private static final int CREATE_INDEX = 1;

    private static final int DELETE_INDEX = 2;

    SnapshotItemComponent(final SnapshotModel model) {
        super();
        this.model = model;
        this.attr = new Accordion();
    }

    @Override
    public final AbstractComponent init() {

        final AbstractComponent attrlayout = createAttributes();
        attr.addTab(attrlayout, "Attributes", WebUiResources.getSettingsSideIcon(), ATTRIBUTES_INDEX);

        final AbstractComponent createDeviceLayout = createDevice();
        attr.addTab(createDeviceLayout, "Create device", WebUiResources.getDeviceSideIcon(), CREATE_INDEX);

        final AbstractComponent deleteLayout = createDelete();
        attr.addTab(deleteLayout, "Delete", WebUiResources.getTrashSideIcon(), DELETE_INDEX);

        return attr;
    }

    /**
     * Update snapshot attributes
     */
    protected final void updateAttributes() {
        final boolean isSelected = attr.getSelectedTab().equals(attr.getTab(ATTRIBUTES_INDEX).getComponent());
        attr.removeTab(attr.getTab(ATTRIBUTES_INDEX));

        final AbstractComponent attrlayout = createAttributes();
        attr.addTab(attrlayout, "Attributes", WebUiResources.getSettingsSideIcon(), ATTRIBUTES_INDEX);
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

        final VerticalLayout layout = new VerticalLayout();
        layout.setSizeFull();
        layout.setMargin(true);
        layout.setSpacing(true);

        final Label label = new Label("Deleting a snapshot can be done, only if it is not the root snapshot.");
        label.setWidth(null);
        layout.addComponent(label);
        layout.setComponentAlignment(label, Alignment.MIDDLE_CENTER);

        final Button deleteButton = new Button("Delete");

        if (model.getItemUuid().equals(model.getSnapshotParent())) {
            // Root snapshot can not be deleted
            deleteButton.setEnabled(false);
        }
        layout.addComponent(deleteButton);
        layout.setComponentAlignment(deleteButton, Alignment.BOTTOM_CENTER);

        deleteButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                try {
                    final SnapshotDeleteWindow deleteWindow = new SnapshotDeleteWindow(model.getItemUuid());
                    deleteWindow.add(model);
                }
                catch (final Exception e) {
                    LOGGER.error("Can not delete snapshot:", e);
                    final ErrorWindow err = new ErrorWindow("Snapshot not deleted: " + e.getMessage());
                    err.add(model);
                }
            }
        });
        return layout;
    }

    /**
     * Create the component to create a device.
     * 
     * @return the component
     */
    @SuppressWarnings("serial")
    private final AbstractComponent createDevice() {

        final VerticalLayout layout = new VerticalLayout();
        layout.setMargin(true);
        layout.setSpacing(true);

        final FormLayout createDeviceLayout = new FormLayout();
        createDeviceLayout.setMargin(true);
        createDeviceLayout.setWidth(null);
        createDeviceLayout.setImmediate(true);
        layout.addComponent(createDeviceLayout);
        layout.setComponentAlignment(createDeviceLayout, Alignment.MIDDLE_CENTER);

        // Enter name
        final TextField deviceName = new TextField("Name", "");
        createDeviceLayout.addComponent(deviceName);

        // Enter size (size of the snapshot by default)
        final TextField deviceSize = new TextField("Size", Long.toString(model.getSnapshotSize()));
        createDeviceLayout.addComponent(deviceSize);

        // Create button
        final Button create = new Button("Create device");
        layout.addComponent(create);
        layout.setComponentAlignment(create, Alignment.MIDDLE_CENTER);

        create.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                boolean success = false;
                try {
                    model.createDevice(deviceName.getValue(), Long.valueOf(deviceSize.getValue()));
                    success = true;
                    Notification.show("New device created", Notification.Type.TRAY_NOTIFICATION);
                }
                catch (final NumberFormatException e) {
                    final ErrorWindow err = new ErrorWindow("Size must be a valid number");
                    err.add(model);
                }
                catch (final Exception e) {
                    final ErrorWindow err = new ErrorWindow("Device not created: " + e.getMessage());
                    err.add(model);
                }

                if (success) {
                    // Reset text fields on success
                    deviceName.setValue("");
                    deviceSize.setValue(Long.toString(model.getSnapshotSize()));
                }
            }
        });
        return layout;
    }

    /**
     * Create the component for the snapshot attributes
     * 
     * @return the component
     */
    private final AbstractComponent createAttributes() {

        final VerticalLayout layout = new VerticalLayout();

        final FormLayout snapshotAttributesLayout = new FormLayout();
        snapshotAttributesLayout.setMargin(true);
        snapshotAttributesLayout.setWidth(null);
        snapshotAttributesLayout.setImmediate(true);
        layout.addComponent(snapshotAttributesLayout);
        layout.setComponentAlignment(snapshotAttributesLayout, Alignment.MIDDLE_CENTER);

        // Enter NAME
        WebUiUtils.createFieldString(new StringAttributeOperation() {
            @Override
            public void setStringValue(final String value) {
                model.setSnapshotName(value);
            }

            @Override
            public String getStringValue() {
                return model.getSnapshotName();
            }
        }, "Name", snapshotAttributesLayout, model);

        // Enter DESCRIPTION
        WebUiUtils.createFieldString(new StringAttributeOperation() {
            @Override
            public void setStringValue(final String value) {
                model.setSnapshotDescription(value);
            }

            @Override
            public String getStringValue() {
                return model.getSnapshotDescription();
            }
        }, "Description", snapshotAttributesLayout, model);

        // Display UUID (not editable)
        final TextField snapUUID = new TextField("UUID", model.getItemUuid().toString());
        snapUUID.setReadOnly(true);
        snapUUID.setWidth("300px");
        snapshotAttributesLayout.addComponent(snapUUID);

        // Display size (not editable)
        final TextField snapSize = new TextField("Size", Long.toString(model.getSnapshotSize()));
        snapSize.setReadOnly(true);
        snapSize.setWidth("300px");
        snapshotAttributesLayout.addComponent(snapSize);

        return layout;
    }
}
