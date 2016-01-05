package io.eguan.webui.component;

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

import io.eguan.webui.WebUiResources;
import io.eguan.webui.model.AbstractItemModel;
import io.eguan.webui.model.DeviceModel;
import io.eguan.webui.model.ModelCreator;
import io.eguan.webui.model.SnapshotModel;
import io.eguan.webui.model.VvrModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.vaadin.data.Property.ValueChangeEvent;
import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.ui.AbstractComponent;
import com.vaadin.ui.Tree;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.BaseTheme;

/**
 * The class represents the VVR as a tree of devices and snapshots.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public class VvrTreeComponent implements VvrComponent {

    private Tree vvrTree;
    private final VerticalLayout treeItemlayout;
    private TreeItemComponent selectedItem;
    private final Map<UUID, AbstractItemModel> items = new HashMap<>();

    public VvrTreeComponent(final VerticalLayout layout) {
        this.treeItemlayout = layout;
    }

    @SuppressWarnings({ "serial" })
    @Override
    public final AbstractComponent createComponent(final VvrModel model, final ModelCreator handler) {
        vvrTree = new Tree();
        vvrTree.setChildrenAllowed(vvrTree, true);
        vvrTree.setSizeFull();
        vvrTree.setImmediate(true);
        vvrTree.addStyleName(BaseTheme.TREE_CONNECTORS);

        // Add snapshots on the tree
        final Set<UUID> snapshotUuidList = model.getSnapshotsList();
        for (final UUID snapshotUuid : snapshotUuidList) {
            addSnapshot(handler.createSnapshotModel(model, snapshotUuid));
        }
        // Add devices on the tree
        final Set<UUID> deviceUuidList = model.getDevicesList();
        for (final UUID deviceUuid : deviceUuidList) {
            addDevice(handler.createDeviceModel(model, deviceUuid));
        }

        vvrTree.addValueChangeListener(new ValueChangeListener() {
            @Override
            public void valueChange(final ValueChangeEvent event) {
                final Object id = vvrTree.getValue();

                if (id != null) {
                    if (id instanceof SnapshotModel) {
                        final SnapshotModel snapshotModel = (SnapshotModel) id;
                        treeItemlayout.removeAllComponents();
                        selectedItem = new SnapshotItemComponent(snapshotModel);
                        treeItemlayout.addComponent(selectedItem.init());
                    }
                    else if (id instanceof DeviceModel) {
                        final DeviceModel deviceModel = (DeviceModel) id;
                        treeItemlayout.removeAllComponents();
                        selectedItem = new DeviceItemComponent(deviceModel);
                        treeItemlayout.addComponent(selectedItem.init());
                    }
                }
                else {
                    // Delete all
                    treeItemlayout.removeAllComponents();
                }
            }
        });
        return vvrTree;
    }

    /**
     * Add a new snapshot to the VVR tree.
     * 
     * @param snapshotModel
     *            the new snapshot
     */
    public final void addSnapshot(final SnapshotModel snapshotModel) {

        // Add item to the tree
        vvrTree.addItem(snapshotModel);
        items.put(snapshotModel.getItemUuid(), snapshotModel);

        // Configure it
        final String name = snapshotModel.getSnapshotName();
        if (name.isEmpty() || name == null) {
            vvrTree.setItemCaption(snapshotModel, snapshotModel.getItemUuid().toString());
        }
        else {
            vvrTree.setItemCaption(snapshotModel, name);
        }
        vvrTree.setItemIcon(snapshotModel, WebUiResources.getSnapshotTreeIcon());
        vvrTree.setItemIconAlternateText(snapshotModel, "snapshot");
        vvrTree.areChildrenAllowed(snapshotModel);
        vvrTree.expandItemsRecursively(snapshotModel);

        // Add snapshot parent (if null, node is detached)
        final UUID newParentId = snapshotModel.getSnapshotParent();
        vvrTree.setParent(snapshotModel, items.get(newParentId));

        // If child is already present, set its parent. Otherwise it will be set when child will appear
        final UUID[] childrenDevices = snapshotModel.getSnapshotChildrenDevices();
        for (final UUID child : childrenDevices) {

            final AbstractItemModel childModel = items.get(child);
            if (childModel != null) {
                vvrTree.setParent(childModel, snapshotModel);
            }
        }
        final UUID[] childrenSnapshots = snapshotModel.getSnapshotChildrenSnapshots();
        for (final UUID child : childrenSnapshots) {
            final AbstractItemModel childModel = items.get(child);
            if (childModel != null) {
                vvrTree.setParent(childModel, snapshotModel);
            }
        }
    }

    /**
     * Remove a snapshot from the tree.
     * 
     * @param uuid
     *            the UUID of the snapshot to remove
     */
    public final void removeSnapshot(final UUID uuid) {

        final SnapshotModel snapshotModel = (SnapshotModel) items.remove(uuid);
        final Object parentId = vvrTree.getParent(snapshotModel);

        // Set snapshot parent to snapshot children
        final Collection<?> children = vvrTree.getChildren(snapshotModel);

        if (children != null) {
            // Copy children in a list
            final ArrayList<AbstractItemModel> childrenList = new ArrayList<>();
            for (final Object child : children) {
                childrenList.add((AbstractItemModel) child);
            }
            // Change their parent
            for (final AbstractItemModel child : childrenList) {
                vvrTree.setParent(child, parentId);
            }
        }
        vvrTree.removeItem(snapshotModel);
    }

    /**
     * Modify the name of a snapshot.
     * 
     * @param snapshotUuid
     *            the UUID of the snapshot to rename
     * @param name
     *            the new name of the snapshot
     */
    public final void modifySnapshotName(final UUID snapshotUuid, final String name) {
        final SnapshotModel snapshotModel = (SnapshotModel) items.get(snapshotUuid);

        // Change name
        if (name.isEmpty() || name == null) {
            vvrTree.setItemCaption(snapshotModel, snapshotModel.getItemUuid().toString());
        }
        else {
            vvrTree.setItemCaption(snapshotModel, name);
        }

        modifySnapshotLayout(snapshotUuid);
    }

    /**
     * Modify the description of a snapshot.
     * 
     * @param snapshotUuid
     *            the UUID of the snapshot
     * @param description
     *            the new description
     */
    public void modifySnapshotDescription(final UUID snapshotUuid, final String description) {
        modifySnapshotLayout(snapshotUuid);
    }

    /**
     * Modify the snapshot layout. Reload all the attributes.
     * 
     * @param snapshotUuid
     *            the snapshot UUID to change
     */
    private final void modifySnapshotLayout(final UUID snapshotUuid) {
        // Update item layout only if selected
        final Object id = vvrTree.getValue();
        if (id instanceof SnapshotModel) {
            final SnapshotModel snapshotModel = (SnapshotModel) items.get(snapshotUuid);
            final SnapshotModel snapshotModelSelected = (SnapshotModel) id;
            if (snapshotModel.equals(snapshotModelSelected)) {
                ((SnapshotItemComponent) selectedItem).updateAttributes();
            }
        }
    }

    /**
     * Add a new device to the VVR tree
     * 
     * @param model
     *            the device model to add
     */
    public final void addDevice(final DeviceModel model) {

        // Add item to the tree
        vvrTree.addItem(model);
        vvrTree.setItemCaption(model, model.getDeviceName());
        vvrTree.setItemIcon(model, WebUiResources.getDeviceTreeIcon());

        items.put(model.getItemUuid(), model);
        // Add device parent (if null, node is detached)
        final UUID newParentId = model.getDeviceParent();
        vvrTree.setParent(model, items.get(newParentId));
        vvrTree.setItemIconAlternateText(model, "device");
        vvrTree.setChildrenAllowed(model, false);
    }

    /**
     * Remove a device from the VVR tree
     * 
     * @param uuid
     *            the UUID of the device to remove.
     */
    public final void removeDevice(final UUID uuid) {
        final DeviceModel deviceModel = (DeviceModel) items.remove(uuid);
        vvrTree.removeItem(deviceModel);
    }

    /**
     * Modify the device name.
     * 
     * @param deviceUuid
     *            the UUID of the device to rename
     * @param name
     *            the new name of the device
     */
    public void modifyDeviceName(final UUID deviceUuid, final String name) {
        final DeviceModel deviceModel = (DeviceModel) items.get(deviceUuid);

        // Change name
        if (name.isEmpty() || name == null) {
            vvrTree.setItemCaption(deviceModel, deviceModel.getItemUuid().toString());
        }
        else {
            vvrTree.setItemCaption(deviceModel, name);
        }

        modifyDeviceLayout(deviceUuid);
    }

    /**
     * Modify the device description.
     * 
     * @param deviceUuid
     *            the device uuid
     * @param description
     *            the new description
     */
    public final void modifyDeviceDescription(final UUID deviceUuid, final String name) {
        modifyDeviceLayout(deviceUuid);
    }

    /**
     * Modify the layout of the device.
     * 
     * @param deviceUuid
     *            the device UUID
     */
    private final void modifyDeviceLayout(final UUID deviceUuid) {

        // Update item layout if selected
        final Object id = vvrTree.getValue();
        if (id instanceof DeviceModel) {
            final DeviceModel snapshotModel = (DeviceModel) items.get(deviceUuid);
            final DeviceModel snapshotModelSelected = (DeviceModel) id;
            if (snapshotModel.equals(snapshotModelSelected)) {
                ((DeviceItemComponent) selectedItem).updateAttributes();
            }
        }
    }

}
