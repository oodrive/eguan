package com.oodrive.nuage.webui;

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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import javax.servlet.annotation.WebServlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.webui.component.VvrAttributesComponent;
import com.oodrive.nuage.webui.component.VvrOperationComponent;
import com.oodrive.nuage.webui.component.VvrTreeComponent;
import com.oodrive.nuage.webui.component.window.VvrCreateWindow;
import com.oodrive.nuage.webui.component.window.VvrCreateWindow.PostProcessing;
import com.oodrive.nuage.webui.jmx.JmxHandler;
import com.oodrive.nuage.webui.model.VvrManagerModel;
import com.oodrive.nuage.webui.model.VvrModel;
import com.vaadin.annotations.Theme;
import com.vaadin.annotations.VaadinServletConfiguration;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServlet;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.AbsoluteLayout;
import com.vaadin.ui.AbstractComponent;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.Label;
import com.vaadin.ui.ProgressBar;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.TabSheet.SelectedTabChangeEvent;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

/**
 * Web UI class. Used for VVRs administration.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */

@Theme("mytheme")
@SuppressWarnings("serial")
public class VvrManagerUi extends UI {

    @WebServlet(value = { "/*" }, asyncSupported = true)
    @VaadinServletConfiguration(productionMode = false, ui = VvrManagerUi.class, widgetset = "com.vaadin.DefaultWidgetSet")
    public static class VvrManagerUiServlet extends VaadinServlet {

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(VvrManagerUi.class);

    // Width and Height of layouts
    private static final String rootLayoutWidth = "1280px";
    private static final int opLayoutHeightInt = 62;
    private static final String opLayoutHeight = opLayoutHeightInt + "px";
    private static final int labelLayoutHeightInt = 10;
    private static final String labelLayoutHeight = labelLayoutHeightInt + "px";
    private static final int panelLayoutHeightInt = 700;
    private static final String panelLayoutHeight = panelLayoutHeightInt + "px";

    // +2 for the line between operation layout and panel.
    private static final String lastLayoutHeight = (opLayoutHeightInt + labelLayoutHeightInt + panelLayoutHeightInt + 2)
            + "px";

    // Root layout
    private final AbsoluteLayout rootLayout = new AbsoluteLayout();
    // Middle layout
    private final VerticalLayout vvrManagerLayout = new VerticalLayout();
    // Tabsheet for each VVR
    private TabSheet vvrsTabsheet;
    // Map to keep a reference on the tree of each VVR
    private final HashMap<UUID, VvrTreeComponent> vvrTreeComponents = new HashMap<>();
    // Map to keep a reference on the layout of each VVR
    private final HashMap<UUID, VerticalLayout> vvrLayouts = new HashMap<>();
    // Map to keep a reference on the VVR model
    private final HashMap<UUID, VvrModel> vvrModels = new HashMap<>();

    // Handler JMX
    private final JmxHandler jmxHandler = new JmxHandler(this);
    // Model for VVR Manager
    private VvrManagerModel vvrManagerModel;

    @Override
    protected void init(final VaadinRequest request) {

        // Listener to disconnect JMX connection on exit.
        addDetachListener(new DetachListener() {
            @Override
            public void detach(final DetachEvent event) {
                jmxHandler.disconnect();
            }
        });
        setPollInterval(1000);

        final Label labelLeft = new Label("");
        final Label labelRight = new Label("");

        labelLeft.setHeight("100%");
        labelRight.setHeight("100%");

        final HorizontalLayout content = new HorizontalLayout();
        content.setSizeFull();

        rootLayout.setWidth(rootLayoutWidth);
        rootLayout.setImmediate(true);
        rootLayout.addComponent(vvrManagerLayout);
        vvrManagerLayout.setMargin(false);
        vvrManagerLayout.setSpacing(true);
        vvrManagerLayout.setWidth(rootLayoutWidth);

        content.addComponent(labelLeft);
        content.addComponent(rootLayout);
        content.addComponent(labelRight);

        content.setExpandRatio(labelLeft, 0.5f);
        content.setExpandRatio(labelRight, 0.5f);

        content.setComponentAlignment(rootLayout, Alignment.TOP_CENTER);
        setContent(content);

        // Init Jmx Handler
        try {
            jmxHandler.connect();
            // Init ui
            initVvrManagerUi(jmxHandler);
        }
        catch (final Exception e) {
            LOGGER.error("Can not connect to JMX", e);
        }
    }

    /**
     * Initialize VVRs representation.
     * 
     * @param jmxHandler
     */
    private final void initVvrManagerUi(final JmxHandler jmxHandler) {

        // Create Model for vvrManager
        vvrManagerModel = jmxHandler.createVvrManagerModel();

        // Vvr representation
        vvrsTabsheet = new TabSheet();
        vvrManagerLayout.addComponent(vvrsTabsheet);

        // Sheet 0 create new VVR
        final VerticalLayout lastLayout = new VerticalLayout();
        lastLayout.setWidth("100%");
        lastLayout.setHeight(lastLayoutHeight);
        vvrsTabsheet.addTab(lastLayout, "+");

        // Add a sheet for each vvr
        final Set<UUID> vvrUuidList = vvrManagerModel.getVvrs();
        for (final UUID vvrUuid : vvrUuidList) {
            addVvr(vvrUuid);
        }

        // Window to create a new vvr (display on the last tabsheet)
        final VvrCreateWindow createWindow = new VvrCreateWindow(new PostProcessing() {
            @Override
            public void execute() {
                // After creation select the first tab
                vvrsTabsheet.setSelectedTab(0);
            }
        });

        vvrsTabsheet.addSelectedTabChangeListener(new TabSheet.SelectedTabChangeListener() {
            @Override
            public void selectedTabChange(final SelectedTabChangeEvent event) {
                final TabSheet tabsheet = event.getTabSheet();
                final String caption = tabsheet.getTab(tabsheet.getSelectedTab()).getCaption();

                if (caption.equals("+")) {
                    createWindow.add(vvrManagerModel);
                }
                else {
                    // Remove window if another tab is selected
                    createWindow.remove();
                }
            }
        });

        // If no other tab, display creation window (+ tab can not be selected)
        if (vvrUuidList.isEmpty()) {
            createWindow.add(vvrManagerModel);
        }
    }

    /**
     * Add a progress bar for long task.
     * 
     * @return the layout
     */
    public VerticalLayout addProgressBar() {
        final VerticalLayout layout = new VerticalLayout();
        layout.setSizeFull();

        final ProgressBar bar = new ProgressBar();
        layout.addComponent(new Label("This operation can take some time. Please wait..."));
        bar.setIndeterminate(true);
        bar.setImmediate(true);

        layout.addComponent(bar);
        layout.setComponentAlignment(bar, Alignment.MIDDLE_CENTER);

        rootLayout.addComponent(layout, "top: 50%; left: 39%");
        vvrManagerLayout.setEnabled(false);
        return layout;
    }

    /**
     * Remove the progress bar.
     * 
     * @param layout
     */
    public final void removeProgressBar(final VerticalLayout layout) {
        rootLayout.removeComponent(layout);
        vvrManagerLayout.setEnabled(true);

        // Set enabled all the component contained in the VVR manager layout
        final Iterator<Component> iterate = vvrManagerLayout.iterator();
        while (iterate.hasNext()) {
            final Component c = iterate.next();
            c.setEnabled(true);
        }
    }

    /**
     * Add a VVR user interface.
     * 
     * @param vvrUuid
     *            the vvr unique identifier
     * 
     */
    public final void addVvr(final UUID vvrUuid) {

        // Create a vvr model
        final VvrModel vvrModel = jmxHandler.createVvrModel(vvrUuid);
        vvrModels.put(vvrUuid, vvrModel);

        // Layout for the first component
        final VerticalLayout vvrLayout = new VerticalLayout();
        vvrLayout.setWidth("100%");

        vvrsTabsheet.addTab(vvrLayout, vvrModel.getVvrName(), null, vvrsTabsheet.getComponentCount() - 1);
        vvrLayouts.put(vvrUuid, vvrLayout);

        // Create component for vvr operations
        final VvrOperationComponent op = new VvrOperationComponent(vvrManagerModel);
        final AbstractComponent opComponent = op.createComponent(vvrModel, jmxHandler);
        opComponent.setHeight(opLayoutHeight);
        vvrLayout.addComponent(opComponent);

        final Label label = new Label("&nbsp", ContentMode.HTML);
        label.setHeight(labelLayoutHeight);
        vvrLayout.addComponent(label);
        vvrLayout.addComponent(new Label("<hr />", ContentMode.HTML));

        // Create Tool tip for attributes
        final VvrAttributesComponent attr = new VvrAttributesComponent(vvrUuid);
        vvrsTabsheet.getTab(vvrLayout).setDescription(attr.createComponent(vvrModel));

        // If there was only the + sheet, select the new vvr sheet
        if (vvrsTabsheet.getComponentCount() == 2) {
            vvrsTabsheet.setSelectedTab(0);
        }

        // Create its panel
        final HorizontalSplitPanel panel = new HorizontalSplitPanel();
        vvrLayout.addComponent(panel);
        panel.setWidth("100%");
        panel.setHeight(panelLayoutHeight);
        panel.setSplitPosition(35);

        // Component to display snapshot/device atributes
        final VerticalLayout vvrTreeLayout = new VerticalLayout();
        final VvrTreeComponent vvrTreeComponent = new VvrTreeComponent(vvrTreeLayout);
        panel.setFirstComponent(vvrTreeComponent.createComponent(vvrModel, jmxHandler));
        panel.setSecondComponent(vvrTreeLayout);

        vvrTreeComponents.put(vvrUuid, vvrTreeComponent);
    }

    /**
     * Remove vvr.
     * 
     * @param vvrUuid
     *            the VVR unique identifier
     */
    public final void removeVvr(final UUID vvrUuid) {

        // Remove model
        vvrModels.remove(vvrUuid);

        // Remove tree
        vvrTreeComponents.remove(vvrUuid);

        // Remove Split panel
        vvrsTabsheet.removeComponent(vvrLayouts.remove(vvrUuid));
    }

    /**
     * Modify VVR name.
     * 
     * @param vvrUuid
     *            the vvr unique identifier
     * 
     * @param newValue
     *            the new name of the VVR
     */
    public void modifyVvrName(final UUID vvrUuid, final String newValue) {
        final VerticalLayout vvrLayout = vvrLayouts.get(vvrUuid);
        vvrsTabsheet.getTab(vvrLayout).setCaption(newValue);
    }

    /**
     * Add snapshot user interface.
     * 
     * @param vvrUuid
     *            the vvr unique identifier
     * 
     * @param snapshotUuid
     *            the snapshot unique identifier
     * 
     */
    public final void addSnapshot(final UUID vvrUuid, final UUID snapshotUuid) {
        final VvrTreeComponent c = vvrTreeComponents.get(vvrUuid);
        if (c != null) {
            final VvrModel vvr = vvrModels.get(vvrUuid);
            c.addSnapshot(jmxHandler.createSnapshotModel(vvr, snapshotUuid));
        }
    }

    /**
     * Add snapshot user interface.
     * 
     * @param vvrUuid
     *            the VVR unique identifier
     * @param snapshotUuid
     *            the snapshot unique identifier
     */
    public final void removeSnapshot(final UUID vvrUuid, final UUID snapshotUuid) {
        final VvrTreeComponent c = vvrTreeComponents.get(vvrUuid);
        if (c != null) {
            c.removeSnapshot(snapshotUuid);
        }
    }

    /**
     * Modify snapshot name.
     * 
     * @param vvrUuid
     *            the VVR unique identifier
     * @param snapshotUuid
     *            the snapshot unique identifier
     * @param newName
     *            the new snapshot name
     */
    public final void modifySnapshotName(final UUID vvrUuid, final UUID snapshotUuid, final String newName) {
        final VvrTreeComponent c = vvrTreeComponents.get(vvrUuid);
        if (c != null) {
            c.modifySnapshotName(snapshotUuid, newName);
        }
    }

    /**
     * Modify snapshot description.
     * 
     * @param vvrUuid
     *            the VVR unique identifier
     * @param snapshotUuid
     *            the snapshot unique identifier
     * @param newDesc
     *            the new snapshot description
     */
    public final void modifySnapshotDescription(final UUID vvrUuid, final UUID snapshotUuid, final String newDesc) {
        final VvrTreeComponent c = vvrTreeComponents.get(vvrUuid);
        if (c != null) {
            c.modifySnapshotDescription(snapshotUuid, newDesc);
        }
    }

    /**
     * Add device user interface.
     * 
     * @param vvrUuid
     *            the VVR unique identifier
     * @param deviceUuid
     *            the device unique identifier
     */
    public final void addDevice(final UUID vvrUuid, final UUID deviceUuid) {
        final VvrTreeComponent c = vvrTreeComponents.get(vvrUuid);
        if (c != null) {
            final VvrModel vvr = vvrModels.get(vvrUuid);
            c.addDevice(jmxHandler.createDeviceModel(vvr, deviceUuid));
        }
    }

    /**
     * Remove device user interface.
     * 
     * @param vvrUuid
     *            the VVR unique identifier
     * @param deviceUuid
     *            the device unique identifier
     */
    public final void removeDevice(final UUID vvrUuid, final UUID deviceUuid) {
        final VvrTreeComponent c = vvrTreeComponents.get(vvrUuid);
        if (c != null) {
            c.removeDevice(deviceUuid);
        }
    }

    /**
     * Modify device name.
     * 
     * @param vvrUuid
     *            the VVR unique identifier
     * @param deviceUuid
     *            the device unique identifier
     * @param newName
     *            the new device name
     */
    public final void modifyDeviceName(final UUID vvrUuid, final UUID deviceUuid, final String newName) {
        final VvrTreeComponent c = vvrTreeComponents.get(vvrUuid);
        if (c != null) {
            c.modifyDeviceName(deviceUuid, newName);
        }
    }

    /**
     * Modify device description.
     * 
     * @param vvrUuid
     *            the VVR unique identifier
     * @param deviceUuid
     *            the device unique identifier
     * @param newDesc
     *            the new device description
     */
    public final void modifyDeviceDescription(final UUID vvrUuid, final UUID deviceUuid, final String newDesc) {
        final VvrTreeComponent c = vvrTreeComponents.get(vvrUuid);
        if (c != null) {
            c.modifyDeviceDescription(deviceUuid, newDesc);
        }
    }

}
