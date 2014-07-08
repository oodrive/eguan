package com.oodrive.nuage.webui.component.window;

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

import java.util.UUID;

import com.oodrive.nuage.webui.component.WaitingComponent;
import com.oodrive.nuage.webui.component.WaitingComponent.Background;
import com.oodrive.nuage.webui.model.AbstractItemModel;
import com.oodrive.nuage.webui.model.VvrModel;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

/**
 * The class used to display and modify the VVR attributes.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class VvrAttributesWindow extends AbstractWindow {

    private final UUID vvrUuid;

    public VvrAttributesWindow(final UUID vvrUuid) {
        super(true);
        this.vvrUuid = vvrUuid;
    }

    @SuppressWarnings("serial")
    @Override
    public final Window init(final AbstractItemModel model) {

        // Cast model in vvrModel
        final VvrModel vvrModel = (VvrModel) model;
        // Add new window
        final Window vvrAttributesWindow = new Window("VVR Attributes");
        vvrAttributesWindow.center();
        vvrAttributesWindow.setWidth("400px");
        vvrAttributesWindow.setResizable(false);

        final VerticalLayout layout = new VerticalLayout();
        vvrAttributesWindow.setContent(layout);
        layout.setMargin(true);

        final FormLayout vvrAttributesLayout = new FormLayout();
        layout.addComponent(vvrAttributesLayout);
        vvrAttributesLayout.setMargin(true);

        // Enter NAME
        String value = vvrModel.getVvrName();
        if (value == null) {
            value = "";
        }
        final TextField name = new TextField("Name", value);
        name.setSizeFull();
        vvrAttributesLayout.addComponent(name);

        // Enter description
        value = vvrModel.getVvrDescription();
        if (value == null) {
            value = "";
        }
        final TextField desc = new TextField("Description", value);
        desc.setSizeFull();
        vvrAttributesLayout.addComponent(desc);

        // Enter name
        final TextField vvrUUID = new TextField("UUID");
        vvrUUID.setValue(vvrUuid.toString());
        vvrUUID.setReadOnly(true);
        vvrUUID.setSizeFull();
        vvrAttributesLayout.addComponent(vvrUUID);

        // OK button
        final HorizontalLayout hzlayout = new HorizontalLayout();
        layout.addComponent(hzlayout);
        hzlayout.setSizeFull();

        final Button okButton = new Button("OK");
        hzlayout.addComponent(okButton);
        hzlayout.setComponentAlignment(okButton, Alignment.MIDDLE_CENTER);

        okButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                WaitingComponent.executeBackground(vvrModel, new Background() {
                    @Override
                    public void processing() {
                        vvrModel.setVvrName(name.getValue());
                        vvrModel.setVvrDescription(desc.getValue());
                    }

                    @Override
                    public void postProcessing() {
                    }
                });
                vvrAttributesWindow.close();
            }
        });

        // Cancel button
        final Button cancelButton = new Button("Cancel");
        hzlayout.addComponent(cancelButton);
        hzlayout.setComponentAlignment(cancelButton, Alignment.MIDDLE_CENTER);

        cancelButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                // Just close the window
                vvrAttributesWindow.close();
            }
        });
        return vvrAttributesWindow;
    }
}
