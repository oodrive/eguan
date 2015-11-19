package io.eguan.webui.component.window;

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

import io.eguan.webui.model.AbstractItemModel;
import io.eguan.webui.model.VvrManagerModel;

import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.Window;

/**
 * The class used to create a new VVR.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class VvrCreateWindow extends AbstractWindow {

    /**
     * Represents the process to execute once the VVR is created.
     * 
     * 
     */
    public interface PostProcessing {
        public void execute();
    }

    private final Window vvrCreateWindow;
    private final PostProcessing postProcessing;

    public VvrCreateWindow(final PostProcessing postProcessing) {
        super(false);
        this.vvrCreateWindow = new Window("Create a new VVR");
        this.postProcessing = postProcessing;
    }

    @SuppressWarnings("serial")
    @Override
    public final Window init(final AbstractItemModel model) {

        // Add new window
        vvrCreateWindow.center();
        final FormLayout vvrCreateLayout = new FormLayout();
        vvrCreateLayout.setMargin(true);
        vvrCreateWindow.setContent(vvrCreateLayout);
        vvrCreateWindow.setResizable(false);
        vvrCreateWindow.setClosable(false);

        // Enter name
        final TextField vvrName = new TextField("Name");
        vvrName.setValue("");
        vvrCreateLayout.addComponent(vvrName);

        // Enter decription
        final TextField vvrDescription = new TextField("Description");
        vvrDescription.setValue("");
        vvrCreateLayout.addComponent(vvrDescription);

        // Button create
        final Button vvrCreateButton = new Button("Create");
        vvrCreateLayout.addComponent(vvrCreateButton);
        vvrCreateButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                try {
                    ((VvrManagerModel) model).createVvr(vvrName.getValue(), vvrDescription.getValue());
                    Notification.show("New VVR created", Notification.Type.TRAY_NOTIFICATION);
                    postProcessing.execute();
                }
                catch (final Exception e) {
                    final ErrorWindow err = new ErrorWindow("VVR not created: " + e.getMessage());
                    err.add(model);
                }
            }
        });
        return vvrCreateWindow;
    }

    /**
     * Remove the create Window.
     */
    public final void remove() {
        UI.getCurrent().removeWindow(vvrCreateWindow);
    }
}
