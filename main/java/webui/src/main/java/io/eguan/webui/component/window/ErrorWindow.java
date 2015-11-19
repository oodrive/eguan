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

import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

/**
 * The class used to display error message in a window.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class ErrorWindow extends AbstractWindow {

    /* the message to display */
    private final String message;

    public ErrorWindow(final String message) {
        super(true);
        this.message = message;
    }

    @SuppressWarnings("serial")
    @Override
    public final Window init(final AbstractItemModel model) {
        // Add new window with title "Error"
        final Window vvrErrorWindow = new Window("Error");
        vvrErrorWindow.center();
        vvrErrorWindow.setResizable(false);
        final VerticalLayout vvrErrorLayout = new VerticalLayout();
        vvrErrorLayout.setMargin(true);
        vvrErrorWindow.setContent(vvrErrorLayout);

        // Display message
        final Label errorMessage = new Label(message);
        vvrErrorLayout.addComponent(errorMessage);
        vvrErrorLayout.setComponentAlignment(errorMessage, Alignment.MIDDLE_CENTER);
        vvrErrorLayout.setSpacing(true);

        // Button OK
        final Button okButton = new Button("OK");
        vvrErrorLayout.addComponent(okButton);
        vvrErrorLayout.setComponentAlignment(okButton, Alignment.BOTTOM_CENTER);
        okButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                // Just close the window
                vvrErrorWindow.close();
            }
        });
        return vvrErrorWindow;
    }

}
