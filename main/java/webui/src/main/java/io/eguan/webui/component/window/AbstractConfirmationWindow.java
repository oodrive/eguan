package io.eguan.webui.component.window;

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

import io.eguan.webui.model.AbstractItemModel;

import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

/**
 * Abstract class use to create confirmation windows.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public abstract class AbstractConfirmationWindow extends AbstractWindow {

    /**
     * Interface used to execute action on a given model
     * 
     * 
     */
    public interface ActionModel {
        public void execute(final AbstractItemModel model);
    }

    /* Reference on the action to execute if OK is pressed */
    private final ActionModel action;
    /* Message confirmation to display */
    private final String confirmation;

    public AbstractConfirmationWindow(final ActionModel action, final String confirmation) {
        super(true);
        this.action = action;
        this.confirmation = confirmation;
    }

    @SuppressWarnings("serial")
    @Override
    public Window init(final AbstractItemModel model) {

        // Add new window
        final Window vvrConfirmationWindow = new Window("Confirmation");
        vvrConfirmationWindow.center();
        vvrConfirmationWindow.setResizable(false);
        final VerticalLayout vvrConfirmationLayout = new VerticalLayout();
        vvrConfirmationLayout.setMargin(true);
        vvrConfirmationWindow.setContent(vvrConfirmationLayout);

        // Message to display before buttons
        final Label confirmationMessage = new Label(confirmation);
        vvrConfirmationLayout.addComponent(confirmationMessage);
        vvrConfirmationLayout.setComponentAlignment(confirmationMessage, Alignment.MIDDLE_CENTER);
        vvrConfirmationLayout.setSpacing(true);

        final HorizontalLayout buttonLayout = new HorizontalLayout();
        vvrConfirmationLayout.addComponent(buttonLayout);
        // Button OK
        final Button okButton = new Button("OK");
        buttonLayout.setSizeFull();
        buttonLayout.addComponent(okButton);
        buttonLayout.setComponentAlignment(okButton, Alignment.MIDDLE_CENTER);
        okButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                try {
                    action.execute(model);
                    vvrConfirmationWindow.close();
                }
                catch (final Exception e) {
                    vvrConfirmationWindow.close();
                }
            }
        });

        // Button cancel
        final Button cancelButton = new Button("Cancel");
        buttonLayout.addComponent(cancelButton);
        buttonLayout.setComponentAlignment(cancelButton, Alignment.MIDDLE_CENTER);
        cancelButton.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                // Just close the window
                vvrConfirmationWindow.close();
            }
        });
        return vvrConfirmationWindow;
    }
}
