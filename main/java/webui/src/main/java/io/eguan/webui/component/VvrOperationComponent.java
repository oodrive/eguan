package io.eguan.webui.component;

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

import io.eguan.webui.WebUiResources;
import io.eguan.webui.component.WaitingComponent.Background;
import io.eguan.webui.component.window.ErrorWindow;
import io.eguan.webui.component.window.VvrAttributesWindow;
import io.eguan.webui.component.window.VvrDeleteWindow;
import io.eguan.webui.model.ModelCreator;
import io.eguan.webui.model.VvrManagerModel;
import io.eguan.webui.model.VvrModel;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.server.Resource;
import com.vaadin.ui.AbstractComponent;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.themes.Runo;

/**
 * The class represents the different operations which can be done on a VVR.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
@SuppressWarnings("serial")
public class VvrOperationComponent implements VvrComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(VvrOperationComponent.class);

    private static final String BUTTON_WIDTH = "75px";

    private final VvrManagerModel vvrManagerModel;

    public VvrOperationComponent(final VvrManagerModel vvrManagerModel) {
        this.vvrManagerModel = vvrManagerModel;
    }

    @Override
    public final AbstractComponent createComponent(final VvrModel model, final ModelCreator handler) {

        final HorizontalLayout operationLayout = new HorizontalLayout();
        operationLayout.setMargin(true);
        operationLayout.setSpacing(true);
        operationLayout.setWidth("100%");

        // Start and description buttons
        // START/STOP
        final Button startStop = new Button();
        startStop.setWidth(BUTTON_WIDTH);
        startStop.addStyleName(Runo.BUTTON_BIG);

        final Resource iconStartStop;
        final String description;
        if (!model.isVvrStarted()) {
            iconStartStop = WebUiResources.getStartIcon();
            description = "Start";
        }
        else {
            iconStartStop = WebUiResources.getStopIcon();
            description = "Stop";
        }
        startStop.setIcon(iconStartStop);
        startStop.setDescription(description);

        operationLayout.addComponent(startStop);
        operationLayout.setExpandRatio(startStop, 1f);
        operationLayout.setComponentAlignment(startStop, Alignment.MIDDLE_LEFT);

        final UUID vvrUuid = model.getItemUuid();
        startStop.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                final boolean started = model.isVvrStarted();
                // Start/Stop are done in background
                if (!started) {
                    WaitingComponent.executeBackground(model, new Background() {
                        @Override
                        public void processing() {
                            model.startVvr();
                        }

                        @Override
                        public void postProcessing() {
                            startStop.setIcon(WebUiResources.getStopIcon());
                            startStop.setDescription("Stop");
                            Notification.show("VVR started ", vvrUuid.toString(), Notification.Type.TRAY_NOTIFICATION);
                        }
                    });
                }
                else {
                    WaitingComponent.executeBackground(model, new Background() {
                        @Override
                        public void processing() {
                            model.stopVvr();
                        }

                        @Override
                        public void postProcessing() {
                            startStop.setIcon(WebUiResources.getStartIcon());
                            startStop.setDescription("Start");
                            Notification.show("VVR stopped ", vvrUuid.toString(), Notification.Type.TRAY_NOTIFICATION);
                        }
                    });
                }
            }
        });

        // ATTRIBUTES
        final Button attributes = new Button();
        attributes.addStyleName(Runo.BUTTON_BIG);
        attributes.setWidth(BUTTON_WIDTH);

        operationLayout.addComponent(attributes);
        operationLayout.setExpandRatio(attributes, 1f);
        operationLayout.setComponentAlignment(attributes, Alignment.MIDDLE_LEFT);
        attributes.setIcon(WebUiResources.getSettingsIcon());
        attributes.setDescription("Settings");

        attributes.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                try {
                    final VvrAttributesWindow attributesWindow = new VvrAttributesWindow(vvrUuid);
                    attributesWindow.add(model);
                }
                catch (final Exception e) {
                    LOGGER.error("Can not get VVR attributes: ", e);
                    final ErrorWindow err = new ErrorWindow("Can not display VVR Attributes: " + e.getMessage());
                    err.add(model);
                }
            }
        });

        // DELETE
        final Button delete = new Button();
        delete.addStyleName(Runo.BUTTON_BIG);
        delete.setWidth(BUTTON_WIDTH);
        delete.setIcon(WebUiResources.getTrashIcon());
        delete.setDescription("Delete");

        operationLayout.addComponent(delete);
        operationLayout.setExpandRatio(delete, 12f);
        operationLayout.setComponentAlignment(delete, Alignment.MIDDLE_RIGHT);

        delete.addClickListener(new Button.ClickListener() {
            @Override
            public void buttonClick(final ClickEvent event) {
                try {
                    final VvrDeleteWindow deleteWindow = new VvrDeleteWindow(vvrUuid);
                    deleteWindow.add(vvrManagerModel);
                }
                catch (final Exception e) {
                    LOGGER.error("Can not delete VVR: ", e);
                    final ErrorWindow err = new ErrorWindow("Can not delete VVR: " + e.getMessage());
                    err.add(model);
                }
            }
        });
        return operationLayout;
    }
}
