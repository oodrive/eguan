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

import io.eguan.webui.VvrManagerUi;
import io.eguan.webui.component.window.ErrorWindow;
import io.eguan.webui.model.AbstractItemModel;

import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

public final class WaitingComponent {

    /**
     * If action success, the post processing is executed.
     * 
     * 
     */
    public interface Background {
        void processing();

        void postProcessing();
    }

    /**
     * Create a waiting window, execute action in another thread and execute post processing if previous action succeed.
     * Add an error window in case of exception during action execution.
     * 
     * @param currentUi
     * @param model
     * @param action
     * @param postProcessing
     *            can be null.
     */
    public static final void executeBackground(final AbstractItemModel model, final Background back) {

        final VvrManagerUi vvrManagerUi = (VvrManagerUi) UI.getCurrent();
        final VerticalLayout progressLayout = vvrManagerUi.addProgressBar();

        // Execute action in a parallel thread, to not block the UI
        final Thread actionThread = new Thread() {
            @Override
            public void run() {
                try {
                    back.processing();

                    // Need locked access, due to external thread
                    vvrManagerUi.access(new Runnable() {
                        @Override
                        public void run() {
                            back.postProcessing();
                        }
                    }).get();
                }
                catch (final Exception e) {
                    // Need locked access, due to external thread
                    vvrManagerUi.access(new Runnable() {
                        @Override
                        public void run() {
                            final ErrorWindow err = new ErrorWindow("Can not execute operation: " + e.getMessage());
                            err.add(model);
                        }
                    });
                }
                finally {
                    // Action ends, close the waiting window
                    vvrManagerUi.access(new Runnable() {
                        @Override
                        public void run() {
                            vvrManagerUi.removeProgressBar(progressLayout);
                        }
                    });
                }
            }
        };
        actionThread.start();
    }

}
