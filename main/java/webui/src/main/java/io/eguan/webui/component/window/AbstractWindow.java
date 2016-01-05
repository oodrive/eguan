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

import com.vaadin.ui.UI;
import com.vaadin.ui.Window;

/**
 * Abstract class used to create window.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public abstract class AbstractWindow {

    private final boolean isModal;

    public AbstractWindow(final boolean isModal) {
        this.isModal = isModal;
    }

    protected abstract Window init(final AbstractItemModel model);

    /**
     * Init and add a window to the current UI with a given model.
     * 
     * @param model
     */
    public void add(final AbstractItemModel model) {
        final Window window = init(model);
        // True to avoid parallel modifications
        window.setModal(isModal);
        window.setImmediate(true);
        UI.getCurrent().addWindow(window);
    }
}
