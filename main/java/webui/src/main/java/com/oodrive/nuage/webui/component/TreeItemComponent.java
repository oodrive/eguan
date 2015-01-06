package com.oodrive.nuage.webui.component;

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

import com.vaadin.ui.AbstractComponent;

/**
 * Interface used to initialize item of the tree.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
abstract interface TreeItemComponent {

    /**
     * Init the different components used to represent an item
     * 
     * @return the root component for an item
     */
    AbstractComponent init();

}
