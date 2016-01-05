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

import io.eguan.webui.model.VvrModel;

import java.util.UUID;

/**
 * Summarize the VVR attributes.
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class VvrAttributesComponent {

    private final UUID vvrUuid;

    public VvrAttributesComponent(final UUID vvrUuid) {
        this.vvrUuid = vvrUuid;
    }

    public final String createComponent(final VvrModel model) {
        return "<ul>" + "  <li>uuid : " + vvrUuid.toString() + "</li>" + "  <li>name : " + model.getVvrName() + "</li>"
                + "  <li>description : " + model.getVvrDescription() + "</li>" + "</ul>";
    }
}
