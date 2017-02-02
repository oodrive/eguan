package io.eguan.webui.component.window;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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
import io.eguan.webui.model.SnapshotModel;

import java.util.UUID;

/**
 * The class used to confirm a snapshot deletion wih a window
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class SnapshotDeleteWindow extends AbstractConfirmationWindow {

    public SnapshotDeleteWindow(final UUID snapshotUuid) {
        super(new ActionModel() {
            @Override
            public void execute(final AbstractItemModel model) {
                try {
                    ((SnapshotModel) model).deleteSnapshot();
                }
                catch (final Throwable t) {
                    final ErrorWindow err = new ErrorWindow("Snapshot not deleted: " + t.getMessage());
                    err.add(model);
                    throw t;
                }
            }
        }, "Are you sure you want to delete snapshot: " + snapshotUuid + "?");
    }
}
