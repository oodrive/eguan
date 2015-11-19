package io.eguan.webui;

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

import io.eguan.webui.component.WaitingComponent;
import io.eguan.webui.component.WaitingComponent.Background;
import io.eguan.webui.component.window.ErrorWindow;
import io.eguan.webui.model.AbstractItemModel;

import com.vaadin.data.Property.ValueChangeEvent;
import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextField;

/**
 * Utility class to create object attributes
 * 
 * @author oodrive
 * @author ebredzinski
 * 
 */
public final class WebUiUtils {

    /**
     * Getter and setter for operation with a long.
     * 
     * 
     */
    public interface LongAttributeOperation {
        void setLongValue(final long value);

        long getLongValue();
    }

    /**
     * Getter and setter for operation with an integer.
     * 
     * 
     */
    public interface IntegerAttributeOperation {
        void setIntegerValue(final int value);

        int getIntegerValue();
    }

    /**
     * Getter and setter for operations with a String
     * 
     * 
     */
    public interface StringAttributeOperation {
        void setStringValue(final String value);

        String getStringValue();
    }

    /**
     * Create a new String text field with a given setter/getter.
     * 
     * @param operation
     *            the operation to get/set the value contains in the text field.
     * @param fieldName
     *            the name of the field
     * @param rootLayout
     *            the layout to add the text field
     * @param model
     *            the model used to get/set the value
     */
    @SuppressWarnings({ "serial" })
    public static final void createFieldString(final StringAttributeOperation operation, final String fieldName,
            final FormLayout rootLayout, final AbstractItemModel model) {

        String value = operation.getStringValue();
        // Display empty string if value is null
        if (value == null) {
            value = "";
        }
        final TextField field = new TextField(fieldName, value);
        field.setWidth("250px");
        field.setImmediate(true);
        rootLayout.addComponent(field);

        field.addValueChangeListener(new ValueChangeListener() {
            @Override
            public void valueChange(final ValueChangeEvent event) {
                final String newValue = String.valueOf(event.getProperty().getValue());
                final String oldValue = operation.getStringValue();
                if (!newValue.equals(oldValue)) {
                    // Set value in background (could be a long operation)
                    WaitingComponent.executeBackground(model, new Background() {
                        @Override
                        public void processing() {
                            operation.setStringValue(newValue);
                        }

                        @Override
                        public void postProcessing() {
                            Notification.show(fieldName + " changed", newValue, Notification.Type.TRAY_NOTIFICATION);
                        }
                    });

                }
            }
        });
    }

    /**
     * Create a new long text field with a given setter/getter.
     * 
     * @param operation
     *            the operation to get/set the value contains in the text field.
     * @param fieldName
     *            the name of the field
     * @param rootLayout
     *            the layout to add the text field
     * @param model
     *            the model used to get/set the value
     */
    @SuppressWarnings({ "serial" })
    public static final void createFieldLong(final LongAttributeOperation operation, final String fieldName,
            final FormLayout rootLayout, final AbstractItemModel model) {

        final TextField field = new TextField(fieldName, Long.toString(operation.getLongValue()));
        field.setWidth("250px");
        field.setImmediate(true);
        rootLayout.addComponent(field);

        field.addValueChangeListener(new ValueChangeListener() {
            @Override
            public void valueChange(final ValueChangeEvent event) {
                final String newValue = String.valueOf(event.getProperty().getValue());
                final String oldValue = Long.toString(operation.getLongValue());
                if (!oldValue.equals(newValue)) {
                    try {
                        // Cast here to check format
                        final long longNewValue = Long.valueOf(newValue);

                        // Set value in background (could be a long operation)
                        WaitingComponent.executeBackground(model, new Background() {
                            @Override
                            public void processing() {
                                operation.setLongValue(longNewValue);
                            }

                            @Override
                            public void postProcessing() {
                                // Get the new real value set in the model (can be different)
                                final String newValue = Long.toString(operation.getLongValue());
                                Notification
                                        .show(fieldName + " changed", newValue, Notification.Type.TRAY_NOTIFICATION);
                                // Reset the field with the new value
                                field.setValue(newValue);
                            }
                        });
                    }
                    catch (final NumberFormatException e) {
                        final ErrorWindow err = new ErrorWindow("You must enter a valid number");
                        err.add(model);
                        // Reset the last value
                        field.setValue(oldValue);
                    }
                }
            }
        });
    }

    /**
     * Create a new integer text field with a given setter/getter.
     * 
     * @param operation
     *            the operation to get/set the value contains in the text field.
     * @param fieldName
     *            the name of the field
     * @param rootLayout
     *            the layout to add the text field
     * @param model
     *            the model used to get/set the value
     */
    @SuppressWarnings({ "serial" })
    public static final void createFieldInteger(final IntegerAttributeOperation operation, final String fieldName,
            final FormLayout rootLayout, final AbstractItemModel model, final boolean emptyAllowed) {

        final TextField field = new TextField(fieldName, Integer.toString(operation.getIntegerValue()));
        field.setWidth("250px");
        field.setImmediate(true);
        rootLayout.addComponent(field);

        field.addValueChangeListener(new ValueChangeListener() {
            @Override
            public void valueChange(final ValueChangeEvent event) {
                final String newValue = String.valueOf(event.getProperty().getValue());
                final String oldValue = Integer.toString(operation.getIntegerValue());
                if (!oldValue.equals(newValue)) {
                    try {
                        final int intNewValue;

                        if (emptyAllowed && "".equals(newValue)) {
                            // Empty equals 0
                            intNewValue = 0;
                        }
                        else {
                            // Cast here to check format
                            intNewValue = Integer.valueOf(newValue);
                        }

                        // Set value in background (could be a long operation)
                        WaitingComponent.executeBackground(model, new Background() {
                            @Override
                            public void processing() {
                                operation.setIntegerValue(intNewValue);
                            }

                            @Override
                            public void postProcessing() {
                                // Get the new real value set in the model (can be different)
                                final String newValue = Integer.toString(operation.getIntegerValue());
                                Notification
                                        .show(fieldName + " changed", newValue, Notification.Type.TRAY_NOTIFICATION);
                                // Reset the field with the new value
                                field.setValue(newValue);
                            }
                        });
                    }
                    catch (final NumberFormatException e) {
                        final ErrorWindow err = new ErrorWindow("You must enter a valid number");
                        err.add(model);
                        // Reset the last value
                        field.setValue(oldValue);
                    }
                }
            }
        });
    }
}
