package com.oodrive.nuage.vold.rest.providers;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlRootElement;

import com.oodrive.nuage.vold.rest.generated.model.ObjectFactory;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;

@Provider
public class JsonFormatConfProvider implements ContextResolver<JAXBContext> {
    static {
        // adds all types returned by createXX() methods of the JAXB ObjectFactory to the list
        final ArrayList<Class<?>> typesList = new ArrayList<Class<?>>();
        for (final Method currObjFactoryMethod : ObjectFactory.class.getMethods()) {
            if (!currObjFactoryMethod.getName().startsWith("create")) {
                continue;
            }
            final Class<?> type = currObjFactoryMethod.getReturnType();
            if (type.isAnnotationPresent(XmlRootElement.class)) {
                typesList.add(type);
            }
        }
        types = Collections.unmodifiableList(typesList);
    }

    private static final List<Class<?>> types;

    private final JAXBContext context;

    public JsonFormatConfProvider() throws JAXBException {
        this.context = new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(true).build(),
                types.toArray(new Class[types.size()]));
    }

    @Override
    public JAXBContext getContext(final Class<?> type) {
        if (types.contains(type)) {
            return context;
        }
        return null;
    }

}
