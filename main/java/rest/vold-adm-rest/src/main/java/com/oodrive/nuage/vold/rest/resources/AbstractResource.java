package com.oodrive.nuage.vold.rest.resources;

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

import java.net.URI;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXB;

import com.oodrive.nuage.vold.rest.generated.model.ObjectFactory;

/**
 * Abstract superclass of all REST Resource implementations.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public abstract class AbstractResource {

    /**
     * The JAXB-generated {@link ObjectFactory}
     */
    private static final ObjectFactory objectFactory = new ObjectFactory();

    /**
     * Gets the ObjectFactory instance used to fabricate all bean instances.
     * 
     * @return the {@link JAXB} object factory
     */
    protected static final ObjectFactory getObjectFactory() {
        return objectFactory;
    }

    @Context
    public UriInfo uriInfo;

    /**
     * Gets the resource's {@link URI}.
     * 
     * @return an absolute {@link URI}
     */
    protected abstract URI getResourceUri();

}
