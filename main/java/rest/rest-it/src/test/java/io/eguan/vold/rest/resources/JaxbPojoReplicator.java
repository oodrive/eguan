package io.eguan.vold.rest.resources;

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

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.util.JAXBSource;
import javax.xml.namespace.QName;

import com.google.common.base.Strings;

/**
 * Replicator for JAXB-annotated POJOs.
 * 
 * This replicator only managed one class that must be annotated with either {@link XmlRootElement} or {@link XmlType}.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 * @param <T>
 *            the JAXB-annotated POJO class to manage
 */
public final class JaxbPojoReplicator<T extends Object> {

    private final Class<T> targetClass;

    private volatile Boolean initialized = Boolean.FALSE;

    @GuardedBy("initialized")
    private QName qName;

    @GuardedBy("initialized")
    private JAXBContext context;

    @GuardedBy("initialized")
    private Unmarshaller unmarshaller;

    /**
     * Constructs a replicator for instances of the given class.
     * 
     * @param targetClass
     *            the
     */
    public JaxbPojoReplicator(@Nonnull final Class<T> targetClass) {
        this.targetClass = Objects.requireNonNull(targetClass);
    }

    /**
     * Initializes the JAXB context classes needed for (de-)serialization-based replication.
     * 
     * @throws JAXBException
     *             if initialization fails
     */
    public final void init() throws JAXBException {
        synchronized (initialized) {
            context = JAXBContext.newInstance(targetClass);

            unmarshaller = context.createUnmarshaller();

            final XmlRootElement rootAnnotation = targetClass.getAnnotation(XmlRootElement.class);
            if ((rootAnnotation != null) && (!Strings.isNullOrEmpty(rootAnnotation.name()))) {
                this.qName = new QName(rootAnnotation.name());
            }
            else {
                final XmlType typeAnnotation = targetClass.getAnnotation(XmlType.class);
                if ((typeAnnotation == null) || (Strings.isNullOrEmpty(typeAnnotation.name()))) {
                    throw new IllegalStateException("No QName found for " + targetClass);
                }
                this.qName = new QName(typeAnnotation.name());
            }
            initialized = Boolean.TRUE;
        }
    }

    /**
     * Returns this instance to a uninitialized state.
     */
    public final void fini() {
        synchronized (initialized) {
            qName = null;
            unmarshaller = null;
            context = null;
            initialized = Boolean.FALSE;
        }
    }

    /**
     * Attempts to replicate the provided instance using JAXB (de-)serialization.
     * 
     * @param original
     *            the instance to replicate
     * @return an independent copy of the bean
     * @throws JAXBException
     *             if (de-)serialization fails
     */
    public T replicate(final T original) throws JAXBException {
        if (!initialized) {
            init();
        }
        synchronized (initialized) {
            final JAXBElement<T> vvrElem = new JAXBElement<>(qName, targetClass, original);
            final JAXBSource vvrSource = new JAXBSource(context, vvrElem);

            return unmarshaller.unmarshal(vvrSource, targetClass).getValue();
        }
    }

}
