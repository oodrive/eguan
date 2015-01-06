package com.oodrive.nuage.rest.jaxrs;

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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container class for JAX-RS annotated classes and singleton instances.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public class JaxRsAppContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(JaxRsAppContext.class);

    private final Set<Object> resources = new HashSet<Object>();

    private final Set<Object> providers = new HashSet<Object>();

    public JaxRsAppContext() {
        super();
    }

    /**
     * Adds a resource instance.
     * 
     * Resource classes must have at least one (inherited) {@link Path} annotation, either at class or method level.
     * 
     * @param resource
     *            a functional (singleton) instance of a resource class
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     * @throws IllegalArgumentException
     *             if no (even inherited) {@link Path} annotation can be found on the resource class or methods
     * @see Path
     */
    public final void addResource(@Nonnull final Object resource) throws NullPointerException, IllegalArgumentException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("adding resource instance {}", resource);
        }
        final Class<? extends Object> resClass = Objects.requireNonNull(resource).getClass();

        if (!hasPathAnnotation(resClass)) {
            throw new IllegalArgumentException("Missing @Path annotation on resource class "
                    + resClass.getCanonicalName());
        }

        this.resources.add(resource);
    }

    /**
     * Adds all elements of the array as resources.
     * 
     * @param resources
     *            a non-<code>null</code> array of (singleton) instances
     * @throws NullPointerException
     *             if any of the resources is <code>null</code>
     * @throws IllegalArgumentException
     *             if no {@link Path} annotation can be found on any of the resources' classes or methods
     * @see #addResource(Object)
     */
    public final void addAllResources(@Nonnull final Object[] resources) throws NullPointerException,
            IllegalArgumentException {
        for (final Object currObject : resources) {
            addResource(currObject);
        }
    }

    /**
     * Adds a {@link Provider} instance.
     * 
     * Provider classes (or any of their superclasses or -interfaces) must be annotated with {@link Provider}.
     * 
     * @param provider
     *            an non-<code>null</code> {@link Object} instance
     * @throws NullPointerException
     *             if the argument is <code>null</code
     * @throws IllegalArgumentException
     *             if no (inherited) {@link Provider} annotation can be found on the provider's class
     * @see Provider
     */
    public final void addProvider(@Nonnull final Object provider) throws NullPointerException, IllegalArgumentException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("adding provider instance {}", provider);
        }
        final Class<?> provClass = Objects.requireNonNull(provider.getClass());
        if (!hasProviderAnnotation(provClass)) {
            throw new IllegalArgumentException("Missing @Provider annotation on class " + provClass.getCanonicalName());
        }
        this.providers.add(provider);
    }

    /**
     * Adds all elements of the array as providers.
     * 
     * @param provider
     *            a non-<code>null</code> array of (singleton) instances
     * @throws NullPointerException
     *             if any of the providers is <code>null</code>
     * @throws IllegalArgumentException
     *             if no {@link Provider} annotation can be found on any of the providers' classes
     * @see #addProvider(Object)
     */
    public final void addAllProviders(@Nonnull final Object[] providers) throws NullPointerException,
            IllegalArgumentException {
        for (final Object currObject : providers) {
            addProvider(currObject);
        }
    }

    /**
     * Gets all singleton {@link Object} instances to be inserted either as resources or providers into the
     * {@link Application JAX-RS application} context.
     * 
     * @return a (possibly empty) {@link Set} of {@link Object}s
     */
    public final Set<Object> getSingletonInstances() {
        final HashSet<Object> result = new HashSet<Object>(resources);
        result.addAll(providers);
        return Collections.unmodifiableSet(result);
    }

    /**
     * Recursively determines if the provided {@link Class} or any of its superclasses, methods or interfaces has a
     * {@link Path} annotation.
     * 
     * @param clazz
     *            the {@link Class} to check
     * @return <code>true</code> if a {@link Path} annotation was found, <code>false</code> otherwise
     */
    private static boolean hasPathAnnotation(final Class<?> clazz) {
        if (clazz == null) {
            return false;
        }

        boolean hasPathAnnotation = (clazz.getAnnotation(Path.class) != null);

        if (!hasPathAnnotation) {
            for (final Method currMethod : clazz.getMethods()) {
                hasPathAnnotation |= (currMethod.getAnnotation(Path.class) != null);
            }
        }

        if (!hasPathAnnotation) {
            hasPathAnnotation |= hasPathAnnotation(clazz.getSuperclass());
        }

        if (!hasPathAnnotation) {
            for (final Class<?> currInterface : clazz.getInterfaces()) {
                hasPathAnnotation |= hasPathAnnotation(currInterface);
            }
        }

        return hasPathAnnotation;
    }

    /**
     * Recursively determines if the provided {@link Class} or any of its superclasses or interfaces has a
     * {@link Provider} annotation.
     * 
     * @param clazz
     *            the {@link Class} to check
     * @return <code>true</code> if a {@link Provider} annotation was found, <code>false</code> otherwise
     */
    private static boolean hasProviderAnnotation(final Class<?> clazz) {
        if (clazz == null) {
            return false;
        }

        boolean hasProviderAnnotation = (clazz.getAnnotation(Provider.class) != null);

        if (!hasProviderAnnotation) {
            hasProviderAnnotation |= hasProviderAnnotation(clazz.getSuperclass());
        }

        if (!hasProviderAnnotation) {
            for (final Class<?> currInterface : clazz.getInterfaces()) {
                hasProviderAnnotation |= hasProviderAnnotation(currInterface);
            }
        }

        return hasProviderAnnotation;
    }

}
