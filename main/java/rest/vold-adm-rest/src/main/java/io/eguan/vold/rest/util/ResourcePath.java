package io.eguan.vold.rest.util;

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

import io.eguan.vold.rest.errors.CustomResourceException;
import io.eguan.vold.rest.errors.ServerErrorFactory;
import io.eguan.vold.rest.resources.AbstractResource;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

/**
 * Utility methods for resource path discovery and manipulation.
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public final class ResourcePath {

    private ResourcePath() {
        throw new AssertionError("Not instantiable");
    }

    /**
     * Injects {@link UriInfo} into properly typed and {@link Context}-annotated fields of the target resource.
     * 
     * @param uriInfo
     *            the {@link UriInfo} to inject
     * @param targetResource
     *            the target {@link AbstractResource} instance
     * @throws CustomResourceException
     *             if the annotated field is enforcing Java language access control and the annotated field is either
     *             inaccessible or final
     * @throws NullPointerException
     *             if the specified target resource is <code>null</code> and the annotated field is an instance field
     * @throws ExceptionInInitializerError
     *             if the initialization provoked by setting the value fails
     */
    public static final void injectUriInfoContext(final UriInfo uriInfo, final AbstractResource targetResource)
            throws CustomResourceException, NullPointerException, ExceptionInInitializerError {

        for (final Field currField : targetResource.getClass().getFields()) {
            if (!currField.isAnnotationPresent(Context.class) || !UriInfo.class.isAssignableFrom(currField.getType())
                    || Modifier.isFinal(currField.getModifiers())) {
                continue;
            }
            try {
                currField.set(targetResource, uriInfo);
            }
            catch (final IllegalAccessException e) {
                throw ServerErrorFactory.newInternalErrorException("Internal error",
                        "Context UriInfo injection failed on " + targetResource, e);
            }
        }
    }

    /**
     * Extracts the closest instance in the hierarchy of matched resources from the given {@link UriInfo} that can be
     * cast to the specified ancestor {@link Class}.
     * 
     * @param uriInfo
     *            the {@link UriInfo} from which to extract the ancestor
     * @param ancestorClass
     *            the {@link Class} or any of its subclasses or implementations to search
     * @return a properly cast instance of the requested {@link Class}, <code>null</code> if no match was found
     * @throws NullPointerException
     *             if either parameter in <code>null</code>
     */
    public static final <T extends AbstractResource> T extractAncestorFromMatchedResources(final UriInfo uriInfo,
            final Class<T> ancestorClass) throws NullPointerException {
        // iterates over matched resources and returns first match
        for (final Object currResource : uriInfo.getMatchedResources()) {
            if (ancestorClass.isAssignableFrom(currResource.getClass())) {
                return ancestorClass.cast(currResource);
            }
        }
        return null;
    }

    /**
     * Extracts the part of the URI path referencing a not yet matched sub-resource.
     * 
     * This method must only be called by sub-resource locators (see <a href='http
     * ://jsr311.java.net/nonav/releases/1.1/spec/spec3.html#x3-310003.4.1'>JSR-311 r1.1 chapter 3.4.1</a>), as the
     * implementation relies on certain assertions being met only in this context.
     * 
     * @param uriInfo
     *            the {@link UriInfo} of the request causing the call of the sub-resource locator
     * @return the part of the resource path matching the not yet matched sub-resource
     * @throws NullPointerException
     *             if the argument is <code>null</code>
     */
    public static final String extractPathForNewSubResource(final UriInfo uriInfo) throws NullPointerException {
        final List<String> matchedUris = uriInfo.getMatchedURIs();
        final List<Object> matchedResources = uriInfo.getMatchedResources();

        // checks for preconditions characteristic to sub-resource locators
        assert (matchedUris.size() > 1);
        assert (matchedUris.size() == matchedResources.size() + 1);

        return matchedUris.get(0).replace(matchedUris.get(1), "");
    }

    /**
     * Extracts the path for a sub-resource of a given type from the class containing the sub-resource locator.
     * 
     * This method searches the methods of the target class for a sub-resource locator annotated with {@link Path} and
     * returns the defined path value if the return type matches the requested one. It only makes sense in this
     * particular context, so don't try anything else!
     * 
     * @param targetClass
     *            the {@link Class} which defines the {@link Path} annotated method returning the requested resource
     *            type
     * @param expectedResourceType
     *            the {@link Class} defining the resource type (may be an interface or superclass)
     * @return the relative resource path if a sub-resource locator could be found, <code>null</code> otherwise
     */
    public static String extractPathForSubResourceLocator(final Class<?> targetClass,
            final Class<?> expectedResourceType) {
        if (targetClass == null) {
            return null;
        }

        // iterates over class methods and returns the first match on return type and @Path annotation
        for (final Method currMethod : targetClass.getMethods()) {
            if (expectedResourceType.isAssignableFrom(currMethod.getReturnType())) {
                if (!currMethod.isAnnotationPresent(Path.class)) {
                    continue;
                }
                return currMethod.getAnnotation(Path.class).value();
            }
        }

        // recursively calls all implemented interfaces and returns the first match
        String result = null;
        for (final Class<?> currInterface : targetClass.getInterfaces()) {
            result = extractPathForSubResourceLocator(currInterface, expectedResourceType);
            if (result != null) {
                return result;
            }
        }

        // finally, recursively calls the super class
        result = extractPathForSubResourceLocator(targetClass.getSuperclass(), expectedResourceType);

        return result;
    }

}
