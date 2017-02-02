package io.eguan.rest.jaxrs;

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

import java.io.InputStream;
import java.util.Set;

import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.ext.Provider;

import org.eclipse.jetty.xml.XmlConfiguration;

/**
 * {@link Application} implementation serving as container for {@link Path Resource}s and {@link Provider}s
 * 
 * The context is initialized by an instance of {@link JaxRsAppContext} configured via {@link XmlConfiguration jetty xml
 * configuration}. This implementation only provides {@link #getSingletons() singletons}.
 * 
 * @see javax.ws.rs.core.Application;
 * 
 * @author oodrive
 * @author pwehrle
 * 
 */
public class JaxRsConfiguredApplication extends Application {

    /**
     * {@link JaxRsAppContext} instance holding the object instances to pass to the web app.
     */
    private final JaxRsAppContext appContext;

    /**
     * Constructs an initialized {@link Application} from the commands found in the <a href='http
     * ://www.eclipse.org/jetty/documentation/current/jetty-xml-syntax.html'>Jetty XML</a>-formatted source.
     * 
     * @param xmlConfigurationSource
     *            Jetty XML format input configuring a {@link JaxRsAppContext} instance
     * @throws IllegalArgumentException
     *             if the input cannot be read or parsed
     */
    public JaxRsConfiguredApplication(final InputStream xmlConfigurationSource) throws IllegalArgumentException {

        XmlConfiguration xmlConf;
        try {
            xmlConf = new XmlConfiguration(xmlConfigurationSource);
            appContext = (JaxRsAppContext) xmlConf.configure(new JaxRsAppContext());
        }
        catch (final Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Set<Object> getSingletons() {
        return appContext.getSingletonInstances();
    }

}
