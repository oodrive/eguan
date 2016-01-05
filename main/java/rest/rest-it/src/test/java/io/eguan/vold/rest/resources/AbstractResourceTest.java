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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import io.eguan.configuration.MetaConfiguration;
import io.eguan.rest.container.JettyConfigurationContext;
import io.eguan.rest.container.RestContextPathConfigKey;
import io.eguan.rest.container.ServerAddressConfigKey;
import io.eguan.rest.container.ServerPortConfigKey;
import io.eguan.rest.container.ServletServer;
import io.eguan.rest.jaxrs.JaxRsAppContext;
import io.eguan.vold.adm.RestLauncher;
import io.eguan.vold.rest.generated.model.ObjectFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;

import javax.ws.rs.core.UriBuilder;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.eclipse.jetty.xml.XmlConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.xml.sax.SAXException;

/**
 * Abstract superclass for fixture common to all resource tests.
 * 
 * This includes all JMX-client configuration and starting the {@link ServletServer}.
 * 
 * @author oodrive
 * @author pwehrle
 * @author ebredzinski
 * 
 */
public abstract class AbstractResourceTest extends AbstractJmxRemoteTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractResourceTest.class);

    /**
     * The JAXB-generated {@link ObjectFactory}.
     */
    protected static ObjectFactory objectFactory;

    protected static MetaConfiguration jettyConfig;

    private static RestLauncher testLauncher;

    private static final String DEFAULT_JETTY_CONFIG_RES = "/RestLauncher.properties";

    private static final String DEFAULT_JAXRS_APP_RES = "/vold-adm.xml";

    static String SERVER_BASE_URI = "http://localhost:8888/storage";

    /**
     * Sets up common static fixture.
     */
    @BeforeClass
    public static final void setUpClass() throws InitializationError {
        objectFactory = new ObjectFactory();

        try {

            configureJaxRsXml(AbstractResourceTest.class.getResourceAsStream(DEFAULT_JAXRS_APP_RES), getJmxTestHelper()
                    .getServerUrl(), new File(AbstractResourceTest.class.getResource(DEFAULT_JAXRS_APP_RES).toURI()));

            testLauncher = new RestLauncher();
            assertNotNull(testLauncher);
            assertFalse(testLauncher.isInitialized());
            assertFalse(testLauncher.isStarted());

            testLauncher.init();
            assertTrue(testLauncher.isInitialized());

            testLauncher.start();
            assertTrue(testLauncher.isStarted());

            final InputStream configInputStream = AbstractResourceTest.class
                    .getResourceAsStream(DEFAULT_JETTY_CONFIG_RES);
            jettyConfig = MetaConfiguration
                    .newConfiguration(configInputStream, JettyConfigurationContext.getInstance());

            final InetAddress host = ServerAddressConfigKey.getInstance().getTypedValue(jettyConfig);
            final Integer port = ServerPortConfigKey.getInstance().getTypedValue(jettyConfig);
            final String servletRoot = RestContextPathConfigKey.getInstance().getTypedValue(jettyConfig)
                    .replace("/*", "");

            SERVER_BASE_URI = UriBuilder
                    .fromUri(new URI("http://" + host.getHostAddress() + ":" + port.toString() + "/"))
                    .path(servletRoot).build().toString();

        }
        catch (final Exception e) {
            LOGGER.error("Setup threw exception", e);
            throw new InitializationError(e);
        }

    }

    /**
     * Tears down common static fixture.
     * 
     * @throws InitializationError
     */
    @AfterClass
    public static final void tearDownClass() throws InitializationError {
        assert testLauncher != null;
        try {
            testLauncher.stop();
            assert !testLauncher.isStarted();
        }
        catch (final Exception e) {
            throw new InitializationError(e);
        }
        finally {
            testLauncher.fini();
        }

    }

    /**
     * Injects the provided server URL as constructor parameter into an existing {@link XmlConfiguration} resource
     * describing a {@link JaxRsAppContext} which includes a {@link VvrsResourceJmxImpl}.
     * 
     * @param originalResource
     *            the original configuration
     * @param serverUrl
     *            the URL to inject
     * @return the altered configuration
     * @throws InitializationError
     *             if anything goes wrong
     */
    private static final void configureJaxRsXml(final InputStream originalResource, final String serverUrl,
            final File destination) throws InitializationError {
        try {
            // parses the XML document
            final DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            final Document configDoc = docBuilder.parse(originalResource);

            // selects and alters the node containing the server URL
            final String xPathExpr = "//*/New[@class='" + VvrsResourceJmxImpl.class.getCanonicalName() + "']/Arg";
            final XPath xpath = XPathFactory.newInstance().newXPath();
            final Node target = (Node) xpath.evaluate(xPathExpr, configDoc, XPathConstants.NODE);
            target.setTextContent(serverUrl);

            // re-serializes the XML document and returns the result
            final DOMImplementationLS domImpl = (DOMImplementationLS) DOMImplementationRegistry.newInstance()
                    .getDOMImplementation("LS");

            final LSOutput lsOut = domImpl.createLSOutput();
            lsOut.setEncoding(configDoc.getInputEncoding() == null ? configDoc.getInputEncoding() : "UTF-8");
            final FileOutputStream outStream = new FileOutputStream(destination);
            lsOut.setByteStream(outStream);

            domImpl.createLSSerializer().write(configDoc, lsOut);
            // TODO: make a nice little XSLT that keeps all the exceptions at bay!
        }
        catch (SAXException | ParserConfigurationException | IOException | XPathExpressionException
                | ClassNotFoundException | InstantiationException | IllegalAccessException | ClassCastException e) {
            throw new InitializationError(e);
        }
    }
}
