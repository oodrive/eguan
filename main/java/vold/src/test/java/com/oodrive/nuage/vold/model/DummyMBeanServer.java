package com.oodrive.nuage.vold.model;

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

import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.loading.ClassLoaderRepository;

import junit.framework.Assert;

import com.oodrive.nuage.dtx.DtxManager;
import com.oodrive.nuage.dtx.DtxManager.DtxLocalNode;
import com.oodrive.nuage.net.MsgClientStartpoint;
import com.oodrive.nuage.net.MsgServerEndpoint;
import com.oodrive.nuage.vold.VoldMXBean;

/**
 * This class mimic an {@link MBeanServer} to be used by tests.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * @author jmcaba
 * 
 */
public class DummyMBeanServer implements MBeanServer {

    /**
     * Location where MBeans are stored when they are registered.
     */
    private final Map<ObjectName, Object> registeredMbeans = new HashMap<ObjectName, Object>();

    /**
     * {@link DummyMBeanServer} instance.
     */
    private static final DummyMBeanServer mbeanServer1 = new DummyMBeanServer();

    /**
     * {@link DummyMBeanServer} instance.
     */
    private static final DummyMBeanServer mbeanServer2 = new DummyMBeanServer();

    /**
     * @return Get the {@link MBeanServer}.
     */
    public static DummyMBeanServer getMBeanServer1() {
        return mbeanServer1;
    }

    /**
     * @return Get the {@link MBeanServer}.
     */
    public static DummyMBeanServer getMBeanServer2() {
        return mbeanServer2;
    }

    /**
     * @return The MXBean corresponding to objectName or null
     */
    public Object getMXBean(final ObjectName objectName) {
        return registeredMbeans.get(objectName);
    }

    private static final long WAIT_MXBEAN_TIMEOUT = 20 * 1000L; // 20 s

    /**
     * Wait some time for a MXBean to be registered.
     * 
     * @return The MXBean corresponding to objectName or null
     */
    public Object waitMXBean(final ObjectName objectName) {
        final long end = System.currentTimeMillis() + WAIT_MXBEAN_TIMEOUT;
        Object result;
        do {
            result = registeredMbeans.get(objectName);
            try {
                Thread.sleep(WAIT_MXBEAN_TIMEOUT / 15);
            }
            catch (final InterruptedException e) {
                // Ignored
            }
        } while (result == null && end > System.currentTimeMillis());
        return result;
    }

    /**
     * Wait some time for a MXBean to be unregistered.
     * 
     * @return The MXBean corresponding to objectName or null
     */
    public boolean waitMXBeanUnregistered(final ObjectName objectName) {
        final long end = System.currentTimeMillis() + WAIT_MXBEAN_TIMEOUT;
        Object result;
        do {
            result = registeredMbeans.get(objectName);
            try {
                Thread.sleep(WAIT_MXBEAN_TIMEOUT / 15);
            }
            catch (final InterruptedException e) {
                // Ignored
            }
        } while (result != null && end > System.currentTimeMillis());
        return (result == null);
    }

    public int getNbMXBeans() {
        return registeredMbeans.size();
    }

    public void register(final VoldMXBean voldMxBean, final ObjectName name) {
        registeredMbeans.put(name, voldMxBean);
    }

    public void register(final MsgClientStartpoint msgClient, final ObjectName name) {
        registeredMbeans.put(name, msgClient);
    }

    public void register(final MsgServerEndpoint endpoint, final ObjectName name) {
        registeredMbeans.put(name, endpoint);
    }

    public void register(final VvrManager vvrManager, final ObjectName name) {
        registeredMbeans.put(name, vvrManager);
    }

    public void register(final DtxManager dtxManager, final ObjectName name) {
        registeredMbeans.put(name, dtxManager);
    }

    public void register(final DtxLocalNode dtxLocalNode, final ObjectName name) {
        registeredMbeans.put(name, dtxLocalNode);
    }

    @Override
    public ObjectInstance createMBean(final String className, final ObjectName name) throws ReflectionException,
            InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException {

        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ObjectInstance createMBean(final String className, final ObjectName name, final ObjectName loaderName)
            throws ReflectionException, InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException,
            NotCompliantMBeanException, InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ObjectInstance createMBean(final String className, final ObjectName name, final Object[] params,
            final String[] signature) throws ReflectionException, InstanceAlreadyExistsException,
            MBeanRegistrationException, MBeanException, NotCompliantMBeanException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ObjectInstance createMBean(final String className, final ObjectName name, final ObjectName loaderName,
            final Object[] params, final String[] signature) throws ReflectionException,
            InstanceAlreadyExistsException, MBeanRegistrationException, MBeanException, NotCompliantMBeanException,
            InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    /**
     * This method add the MBean provided with it {@link ObjectName} into the local map.
     */
    @Override
    public ObjectInstance registerMBean(final Object object, final ObjectName name)
            throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {

        if (registeredMbeans.containsKey(name)) {
            throw new InstanceAlreadyExistsException(name.toString());
        }

        registeredMbeans.put(name, object);
        return new ObjectInstance(name, name.getClass().getName());
    }

    /**
     * Simply removes the {@link ObjectName} provided.
     * 
     * @exception InstanceNotFoundException
     *                if {@link ObjectName} doesn't belong to the local map, the {@link InstanceNotFoundException} is
     *                raised.
     */
    @Override
    public void unregisterMBean(final ObjectName name) throws InstanceNotFoundException, MBeanRegistrationException {

        if (!registeredMbeans.containsKey(name)) {
            throw new InstanceNotFoundException();
        }

        registeredMbeans.remove(name);
    }

    @Override
    public ObjectInstance getObjectInstance(final ObjectName name) throws InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Set<ObjectInstance> queryMBeans(final ObjectName name, final QueryExp query) {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Set<ObjectName> queryNames(final ObjectName name, final QueryExp query) {
        throw new AssertionError("Not implemented", null);
    }

    /**
     * Get a set of mxbeans which belong to a VVR, typed by the provided parameter. The different type are specified by
     * {@link Constants}.
     * 
     * @param type
     *            The type of the mxbeans to retrieve
     * @return The corresponding set of mxbeans
     */
    public Set<ObjectName> getByType(final String vvrUuid, final String type) {

        Assert.assertNotNull(vvrUuid);
        Assert.assertNotNull(type);

        final Set<ObjectName> result = new HashSet<ObjectName>();

        for (final ObjectName on : registeredMbeans.keySet()) {
            final Map<String, String> propertyList = on.getKeyPropertyList();
            final String currentType = propertyList.get("type");
            final String currentVvr = propertyList.get("vvr");

            if ((currentVvr != null) && (currentType != null)) {
                if (currentVvr.equals(vvrUuid) && (currentType.equals(type))) {
                    result.add(on);
                }
            }
        }
        return result;
    }

    @Override
    public boolean isRegistered(final ObjectName name) {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Integer getMBeanCount() {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Object getAttribute(final ObjectName name, final String attribute) throws MBeanException,
            AttributeNotFoundException, InstanceNotFoundException, ReflectionException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public AttributeList getAttributes(final ObjectName name, final String[] attributes)
            throws InstanceNotFoundException, ReflectionException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public void setAttribute(final ObjectName name, final Attribute attribute) throws InstanceNotFoundException,
            AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public AttributeList setAttributes(final ObjectName name, final AttributeList attributes)
            throws InstanceNotFoundException, ReflectionException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Object invoke(final ObjectName name, final String operationName, final Object[] params,
            final String[] signature) throws InstanceNotFoundException, MBeanException, ReflectionException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public String getDefaultDomain() {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public String[] getDomains() {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public void addNotificationListener(final ObjectName name, final NotificationListener listener,
            final NotificationFilter filter, final Object handback) throws InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public void addNotificationListener(final ObjectName name, final ObjectName listener,
            final NotificationFilter filter, final Object handback) throws InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public void removeNotificationListener(final ObjectName name, final ObjectName listener)
            throws InstanceNotFoundException, ListenerNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public void removeNotificationListener(final ObjectName name, final ObjectName listener,
            final NotificationFilter filter, final Object handback) throws InstanceNotFoundException,
            ListenerNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public void removeNotificationListener(final ObjectName name, final NotificationListener listener)
            throws InstanceNotFoundException, ListenerNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public void removeNotificationListener(final ObjectName name, final NotificationListener listener,
            final NotificationFilter filter, final Object handback) throws InstanceNotFoundException,
            ListenerNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public MBeanInfo getMBeanInfo(final ObjectName name) throws InstanceNotFoundException, IntrospectionException,
            ReflectionException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public boolean isInstanceOf(final ObjectName name, final String className) throws InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Object instantiate(final String className) throws ReflectionException, MBeanException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Object instantiate(final String className, final ObjectName loaderName) throws ReflectionException,
            MBeanException, InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Object instantiate(final String className, final Object[] params, final String[] signature)
            throws ReflectionException, MBeanException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public Object instantiate(final String className, final ObjectName loaderName, final Object[] params,
            final String[] signature) throws ReflectionException, MBeanException, InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ObjectInputStream deserialize(final ObjectName name, final byte[] data) throws InstanceNotFoundException,
            OperationsException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ObjectInputStream deserialize(final String className, final byte[] data) throws OperationsException,
            ReflectionException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ObjectInputStream deserialize(final String className, final ObjectName loaderName, final byte[] data)
            throws InstanceNotFoundException, OperationsException, ReflectionException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ClassLoader getClassLoaderFor(final ObjectName mbeanName) throws InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ClassLoader getClassLoader(final ObjectName loaderName) throws InstanceNotFoundException {
        throw new AssertionError("Not implemented", null);
    }

    @Override
    public ClassLoaderRepository getClassLoaderRepository() {
        throw new AssertionError("Not implemented", null);
    }

}
