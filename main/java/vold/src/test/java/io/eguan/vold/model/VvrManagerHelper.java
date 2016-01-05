package io.eguan.vold.model;

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

import io.eguan.dtx.DtxManager;
import io.eguan.dtx.DtxResourceManager;
import io.eguan.dtx.DtxResourceManagerContext;
import io.eguan.dtx.DtxTaskInfo;
import io.eguan.proto.vvr.VvrRemote.RemoteOperation;
import io.eguan.vold.model.VvrManager;
import io.eguan.vvr.remote.VvrDtxRmContext;
import io.eguan.vvr.remote.VvrRemoteUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import javax.management.ObjectName;
import javax.transaction.xa.XAException;

import org.junit.Assert;

import com.google.protobuf.InvalidProtocolBufferException;

public class VvrManagerHelper {

    private final static String ISCSI_ADDRESS = InetAddress.getLoopbackAddress().getHostAddress();
    private final static Integer ISCSI_PORT = Integer.valueOf(3260);

    private final static String NBD_ADDRESS = InetAddress.getLoopbackAddress().getHostAddress();
    private final static Integer NBD_PORT = Integer.valueOf(10809);

    private VoldTestHelper voldTestHelper;

    private VvrManager vvrManager;

    private DtxManager dtxManager;

    private ObjectName dtxManagerObjectName;

    private ObjectName dtxLocalNodeObjectName;

    public final static void initDtxManagement(final VvrManager vvrManager, final DtxManager dtxManager)
            throws IOException {
        vvrManager.init(dtxManager);
        final UUID owner = UUID.fromString(vvrManager.getOwnerUuid());
        dtxManager.registerResourceManager(new DtxResourceManager() {

            @Override
            public final UUID getId() {
                return owner;
            }

            @Override
            public final DtxResourceManagerContext start(final byte[] payload) throws XAException, NullPointerException {
                try {
                    return VvrRemoteUtils.createDtxContext(owner, payload);
                }
                catch (final InvalidProtocolBufferException e) {
                    throw new XAException(XAException.XAER_INVAL);
                }
            }

            @Override
            public final Boolean prepare(final DtxResourceManagerContext context) throws XAException {
                final VvrDtxRmContext vvrDtxRmContext = (VvrDtxRmContext) context;
                return vvrManager.prepare(vvrDtxRmContext);
            }

            @Override
            public final void commit(final DtxResourceManagerContext context) throws XAException {
                final VvrDtxRmContext vvrDtxRmContext = (VvrDtxRmContext) context;
                vvrManager.commit(vvrDtxRmContext);
            }

            @Override
            public final void rollback(final DtxResourceManagerContext context) throws XAException {
                final VvrDtxRmContext vvrDtxRmContext = (VvrDtxRmContext) context;
                vvrManager.rollback(vvrDtxRmContext);
            }

            @Override
            public final void processPostSync() {
                vvrManager.processPostSync();
            }

            @Override
            public final DtxTaskInfo createTaskInfo(final byte[] payload) {
                final RemoteOperation operation;
                try {
                    operation = RemoteOperation.parseFrom(payload);
                }
                catch (final InvalidProtocolBufferException e) {
                    return null;
                }
                return vvrManager.createTaskInfo(operation);
            }
        });
    }

    public void init(final boolean vvrStarted, final DummyMBeanServer dummyMBeanServer, final int number)
            throws Exception {
        this.voldTestHelper = new VoldTestHelper(Boolean.valueOf(vvrStarted));

        voldTestHelper.createTemporary(null, null, null, null, ISCSI_ADDRESS,
                Integer.valueOf(ISCSI_PORT.intValue() + 2 * number), NBD_ADDRESS,
                Integer.valueOf(NBD_PORT.intValue() + 2 * number));

        this.vvrManager = VvrManagerTestUtils.createVvrManager(voldTestHelper, dummyMBeanServer);

        this.dtxManager = VvrManagerTestUtils.createDtxManagerStandAlone(dummyMBeanServer, voldTestHelper);

        this.dtxManagerObjectName = VvrManagerTestUtils.registerDtxManagerMXBean(dummyMBeanServer, voldTestHelper,
                dtxManager);
        this.dtxLocalNodeObjectName = VvrManagerTestUtils.registerDtxLocalNodeMXBean(dummyMBeanServer, voldTestHelper,
                dtxManager.new DtxLocalNode());
        Assert.assertNotNull(dtxManager);
        initDtxManagement(vvrManager, dtxManager);

        Assert.assertTrue(VvrManagerTestUtils.waitMXBeanNumber(dummyMBeanServer, VoldTestHelper.MXBEANS_NUMBER_INIT));
    }

    public void fini(final DummyMBeanServer dummyMBeanServer) throws Exception {
        vvrManager.fini();

        if (dtxManagerObjectName != null) {
            dummyMBeanServer.unregisterMBean(dtxManagerObjectName);
            dtxManagerObjectName = null;
        }
        if (dtxLocalNodeObjectName != null) {
            dummyMBeanServer.unregisterMBean(dtxLocalNodeObjectName);
            dtxLocalNodeObjectName = null;
        }

        if (dtxManager != null) {
            dtxManager.stop();
            dtxManager.fini();
            dtxManager = null;
        }

        // All MX Beans must be unregistered
        Assert.assertEquals(0, dummyMBeanServer.getNbMXBeans());

        voldTestHelper.destroy();
        vvrManager = null;
    }

    public final VoldTestHelper getVoldTestHelper() {
        return voldTestHelper;
    }

    public final VvrManager getVvrManager() {
        return vvrManager;
    }

    public final DtxManager getDtxManager() {
        return dtxManager;
    }

    public final ObjectName getDtxManagerObjectName() {
        return dtxManagerObjectName;
    }

    public final ObjectName getDtxLocalNodeObjectName() {
        return dtxLocalNodeObjectName;
    }

    public final void setVoldTestHelper(final VoldTestHelper voldTestHelper) {
        this.voldTestHelper = voldTestHelper;
    }

    public final void setVvrManager(final VvrManager vvrManager) {
        this.vvrManager = vvrManager;
    }

    public final void setDtxManager(final DtxManager dtxManager) {
        this.dtxManager = dtxManager;
    }

    public final void setDtxManagerObjectName(final ObjectName dtxManagerObjectName) {
        this.dtxManagerObjectName = dtxManagerObjectName;
    }

    public final void setDtxLocalNodeObjectName(final ObjectName dtxLocalNodeObjectName) {
        this.dtxLocalNodeObjectName = dtxLocalNodeObjectName;
    }
}
