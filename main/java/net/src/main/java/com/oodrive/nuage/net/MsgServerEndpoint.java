package com.oodrive.nuage.net;

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

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.MessageLite;
import com.oodrive.nuage.proto.net.MsgWrapper;

/**
 * Message server endpoint which accepts connections from remote peers.
 * 
 * @author oodrive
 * @author ebredzinski
 * @author llambert
 * 
 */
public final class MsgServerEndpoint implements MsgServerMXBean {
    static final Logger LOGGER = LoggerFactory.getLogger(MsgServerEndpoint.class.getName());

    /** Keep thread factories names */
    static {
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
    }

    /*
     * Constants used to configure the execution handler.
     */
    private static final int NB_THREADS = 10;
    private static final int TIMEOUT_THREAD = 100; // ms
    private static final int MEMORY_CHANNEL = 0; // disabled
    private static final int MEMORY_GLOBAL = 0; // disabled

    /**
     * Factory used to create a pipeline for each channel.
     * 
     * 
     */
    private static final class MsgServerPipelineFactory implements ChannelPipelineFactory {

        private final UUID msgServerId;
        private final ExecutionHandler executionHandler;
        private final MsgServerHandler msgServerHandler;
        private final MessageLite prototype;
        private final ChannelGroup channelGroup;
        private final AtomicBoolean serverStarted;

        MsgServerPipelineFactory(final UUID msgServerId, final ExecutionHandler executionHandler,
                final MsgServerHandler msgServerHandler, final MessageLite prototype, final ChannelGroup channelGroup,
                final AtomicBoolean serverStarted) {
            this.msgServerId = msgServerId;
            this.executionHandler = executionHandler;
            this.msgServerHandler = msgServerHandler;
            this.prototype = prototype;
            this.channelGroup = channelGroup;
            this.serverStarted = serverStarted;
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {

            final ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
            pipeline.addLast("protobufDecoder", new ProtobufDecoder(MsgWrapper.MsgRequest.getDefaultInstance()));
            pipeline.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
            pipeline.addLast("protobufEncoder", new ProtobufEncoder());
            pipeline.addLast("executor handler", executionHandler);
            pipeline.addLast("application logic", new MsgServerGenericHandler(msgServerId, msgServerHandler, prototype,
                    channelGroup, serverStarted));
            return pipeline;
        }
    }

    private final AtomicBoolean serverStarted = new AtomicBoolean(false);

    /** The object which actually do the business logic of the application. */
    private final MsgServerHandler msgServerHandler;
    /** Prototype of Protobuf messages. */
    private final MessageLite prototype;

    /** Bind socket address. */
    private final InetSocketAddress bindSocketAddress;
    /** Unique id of the message server. */
    private final UUID msgServerId;

    /** A set which contains all remote channels. */
    private final ChannelGroup channelGroup = new DefaultChannelGroup("all channels");
    /** The Netty helper used to create the server channel. */
    private ExecutorService bossWorker;
    private ExecutorService slaveWorker;
    private ServerBootstrap bootstrap;
    /**
     * The Netty's workers are relieved through the use of an execution handler which executes the
     * {@link MsgServerHandler}.
     */
    private ExecutionHandler executionHandler;

    /** Thread factory to define thread name */
    private final ThreadFactory nettyThreadFactory;

    /**
     * Constructor of a message server endpoint.
     * 
     * @param self
     *            identity and bind address of this node
     * @param msgServerHandler
     *            The application logic to execute.
     * @param prototype
     *            The Protobuf prototype used to deserialize the received messages
     */
    public MsgServerEndpoint(@Nonnull final MsgNode self, @Nonnull final MsgServerHandler msgServerHandler,
            @Nonnull final MessageLite prototype) {
        this.msgServerHandler = Objects.requireNonNull(msgServerHandler);
        this.prototype = Objects.requireNonNull(prototype);
        this.bindSocketAddress = self.getAddress();
        this.msgServerId = self.getNodeId();
        this.nettyThreadFactory = new NetThreadFactory("MsgSrv[" + bindSocketAddress + "]-");
    }

    /**
     * Start the server, after the call the server is ready to accept connections from remote peers.
     */
    public final synchronized void start() {
        if (serverStarted.getAndSet(true)) {
            throw new IllegalStateException("Msg server already started");
        }

        try {
            bossWorker = Executors.newCachedThreadPool(new NetThreadFactory("MsgSrv[" + bindSocketAddress + "]-b-"));
            slaveWorker = Executors.newCachedThreadPool(new NetThreadFactory("MsgSrv[" + bindSocketAddress + "]-w-"));
            final ChannelFactory factory = new NioServerSocketChannelFactory(bossWorker, slaveWorker, Runtime
                    .getRuntime().availableProcessors() * 2);

            bootstrap = new ServerBootstrap(factory);
            executionHandler = new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(NB_THREADS,
                    MEMORY_CHANNEL, MEMORY_GLOBAL, TIMEOUT_THREAD, TimeUnit.MILLISECONDS, nettyThreadFactory));

            bootstrap.setPipelineFactory(new MsgServerPipelineFactory(msgServerId, executionHandler, msgServerHandler,
                    prototype, channelGroup, serverStarted));
            bootstrap.setOption("child.tcpNoDelay", Boolean.TRUE);
            bootstrap.setOption("child.keepAlive", Boolean.TRUE);
            bootstrap.setOption("child.reuseAddress", Boolean.TRUE);

            final Channel serverChannel = bootstrap.bind(bindSocketAddress);
            channelGroup.add(serverChannel);

            LOGGER.info(
                    "Msg server [{}] started at {}:{}",
                    new Object[] { msgServerId, bindSocketAddress.getHostString(),
                            Integer.valueOf(bindSocketAddress.getPort()) });
        }
        catch (final Throwable t) {
            LOGGER.error("Error while starting the message server [{}] at {}:{}, {}", new Object[] { msgServerId,
                    bindSocketAddress.getHostString(), Integer.valueOf(bindSocketAddress.getPort()), t.toString() });
            serverStarted.set(false);
        }
    }

    /**
     * Stop the server, every allocated resources are released, any connections from remote peers are not accepted.
     */
    public final synchronized void stop() {
        if (!serverStarted.getAndSet(false)) {
            return;
        }

        try {
            channelGroup.close().awaitUninterruptibly();
            channelGroup.clear();

            bootstrap.releaseExternalResources();
            bootstrap = null;
            executionHandler.releaseExternalResources();
            executionHandler = null;

            LOGGER.info("Msg server [{}] stopped...", msgServerId);
        }
        catch (final Throwable t) {
            LOGGER.error("Error while stopping the message server [{}]", t, msgServerId);
        }
    }

    /**
     * Gets server {@link UUID}.
     * 
     * @return the string of the {@link UUID}.
     */
    @Override
    public final String getUuid() {
        return msgServerId.toString();
    }

    /**
     * Gets the server IP address.
     * 
     * @return the read-only IP address.
     */
    @Override
    public final String getIpAddress() {
        return bindSocketAddress.getAddress().getHostAddress();
    }

    /**
     * Gets the server port.
     * 
     * @return the read-only port.
     */
    @Override
    public final int getPort() {
        return bindSocketAddress.getPort();
    }

    /**
     * Tells if the server is started.
     * 
     * @return <code>true</code> if started.
     */
    @Override
    public final boolean isStarted() {
        return serverStarted.get();
    }

    /**
     * Restart the server.
     * 
     */
    @Override
    public final void restart() {
        stop();
        start();
    }

    /**
     * Register the end point MXBean.
     * 
     * @throws InstanceAlreadyExistsException
     * @throws MBeanRegistrationException
     * @throws NotCompliantMBeanException
     * @throws MalformedObjectNameException
     * 
     * @return {@link ObjectName} of the MXBean
     * 
     */
    public final ObjectName registerMXBean(final MBeanServer mbeanServer) throws InstanceAlreadyExistsException,
            MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException {
        final ObjectName serverObjName = new ObjectName(this.getClass().getPackage().getName() + ":type=Server");
        mbeanServer.registerMBean(this, serverObjName);
        return serverObjName;
    }
}
