package com.oodrive.nuage.net;

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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import com.google.protobuf.MessageLite;
import com.oodrive.nuage.proto.net.MsgWrapper;
import com.oodrive.nuage.proto.Common.ProtocolVersion;

final class MsgServerGenericHandler extends SimpleChannelHandler {

    private final UUID msgServerId;
    private final MsgServerHandler msgServerHandler;
    private final MessageLite prototype;
    private final ChannelGroup channelGroup;
    private final AtomicBoolean serverStarted;

    MsgServerGenericHandler(final UUID msgServerId, final MsgServerHandler msgServerHandler,
            final MessageLite prototype, final ChannelGroup channelGroup, final AtomicBoolean serverStarted) {
        this.msgServerId = msgServerId;
        this.msgServerHandler = msgServerHandler;
        this.prototype = prototype;
        this.channelGroup = channelGroup;
        this.serverStarted = serverStarted;
    }

    @Override
    public final void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
        final MsgWrapper.MsgRequest request = (MsgWrapper.MsgRequest) e.getMessage();
        final long msgId = request.getMsgId();

        final MessageLite reply;
        try {
            // Deserialize the message data which represent a Protobuf message
            final MessageLite deserializedMsg = prototype.newBuilderForType().mergeFrom(request.getMsgData()).build();
            reply = msgServerHandler.handleMessage(deserializedMsg);

        }
        catch (final Throwable t) {
            // Return exception
            if (request.getSynchronous()) {
                final MsgWrapper.MsgReply.Builder replyBuilder = MsgWrapper.MsgReply.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(msgId)
                        .setStatus(false).setException(t.getClass().getName());
                ctx.getChannel().write(replyBuilder.build());
            }
            MsgServerEndpoint.LOGGER.error("Msg server [" + msgServerId + "], error while handling the message '"
                    + msgId + "'", t);
            return;
        }

        // Send ACK only if the request is synchronous
        if (request.getSynchronous()) {
            final MsgWrapper.MsgReply.Builder replyBuilder = MsgWrapper.MsgReply.newBuilder().setVersion(ProtocolVersion.VERSION_1).setMsgId(msgId)
                    .setStatus(true);
            // Optional reply
            if (reply != null) {
                replyBuilder.setRepData(reply.toByteString());
            }
            ctx.getChannel().write(replyBuilder.build());
        }
    }

    @Override
    public final void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
        // If the server is stopped then close all new connected channels in order to be able to release allocated
        // resources when the server stop.
        if (!serverStarted.get()) {
            ctx.getChannel().close();
            return;
        }
        channelGroup.add(ctx.getChannel());
        MsgServerEndpoint.LOGGER.info("Msg server [{}], channel connected to \'{}\'", msgServerId, ctx.getChannel()
                .getRemoteAddress());
    }

    @Override
    public final void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) throws Exception {
        MsgServerEndpoint.LOGGER.warn(e.getCause().toString());
    }
}
