package com.liquidlabs.transport.netty.handshake.impl;

import com.liquidlabs.transport.netty.handshake.common.HandshakeEvent;
import org.jboss.netty.channel.*;

import java.util.concurrent.atomic.AtomicInteger;

public class ServerHandler extends SimpleChannelUpstreamHandler {

    // internal vars ----------------------------------------------------------

    private final AtomicInteger counter;
    private final ServerListener listener;
    private String remoteId;
    private Channel channel;

    // constructors -----------------------------------------------------------

    public ServerHandler(ServerListener listener) {
        this.listener = listener;
        this.counter = new AtomicInteger();
    }

    // SimpleChannelUpstreamHandler -------------------------------------------

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
            throws Exception {
        if (e instanceof HandshakeEvent) {
            if (((HandshakeEvent) e).isSuccessful()) {
                out("+++ SERVER-HANDLER :: Handshake successful, connection " +
                        "to " + ((HandshakeEvent) e).getRemoteId() + " is up.");
                this.remoteId = ((HandshakeEvent) e).getRemoteId();
                this.channel = ctx.getChannel();
                // Notify the listener that a new connection is now READY
                this.listener.connectionOpen(this);
            } else {
                out("+++ SERVER-HANDLER :: Handshake failed.");
            }
            return;
        }

        super.handleUpstream(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        this.counter.incrementAndGet();
        this.listener.messageReceived(this, e.getMessage().toString());
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        super.channelClosed(ctx, e);
        out("+++ SERVER-HANDLER :: Channel closed, received " +
                this.counter.get() + " messages: " + e.getChannel());
    }

    // public methods ---------------------------------------------------------

    public void sendMessage(String message) {
        if (!message.endsWith("\n")) {
            this.channel.write(message + '\n');
        } else {
            this.channel.write(message);
        }
    }

    public String getRemoteId() {
        return remoteId;
    }

    // private static helpers -------------------------------------------------

    private static void out(String s) {
        System.err.println(s);
    }
}