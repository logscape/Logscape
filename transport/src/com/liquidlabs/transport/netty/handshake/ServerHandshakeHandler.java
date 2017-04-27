package com.liquidlabs.transport.netty.handshake;

import com.liquidlabs.transport.netty.handshake.common.Challenge;
import com.liquidlabs.transport.netty.handshake.common.HandshakeEvent;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

public class ServerHandshakeHandler extends SimpleChannelHandler {
    private static final Logger logger = Logger.getLogger(ServerHandshakeHandler.class);

    // internal vars ----------------------------------------------------------
    CyperHandler cypherHandler = new CyperHandler();
    private final long timeoutInMillis;
    private final String localId;
    private final ChannelGroup group;
    private final Queue<MessageEvent> messages = new ArrayDeque<MessageEvent>();
    private final HostIpFilter hostsFilter;
    HandshakeStatus hs = new HandshakeStatus();
    boolean isOutEnabled = Boolean.getBoolean("server.handshake.verbose");;




    // constructors -----------------------------------------------------------

    public ServerHandshakeHandler(String localId, ChannelGroup group,
                                  long timeoutInMillis) {
        this.localId = localId;
        this.group = group;
        this.timeoutInMillis = timeoutInMillis;
        this.hostsFilter = new HostIpFilter();

    }

    // SimpleChannelHandler ---------------------------------------------------

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {

        // handle a handshake - or if we have authd the client then let it through
        if (isHandshake(e) || hs.isPassed()  ) {
            // is it good?
            handleHandshake(ctx, e);

        } else if (hs.isPassed() ) {
            super.messageReceived(ctx, e);

        }   else {
            // not-authd
            return;
        }

    }

    private void handleHandshake(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        String clientId = ctx.getChannel().getRemoteAddress().toString();



        if (hs.handshakeFailed.get()) {
            // Bail out fast if handshake already failed
            return;
        }

        if (hs.handshakeComplete.get()) {
            // If handshake succeeded but message still came through this
            // handler, then immediately send it upwards.
            out("1-Passing e:" + e);
            super.messageReceived(ctx, e);
            // should have been removed from the chain

            return;
        }

        synchronized (hs.handshakeMutex) {
            // Recheck conditions after locking the mutex.
            // Things might have changed while waiting for the lock.
            if (hs.handshakeFailed.get()) {
                return;
            }

            if (hs.handshakeComplete.get()) {
                out("2-Passing e:" + e);
                super.messageReceived(ctx, e);

                return;
            }
            String handshake = getHandshake(e);



            // 1. Validate expected clientId:serverId:challenge format
            String[] params = handshake.trim().split(":");
            if (params.length != 3) {
                out("Invalid handshake: expecting 3 params, " +
                        "got " + params.length + " -> '" + handshake + "'");
                hs.fireHandshakeFailed(ctx);
                logger.warn("Rejecting Handshake 'incorrect params' Client:" + ctx.getChannel().getRemoteAddress());
                return;
            }

            // 2. Validate the asserted serverId == localId
            String client = params[0];
//            if (!this.localId.equals(params[1])) {
//                out("+++ SERVER-HS :: Invalid handshake: this is " +
//                        this.localId + " and client thinks it's " + params[1]);
//                this.fireHandshakeFailed(ctx);
//                return;
//            }

            // 3. Validate the challenge format.
            if (!Challenge.isValidChallenge(params[2])) {
                out("Invalid handshake: invalid challenge '" +
                        params[2] + "'");
                logger.warn("Rejecting Handshake 'invalid challenge' Client:" + ctx.getChannel().getRemoteAddress());
                hs.fireHandshakeFailed(ctx);
                return;
            }

            if (!hostsFilter.isValid(ctx.getChannel().getRemoteAddress())) {
                logger.warn("Rejecting InvalidIP");
                hs.fireHandshakeFailed(ctx);

            }

            // Success! Write the challenge response.
            out("Challenge validated, flushing messages & " +
                    "removing handshake handler from  pipeline.");
            String response = params[0] + ':' + params[1] + ':' + Challenge.generateResponse(params[2]) + ":" + Challenge.serverKey + '\n';
            this.writeDownstream(ctx, response);

            hs.fireHandshakeSucceeded(clientId, ctx);
            ctx.getPipeline().remove(this);
        }
    }

    private boolean isHandshake(MessageEvent e) {
        return getHandshake(e).startsWith("_HANDSHAKE_");
    }
    private String getHandshake(MessageEvent e) {
        String handshake = "";
        if (e.getMessage() instanceof String) {
            handshake = (String) e.getMessage();
        } else if (e.getMessage() instanceof ChannelBuffer) {
            ChannelBuffer cb = (ChannelBuffer) e.getMessage();
            handshake = new String(cypherHandler.decrypt(cb.array()));
        }
        return handshake;
    }

    @Override
    public void channelConnected(final ChannelHandlerContext ctx,
                                 ChannelStateEvent e) throws Exception {
        this.group.add(ctx.getChannel());
        out("Incoming connection established from: " +
                e.getChannel().getRemoteAddress());



        new Thread() {

            @Override
            public void run() {
                try {
                    hs.latch.await(timeoutInMillis, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e1) {
                    out("Handshake timeout checker: " +
                            "interrupted!");
                    e1.printStackTrace();
                }

                if (hs.handshakeFailed.get()) {
                    out("(pre-synchro) Handshake timeout " +
                            "checker: discarded (handshake failed)");
                    return;
                }

                if (hs.handshakeComplete.get()) {
//                    out("(pre-synchro) Handshake timeout " +
//                            "checker: discarded (handshake complete)");
                    return;
                }

                synchronized (hs.handshakeMutex) {
                    if (hs.handshakeFailed.get()) {
                        out("(synchro) Handshake timeout " +
                                "checker: already failed.");
                        return;
                    }

                    if (!hs.handshakeComplete.get()) {
                        out("********** (synchro) Handshake timeout " +
                                "checker: timed out, killing connection.");
                        ctx.sendUpstream(HandshakeEvent.handshakeFailed(ctx.getChannel()));
                        hs.handshakeFailed.set(true);
                        ctx.getChannel().close();
                    } else {
                        out("(synchro) Handshake timeout " +
                                "checker: discarded (handshake OK)");
                    }
                }
            }
        }.start();
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        out("Channel closed.");
        if (!this.hs.handshakeComplete.get()) {
            this.hs.fireHandshakeFailed(ctx);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        out("Exception caught.");
        e.getCause().printStackTrace();
        if (e.getChannel().isConnected()) {
            // Closing the channel will trigger handshake failure.
            e.getChannel().close();
        } else {
            // Channel didn't open, so we must fire handshake failure directly.
            this.hs.fireHandshakeFailed(ctx);
        }
    }


    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        // Before doing anything, ensure that noone else is working by
        // acquiring a lock on the handshakeMutex.
        synchronized (this.hs.handshakeMutex) {
            if (this.hs.handshakeFailed.get()) {
                // If the handshake failed meanwhile, discard any messages.
                return;
            }

            // If the handshake hasn't failed but completed meanwhile and
            // messages still passed through this handler, then forward
            // them downwards.
            if (this.hs.handshakeComplete.get()) {
                out("Handshake already completed, not " +
                        "appending '" + e.getMessage().toString().trim() +
                        "' to queue!");
                super.writeRequested(ctx, e);
            } else {
                // Otherwise, queue messages in order until the handshake
                // completes.
                this.messages.offer(e);
            }
        }
    }

    // private static helpers -------------------------------------------------

    private void out(String s) {
        if (isOutEnabled) System.err.println(Thread.currentThread().getName() + "+++ SERVER-HS :: " + s);
    }

    // private helpers --------------------------------------------------------

    private void writeDownstream(ChannelHandlerContext ctx, String data) {
        // Just declaring these variables so that last statement in this
        // method fits inside the 80 char limit... I typically use 120 :)
        ChannelFuture f = Channels.succeededFuture(ctx.getChannel());
        SocketAddress address = ctx.getChannel().getRemoteAddress();
        Channel c = ctx.getChannel();


        ChannelBuffer channelBuffer = getChannelBuffer();
        channelBuffer.writeBytes(data.getBytes());
        ctx.sendDownstream(new DownstreamMessageEvent(c, f, channelBuffer, address));
    }
    public ChannelBuffer getChannelBuffer() {
        return dynamicBuffer();
    }

    private class HandshakeStatus {
        private final AtomicBoolean handshakeComplete = new AtomicBoolean();
        private final AtomicBoolean handshakeFailed = new AtomicBoolean();
        private final Object handshakeMutex = new Object();
        private final CountDownLatch latch = new CountDownLatch(1);

        void fireHandshakeFailed(ChannelHandlerContext ctx) {
            this.handshakeComplete.set(true);
            this.handshakeFailed.set(true);
            this.latch.countDown();
            ctx.getChannel().close();
            ctx.sendUpstream(HandshakeEvent.handshakeFailed(ctx.getChannel()));
        }

        void fireHandshakeSucceeded(String client,
                                    ChannelHandlerContext ctx) {
            this.handshakeComplete.set(true);
            this.handshakeFailed.set(false);
            this.latch.countDown();
            ctx.sendUpstream(HandshakeEvent.handshakeSucceeded(client, ctx.getChannel()));
        }

        public boolean isPassed() {
            boolean complete = this.handshakeComplete.get();
            if (complete) {
                return !this.handshakeFailed.get();
            }
            try {
                latch.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            complete = this.handshakeComplete.get();
            if (complete) {
                return !this.handshakeFailed.get();
            }
            return false;
        }
    }


}