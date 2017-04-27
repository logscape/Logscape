package com.liquidlabs.transport.netty.handshake;

import com.liquidlabs.transport.netty.handshake.common.Challenge;
import com.liquidlabs.transport.netty.handshake.common.HandshakeEvent;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

public class ClientHandshakeHandler extends SimpleChannelHandler {
    private static final Logger LOGGER = Logger.getLogger(ClientHandshakeHandler.class);

    // internal vars ----------------------------------------------------------

    private final long timeoutInMillis;
    private final String localId;
    private final String remoteId;
    private final AtomicBoolean handshakeComplete;
    private final AtomicBoolean handshakeFailed;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Queue<MessageEvent> messages = new ArrayDeque<MessageEvent>();
    private final Object handshakeMutex = new Object();
    private String challenge;
    boolean isOutEnabled = Boolean.getBoolean("server.handshake.verbose");
    CyperHandler cypherHandler = new CyperHandler();

    // constructors -----------------------------------------------------------

    public ClientHandshakeHandler(String localId, String remoteId,
                                  long timeoutInMillis) {

        out("Creating Handler");
        this.localId = "_HANDSHAKE_" + Math.random() * 1000  ;// localId.replaceAll(":","_");
        this.remoteId = remoteId;
        this.timeoutInMillis = timeoutInMillis;
        this.handshakeComplete = new AtomicBoolean(false);
        this.handshakeFailed = new AtomicBoolean(false);
    }

    // SimpleChannelHandler ---------------------------------------------------

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {

        out("Message Received");

        if (this.handshakeFailed.get()) {
            // Bail out fast if handshake already failed
            return;
        }

        if (this.handshakeComplete.get()) {
            // If handshake succeeded but message still came through this
            // handler, then immediately send it upwards.
            // Chances are it's the last time a message passes through
            // this handler...
            super.messageReceived(ctx, e);
            return;
        }

        out("Message Waiting");

        synchronized (this.handshakeMutex) {
            out("Message Waiting>>> ");
            // Recheck conditions after locking the mutex.
            // Things might have changed while waiting for the lock.
            if (this.handshakeFailed.get()) {
                out("Message Rejected");
                return;
            }

            if (this.handshakeComplete.get()) {
                super.messageReceived(ctx, e);
                return;
            }


            String payload = "";
            Object message1 = e.getMessage();
            if (message1 instanceof String) {
                payload = (String) e.getMessage();
            } else if (message1 instanceof  ChannelBuffer) {
                ChannelBuffer cb = (ChannelBuffer) message1;
                payload = new String(cb.array());
            }
            String[] params =  payload.trim().split(":");

            // Parse the challenge.
            // Expected format is "clientId:serverId:challenge"
            if (params.length != 4) {
                out("Invalid handshake: expected 3 params, " +
                        "got " + params.length);
                this.fireHandshakeFailed(ctx);
                return;
            }

            // Silly validations...
            // 1. Validate that server replied correctly to this client's id.
            if (!params[0].equals(this.localId)) {
                out("Handshake failed: local id is " +
                        this.localId +" but challenge response is for '" +
                        params[0] + "'");
                this.fireHandshakeFailed(ctx);
                return;
            }

            // 2. Validate that asserted server id is its actual id.
            if (!params[1].equals(this.remoteId)) {
                out("Handshake failed: expecting remote id " +
                        this.remoteId + " but got " + params[1]);
                this.fireHandshakeFailed(ctx);
                return;
            }

            // 3. Ensure that challenge response is correct and the .
            if (!Challenge.isValidResponse(params[2], params[3])) {
                String sss = "Handshake failed: '" + params[2] +
                        "' is not a valid response for challenge '" +
                        this.challenge + "'";
                LOGGER.warn(sss);
                out(sss);
                this.fireHandshakeFailed(ctx);
                return;
            }

            // Everything went okay!
            out("Challenge validated, flushing messages & " +
                    "removing handshake handler from pipeline.");

            // Flush messages *directly* downwards.
            // Calling ctx.getChannel().write() here would cause the messages
            // to be inserted at the top of the pipeline, thus causing them
            // to pass through this class's writeRequest() and be re-queued.
            Thread.sleep(100);
            out("" + this.messages.size() +
                    " messages in queue to be flushed.");
            for (MessageEvent message : this.messages) {
                out("SendingMessage:" + message);
                ctx.sendDownstream(message);
            }

            // Remove this handler from the pipeline; its job is finished.
        //    ctx.getPipeline().remove(this);

            // Finally fire success message upwards.
            this.fireHandshakeSucceeded(this.remoteId, ctx);
        }
    }

    @Override
    public void channelConnected(final ChannelHandlerContext ctx,
                                 ChannelStateEvent e) throws Exception {
        out("--- CLIENT-HS :: Outgoing connection established to: " +
                e.getChannel().getRemoteAddress());

        // Write the handshake & add a timeout listener.
        ChannelFuture f = Channels.future(ctx.getChannel());
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                // Once this message is sent, start the timeout checker.
                new Thread() {
                    @Override
                    public void run() {
                        // Wait until either handshake completes (releases the
                        // latch) or this latch times out.
                        try {
                            latch.await(timeoutInMillis, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e1) {
                            out("Handshake timeout checker: " +
                                    "interrupted!");
                            e1.printStackTrace();
                        }

                        // Informative output, do nothing...
                        if (handshakeFailed.get()) {
//                            out("(pre-synchro) Handshake " +
//                                    "timeout checker: discarded " +
//                                    "(handshake failed)");
                            return;
                        }

                        // More informative output, do nothing...
                        if (handshakeComplete.get()) {
                            out("(pre-synchro) Handshake " +
                                    "timeout checker: discarded" +
                                    "(handshake completed)");
                            return;
                        }

                        // Handshake has neither failed nor completed, time
                        // to do something! (trigger failure).
                        // Lock on the mutex first...
                        synchronized (handshakeMutex) {
                            // Same checks as before, conditions might have
                            // changed while waiting to get a lock on the
                            // mutex.
                            if (handshakeFailed.get()) {
                                out("(synchro) Handshake " +
                                        "timeout checker: already failed.");
                                return;
                            }

                            if (!handshakeComplete.get()) {
                                // If handshake wasn't completed meanwhile,
                                // time to mark the handshake as having failed.
                                out("(synchro) Handshake " +
                                        "timeout checker: timed out, " +
                                        "killing connection.");
                                fireHandshakeFailed(ctx);
                            } else {
                                // Informative output; the handshake was
                                // completed while this thread was waiting
                                // for a lock on the handshakeMutex.
                                // Do nothing...
                                out("(synchro) Handshake " +
                                        "timeout checker: discarded " +
                                        "(handshake OK)");
                            }
                        }
                    }
                }.start();
            }
        });

        this.challenge = Challenge.generateChallenge();
        String handshake =
               this.localId + ':' + this.remoteId + ':' + challenge + '\n';
        Channel c = ctx.getChannel();

        ChannelBuffer channelBuffer = getChannelBuffer();
        channelBuffer.writeBytes( cypherHandler.encrypt(handshake.getBytes()));

        // Passing null as remoteAddress, since constructor in
        // DownstreamMessageEvent will use remote address from the channel if
        // remoteAddress is null.
        // Also, we need to send the data directly downstream rather than
        // call c.write() otherwise the message would pass through this
        // class's writeRequested() method defined below.

        ctx.sendDownstream(new DownstreamMessageEvent(c, f, channelBuffer, null));
    }
    public ChannelBuffer getChannelBuffer() {
        return dynamicBuffer();
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        out("Channel closed.");
        if (!this.handshakeComplete.get()) {
            this.fireHandshakeFailed(ctx);
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
            this.fireHandshakeFailed(ctx);
        }
    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
        out(">>Write Message");
        while (!this.handshakeComplete.get()) {
            Thread.sleep(1000);
            out("Waiting");
        }

//        out(">>> Write Message ------------------------>" + this.handshakeComplete.get());
//        if (true) {
//            super.writeRequested(ctx, e);
//            return;
//        }

        // Before doing anything, ensure that noone else is working by
        // acquiring a lock on the handshakeMutex.
        synchronized (this.handshakeMutex) {
            if (this.handshakeFailed.get()) {
                // If the handshake failed meanwhile, discard any messages.
                return;
            }

            // If the handshake hasn't failed but completed meanwhile and
            // messages still passed through this handler, then forward
            // them downwards.
            if (this.handshakeComplete.get()) {
//                out("Handshake already completed, not " +
//                        "appending '" + e.getMessage().toString().trim() +
//                        "' to queue!");
                out(" Write Message -->");
                super.writeRequested(ctx, e);
            } else {
                // Otherwise, queue messages in order until the handshake
                // completes.
                out(" Offer Message -->");
                this.messages.offer(e);
            }
        }
        out("<<< Write Message ------------------------>");
    }

    // private static helpers -------------------------------------------------

    private void out(String s) {
        if (isOutEnabled) System.err.println(Thread.currentThread().getName() + " = " + hashCode() + " -- ClientHS ::" + s);
    }

    // private helpers --------------------------------------------------------

    private void fireHandshakeFailed(ChannelHandlerContext ctx) {
        out("FireFailed ****************** ");
        this.handshakeComplete.set(true);
        this.handshakeFailed.set(true);
        this.latch.countDown();
        ctx.getChannel().close();
        ctx.sendUpstream(HandshakeEvent.handshakeFailed(ctx.getChannel()));
    }

    private void fireHandshakeSucceeded(String server,
                                        ChannelHandlerContext ctx) {
        out("FireSuccess ******************** ");
        this.handshakeComplete.set(true);
        this.handshakeFailed.set(false);
        this.latch.countDown();
        ctx.sendUpstream(HandshakeEvent
                .handshakeSucceeded(server, ctx.getChannel()));
    }
}