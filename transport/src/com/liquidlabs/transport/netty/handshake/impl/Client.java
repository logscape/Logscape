package com.liquidlabs.transport.netty.handshake.impl;

import com.liquidlabs.transport.netty.handshake.ClientHandler;
import com.liquidlabs.transport.netty.handshake.ClientHandshakeHandler;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {

    // internal vars ----------------------------------------------------------

    private final String id;
    private final String serverId;
    private final ClientListener listener;
    private ClientBootstrap bootstrap;
    private Channel connector;

    // constructors -----------------------------------------------------------

    public Client(String id, String serverId, ClientListener listener) {
        this.id = id;
        this.serverId = serverId;
        this.listener = listener;
    }

    // public methods ---------------------------------------------------------

    public boolean start() {
        // Standard netty bootstrapping stuff.
        Executor bossPool = Executors.newCachedThreadPool();
        Executor workerPool = Executors.newCachedThreadPool();
        ChannelFactory factory =
                new NioClientSocketChannelFactory(bossPool, workerPool);
        this.bootstrap = new ClientBootstrap(factory);

        // Declared outside to fit under 80 char limit
        final DelimiterBasedFrameDecoder frameDecoder =
                new DelimiterBasedFrameDecoder(Integer.MAX_VALUE,
                        Delimiters.lineDelimiter());
        this.bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                ByteCounter byteCounter =
                        new ByteCounter("--- CLIENT-COUNTER :: ");
                MessageCounter messageCounter =
                        new MessageCounter("--- CLIENT-MSGCOUNTER :: ");
                ClientHandshakeHandler handshakeHandler =
                        new ClientHandshakeHandler(id, serverId, 5000);

                return Channels.pipeline(byteCounter,
                        frameDecoder,
                        new StringDecoder(),
                        new StringEncoder(),
                        messageCounter,
                        handshakeHandler,
                        new ClientHandler(listener));
            }
        });

        ChannelFuture future = this.bootstrap
                .connect(new InetSocketAddress("localhost", 12345));
        if (!future.awaitUninterruptibly().isSuccess()) {
            System.out.println("--- CLIENT - Failed to connect to server at " +
                    "localhost:12345.");
            this.bootstrap.releaseExternalResources();
            return false;
        }

        this.connector = future.getChannel();
        return this.connector.isConnected();
    }

    public void stop() {
        if (this.connector != null) {
            this.connector.close().awaitUninterruptibly();
        }
        this.bootstrap.releaseExternalResources();
        System.out.println("--- CLIENT - Stopped.");
    }

    public boolean sendMessage(String message) {
        if (this.connector.isConnected()) {
            // Append \n if it's not present, because of the frame delimiter
            if (!message.endsWith("\n")) {
                this.connector.write(message + '\n');
            } else {
                this.connector.write(message);
            }
            return true;
        }

        return false;
    }

    /**
     *
     *
     * Make it work
     *
     *
     */

    public static void runClient(final String id, final String serverId,
                                 final int nMessages)
            throws InterruptedException {

        final AtomicInteger cLast = new AtomicInteger();
        final AtomicInteger clientCounter = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(1);

        // Create a client with custom id, that connects to a server with given
        // id and has a message listener that ensures that ALL messages are
        // received in perfect order.
        Client c = new Client(id, serverId, new ClientListener() {
            @Override
            public void messageReceived(String message) {
                int num = Integer.parseInt(message.trim());
                if (num != (cLast.get() + 1)) {
                    System.err.println("--- CLIENT-LISTENER(" + id + ") " +
                            ":: OUT OF ORDER!!! expecting " +
                            (cLast.get() + 1) + " and got " +
                            message);
                } else {
                    cLast.set(num);
                }

                if (clientCounter.incrementAndGet() >= nMessages) {
                    latch.countDown();
                }
            }
        });

        if (!c.start()) {
            return;
        }

        for (int i = 0; i < nMessages; i++) {
            // This sleep here prevents all messages to be instantly queued
            // in the handshake message queue. Since handshake takes some time,
            // all messages sent during handshake will be queued (and later on
            // flushed).
            // Since we want to test the effect of removing the handshake
            // handler from the pipeline (and ensure that message order is
            // preserved), this sleep helps us accomplish that with a random
            // factor.
            // If lucky, a couple of messages will even hit the handshake
            // handler *after* the handshake has been completed but right
            // before the handshake handler is removed from the pipeline.
            // Worry not, that case is also covered :)
            Thread.sleep(1L);
            c.sendMessage((i + 1) + "\n");
        }

        // Run the client for some time, then shut it down.
        latch.await(10, TimeUnit.SECONDS);
        c.stop();
    }

    public static void main(String[] args) throws InterruptedException {
        // More clients will test robustness of the server, but output becomes
        // more confusing.
        int nClients = 1;
        final int nMessages = 10000;
        // Changing this value to something different than the server's id
        // will cause handshaking to fail.
        final String serverId = "server1";
        ExecutorService threadPool = Executors.newCachedThreadPool();
        for (int i = 0; i < nClients; i++) {
            final int finalI = i;
            threadPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Client.runClient("client" + finalI, serverId,
                                nMessages);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}