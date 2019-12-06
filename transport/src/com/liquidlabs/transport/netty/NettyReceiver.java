package com.liquidlabs.transport.netty;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.ProtocolParser;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.netty.handshake.ServerHandshakeHandler;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.http.HttpServerCodec;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.ssl.SslContext;
import org.jboss.netty.util.CharsetUtil;
import org.joda.time.DateTime;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;


import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

@ChannelHandler.Sharable
public class NettyReceiver extends SimpleChannelUpstreamHandler implements Receiver {
    private static final String DASH = "-";
	private static final String PC = "%";
	private static final String USCORE = "_";
	private static final String COLON = ":";
	static final Logger LOGGER = Logger.getLogger(NettyReceiver.class);
    final DefaultChannelGroup allChannels;

    private ProtocolParser protocolParser;
    State state = State.STOPPED;
    private ServerBootstrap bootstrap;
    private final URI endPoint;
	private final ServerSocketChannelFactory factory;
	private Channel channel;
    private final boolean isString;
    private static int maxStringSizeKb = Integer.getInteger("socket.string.max.kb",96) * 1024;
    private static int serverHandshakeTimeOut = Integer.getInteger("socket.handshake.timeout.s", 30) * 1000;
    private SslContext sslCtx = null;

    public NettyReceiver(final URI endPoint, ServerSocketChannelFactory factory, ProtocolParser protocolParser, final boolean isSecureHandshake) throws IOException {

        isString = endPoint.toString().startsWith("raw");
        this.endPoint = endPoint;
		this.factory = factory;
        sslCtx = buildSSLCtx();

        bootstrap = new ServerBootstrap(factory);


        allChannels = new DefaultChannelGroup("SERVER"+endPoint.toString());
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                if (isString) {
                    return Channels.pipeline(
                            new DelimiterBasedFrameDecoder(maxStringSizeKb, Delimiters.lineDelimiter()),
                            //new StringDecoder(CharsetUtil.UTF_8),
                            new HttpServerCodec(),
                            NettyReceiver.this
                    );
                } else if (isSecureHandshake) {
                    return Channels.pipeline(
                            new ServerHandshakeHandler("server", allChannels, serverHandshakeTimeOut),
                            NettyReceiver.this
                    );

                } else {
                    // Add SSL handler first to encrypt and decrypt everything.
                    // In this example, we use a self-signed certificate in the server side
                    // and accept any invalid certificates in the client side.
                    // You will need something more complicated to identify both
                    // and server in the real world.

                    return getUriPipeline(endPoint);
                }
            }
        });


        bootstrap.setOption("localddress", new InetSocketAddress(endPoint.getPort()));
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("backlog", 1024);
        bootstrap.setOption("connectTimeoutMillis", serverHandshakeTimeOut);

        Channel bind = bootstrap.bind(new InetSocketAddress(endPoint.getPort()));
        this.channel = bind;
        


        allChannels.add(bind);
        this.protocolParser = protocolParser;
    }

    private SslContext buildSSLCtx(){
        try {
            return new File(TransportProperties.SSL_CERT).exists() ? SslContext.newServerContext(new File(TransportProperties.SSL_CERT), new File(TransportProperties.SSL_KEY)) : null;
        } catch (SSLException e) {
            e.printStackTrace();
        }
        if(LOGGER.isDebugEnabled()) LOGGER.debug("SSL Comms disabled, missing cert files:" + TransportProperties.SSL_CERT + " CWD:" + new File(".").getAbsolutePath());
        return null;
    }
    public ChannelPipeline getUriPipeline(URI endpoint){
        if(LOGGER.isDebugEnabled()) LOGGER.debug("Starting " + endpoint);

        if(endpoint.getScheme().toLowerCase().equals("stcp") && sslCtx != null) return Channels.pipeline(sslCtx.newHandler(), NettyReceiver.this);

        return Channels.pipeline(NettyReceiver.this);
    }

    private void makeSSLCerts() {
        //        SelfSignedCertificate ssc = null;
//        try {
//            ssc = new SelfSignedCertificate(TransportProperties.getSSLDomain());
//        } catch (CertificateException e) {
//            e.printStackTrace();
//        }
//        FileUtil.copyFile(ssc.certificate(), new File("ssl/p2p.crt"));
//        FileUtil.copyFile(ssc.privateKey(), new File("ssl/p2p.key"));
        //final SslContext sslCtx = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
        if (ctx.getAttachment() == null) ctx.setAttachment(new StreamState());
        final StreamState attachment = (StreamState) ctx.getAttachment();
        StreamState newState = null;
        try {
            attachment.payload = null;
            attachment.reply = null;

            if (attachment.ipAddress == null) {
                String[] addrHost = getAddressFromChannel(ctx, e);
                attachment.ipAddress = addrHost[0];
                attachment.hostname = addrHost[1];
                attachment.serverURI = this.endPoint.toString();
            }

            if (isString) {
                StringProtocolParser pp = (StringProtocolParser) protocolParser;
                newState = pp.process((String) e.getMessage(), attachment);
            } else {
                final ChannelBuffer channelReadMessage = (ChannelBuffer) e.getMessage();

                newState = protocolParser.process(channelReadMessage, attachment, new ReplySender(ctx.getChannel()), new CMDProcessor(endPoint, ctx.getChannel(), attachment.ipAddress));
                if (newState.t != null) {
                    LOGGER.error("BAD msg received from:" + e.getChannel().getRemoteAddress(), newState.t);
                    newState.t = null;
                }
                if (newState.reply != null && newState.getType().equals(Type.SEND_REPLY)) {
                    byte[] reply = newState.reply;
                    try {
                        NettySenderImpl.writeLLMessageToChannel(reply, Type.RESPONSE, dynamicBuffer(), ctx.getChannel());
                    } catch (RetryInvocationException e1) {
                        LOGGER.error(e1);
                    }
                }
            }

        } catch (Exception e1) {
            LOGGER.warn(e1.getMessage(), e1);

        } finally {
            if (newState != null) {
                newState.reply = null;
                newState.payload = null;
                newState.ipAddress = attachment.ipAddress;
                newState.hostname = attachment.hostname;
                ctx.setAttachment(newState);
            }
        }

    }
    public class ReplySender {
    	private final Channel channel;

		public ReplySender(Channel channel) {
			this.channel = channel;
		}

		void sendReply(byte[] reply) {
    		  try {
                  NettySenderImpl.writeLLMessageToChannel(reply, Type.RESPONSE, dynamicBuffer(),channel);
              } catch (Exception e1) {
                  LOGGER.error(e1);
              }
    	}
    }
    final public static String[] getAddressFromChannel(ChannelHandlerContext ctx, MessageEvent event) {
    	try {
	    	InetSocketAddress remoteAddress = (InetSocketAddress) event.getRemoteAddress();
	    	final String address = remoteAddress.getAddress().getHostAddress().replace(PC, DASH).replace(":",DASH);// + USCORE + remoteAddress.getPort();
	    	final String host = NetworkUtils.resolveHostname(remoteAddress);
	    	return new String[] { address, host };
    	} catch (Throwable t) {
    		LOGGER.warn("AddrResolveFailed:" + t.toString());
    		return new String[] { "unknown", "unknown" };
    	}
    }

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (TransportProperties.isConnectionDebug())
            LOGGER.info(endPoint + " Connected:" + e.getChannel().getRemoteAddress());
        allChannels.add(e.getChannel());
    }

    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        if (TransportProperties.isConnectionDebug())
            LOGGER.info(endPoint + " Disconnected:" + e.getChannel().getRemoteAddress());
     //   allChannels.remove(e.getChannel());
    }

    public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
        return null;
    }

    public void start() {
        if (state == State.STARTED) return;
        state = State.STARTED;
    }

    public void stop() {
        if (state == State.STOPPED) return;
        state = State.STOPPED;
		System.out.println(">> NettyReceiver ******* ClosingServer:" + allChannels);
        ChannelGroupFuture future = allChannels.close();
        
        try {
			future.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        System.out.println("<< NettyReceiver ******* ClosingServer:" + allChannels);
    }

    int spamCount;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (spamCount++ < 25 || spamCount % 10 == 0) {
            e.getCause().printStackTrace();
        	System.err.println(new DateTime() + " " + this.toString() + " " + Thread.currentThread() + " ERROR:" + e);
            LOGGER.warn(this.endPoint.toString() + " Exception:" + e, e.getCause());
        }
    }

    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("[NettyReceiver:");
        buffer.append(" allChannels:");
        buffer.append(allChannels.size());
        buffer.append(" endPoint:");
        buffer.append(endPoint);
        buffer.append("]");
        return buffer.toString();
    }
	public boolean isForMe(Object payload) {
		throw new RuntimeException("Not implemented");
	}
	public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
		throw new RuntimeException("Not implemented");
	}


}
