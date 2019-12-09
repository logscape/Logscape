/**
 * 
 */
package com.liquidlabs.transport.netty;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.DefaultSocketChannelConfig;
import org.jboss.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLException;
import java.io.File;
import java.lang.reflect.Field;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static com.liquidlabs.transport.protocol.NetworkConfig.HEADER_BYTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NettySenderImpl implements Sender {

	static boolean debugCreation = Boolean.getBoolean("client.creation.debug");

	private static final String CONNECT_TIMEOUT_MILLIS = "connectTimeoutMillis";

	private static final String HANDLER = "handler";

	private static final String KEEP_ALIVE = "keepAlive";
    private static final Boolean isKeepAlive = System.getProperty("client.keepalive","true").equals("true");

	private static final String TCP_NO_DELAY = "tcpNoDelay";

	private static final String RAW = "raw";

	static final Logger LOGGER = Logger.getLogger(NettySenderImpl.class);
	
	private final ChannelFactory nettyClientFactory;
	private NettyClientHandler handler;
	private URI uri;
	
	private Channel channel;
	private ChannelFuture channelFuture;
	private ChannelFuture writeFuture;
	
	private boolean restrictClientPorts;
	private boolean portScanDebug = true;//Boolean.getBoolean("port.scan.debug");

	LifeCycle.State state = LifeCycle.State.UNASSIGNED;
	
	private int connectionEstablishTimeoutSecs = TransportProperties.getConnectionEstablishTimeout();

	private String context;
	String id = UID.getUUID();

    private int msgsSent;

    public NettySenderImpl(URI uri, ChannelFactory factory, boolean restrictClientPorts) {
		this.uri = uri;
        if (uri.getPort() == -1) {
            throw new RuntimeException("Given Invalid URI:" + uri);
        }

		//RuntimeException re = new RuntimeException("Creating:" + uri);
		//LOGGER.warn("CREATED: " + uri, re);

		nettyClientFactory = factory;
		this.restrictClientPorts = restrictClientPorts;
    }

	public Throwable getException() {
		return handler == null ? null : handler.getException();
	}

	public void dumpStats() {
	}

	public URI getAddress() {
		return null;
	}

	synchronized public byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowlocalRoute) throws InterruptedException, RetryInvocationException {

		msgsSent++;
        if (isReplyExpected) {
			handler.setExpectsReply();
			type = Type.SEND_REPLY;
		}
        if (state != State.STARTED) {
        	System.out.println("XXXXXXXXXX Attempt to use invalid channel:" + state);
        	return null;
        }
		
        if (TransportProperties.isConnectionDebug()) {
        	LOGGER.info(String.format(" Send %s => %s INFO[%s]", channel.getLocalAddress(), channel.getRemoteAddress(), info));
        }

		if (protocol.startsWith(RAW)) {
			writeStringMessageToChannel(bytes, type, handler.getChannelBuffer(), channel);
		} else  {
			try {
				writeFuture = writeLLMessageToChannel(bytes, type, handler.getChannelBuffer(), channel);
			} catch (Exception e) {
				LOGGER.error(e);
			}
		}
		
		if (isReplyExpected) {
			try {
				return handler.getReply(timeoutSeconds, String.format("%s[%s]", endPoint,info));
			} catch (RetryInvocationException t) {
				LOGGER.warn("Failed to get Reply Client:" + this.toString());
				throw t;
			}
		} else {
			channelFuture.await();
			if (handler.isException()) {
				LOGGER.warn("Handler got exception:" + handler.getException());
				Throwable exception = handler.getException();
				if (exception != null) {
					throw new RuntimeException(exception);
				}
				throw new RuntimeException("Failed to send:" + handler);
			}
		}
	
		return new byte[0];
	}


	public static ChannelFuture writeStringMessageToChannel(byte[] bytes, Type type, ChannelBuffer buffer, Channel channel) throws RetryInvocationException {
		try {
			buffer.writeBytes(bytes);
			return channel.write(buffer);
		} catch (Throwable t) {
			throw new RetryInvocationException("WriteStringToChannelFailed:" + t.getMessage(), t);
		}
	}
	public static ChannelFuture writeLLMessageToChannel(byte[] bytes, Type type, ChannelBuffer buffer, Channel channel) throws RetryInvocationException {
		try {
			if (channel == null) {
				throw new RuntimeException("Given NULL Channel");
			}
			buffer.writeBytes(HEADER_BYTES);
			buffer.writeInt(type.ordinal());
			buffer.writeInt(bytes.length);
			buffer.writeBytes(bytes);
			ChannelFuture write = channel.write(buffer);
			return write;
		} catch (Throwable t) {
			LOGGER.warn("Failed to writeLLMsg:", t);
			throw new RetryInvocationException("WriteLLMsgToChannelFailed:" + t.getMessage(), t);
		}
	}

	public void start() {
		state = State.PENDING;
		
		
        ClientBootstrap bootstrap = getBootstrap(uri.getHost(), uri.getPort());
        

		InetSocketAddress remoteAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
        InetSocketAddress localAddress = null;
                
        boolean success = false;
		if (restrictClientPorts) {
    		if (portScanDebug) LOGGER.info("CONNECTING SOCKET TO:" + uri + " RESTRICT:" + restrictClientPorts);
    		success = scanForAvailablePortAndConnection(bootstrap, remoteAddress, success);
    		if (!success) handleFailedToStart();
    	} else {
    		
    		// use ephemeral ports
    		channelFuture = bootstrap.connect(remoteAddress, localAddress);
			try {
				channelFuture.await(TransportProperties.getConnecionTimeOut(), TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			channel = channelFuture.getChannel();
    	}
    	
    	if (channelFuture.getCause() != null) {
            String msg = this.toString() + " Context:" + context;
    		throw new RuntimeException("ConnectFutureException:" + msg + " uri:" + uri + " ex:" + channelFuture.getCause().toString(), channelFuture.getCause());
    	}
    	
    	if (!channelFuture.isSuccess())  throw new RuntimeException("ChannelFuture.isSuccess_FALSE:" + uri + " ");
    	
    	if (channel != null && !channel.isConnected()) throw new RuntimeException("ChannelConnectFailed:" + uri, channelFuture.getCause());
    	
    	
		if (channel == null) handleFailedToStart();
		
		
		state = State.STARTED;
		
        handler.start();

		if (TransportProperties.isConnectionDebug()) {
			LOGGER.info(String.format("Started: %s => %s", channel.getLocalAddress(), channel.getRemoteAddress()));
		}

	}

	private boolean scanForAvailablePortAndConnection(ClientBootstrap bootstrap, InetSocketAddress remoteAddress, boolean success) {
		
		try {
			InetSocketAddress localAddress;
			boolean connected = false;
			int attempts = 0;
			int increment = 1;
			if (portScanDebug) System.out.println("> GET PORT:");

			while (!connected && attempts++ < TransportProperties.getClientMaxPorts()*3 && !isStopped()) {
				int portBeingUsed = TransportProperties.getClientBasePort();
				try {
					if (portScanDebug) System.out.println("  -- try:" + portBeingUsed + " attempts:" + attempts);

					localAddress = new InetSocketAddress(portBeingUsed);
					TransportProperties.updateBasePort(localAddress.getPort()+increment);
					channelFuture = bootstrap.connect(remoteAddress, localAddress);
			    	channelFuture.await(connectionEstablishTimeoutSecs, TimeUnit.SECONDS);
			    	Thread.yield();
			    	Thread.yield();
			    	
			    	if (channelFuture.getCause() != null) {
			    		throw channelFuture.getCause();
			    	}
			    	if (handler.isException()) {
			    		throw handler.getException();
			    	}
			    	
			    	channel = channelFuture.getChannel();
			    	
			    	if (channel != null && !channel.isConnected()) {
                        try {
                            Thread.sleep(1000);
                        } catch (Throwable t) {

                        }
                        if (!channel.isConnected()) throw new RuntimeException("ChannelConnectFailed");
                        else {
                            LOGGER.warn("Recovered Connection");
                        }
                    }
			    	
			    	
			    	if (portScanDebug) System.out.println(String.format(" -- checking:%d  BOUND[%b %b %b]", attempts, channel.isBound(), channel.isConnected(), channel.isOpen()));
			    	
			    	if (channelFuture.isDone() && channelFuture.isSuccess() && channel.isBound()) {
			    		if (portScanDebug) System.out.println("  -- connected ClientPORT:" + portBeingUsed);
			    		connected = true;
			    		continue;
			    	}
			    	
			    	if (channelFuture.isCancelled()) throw new RuntimeException("Future Cancelled");			    						
			    	
				} catch (BindException t) {
					Thread.yield();
					bootstrap = getBootstrap(uri.getHost(), uri.getPort());
					//LOGGER.warn(attempts + " XXXXXXXXXXX 2 BAD connection again:" + portBeingUsed + " ->" + remoteAddress);
				} catch (InterruptedException t) {
					throw new RuntimeException(t);
				} catch (ConnectException t) {
					throw t;
				} catch (Throwable t) {
					if (attempts > 100) {
						LOGGER.error("Too many failed scans:" + t.toString(), t);
						throw t;
					}

					if (portScanDebug) {
						System.out.println("Scan throw:" + t);
						t.printStackTrace();
					}
//					throw new RuntimeException(t.getMessage(), t);
					Thread.sleep((long) (300 * Math.random()));
					bootstrap = getBootstrap(uri.getHost(), uri.getPort());
				}
			}
			if (!connected) {
				LOGGER.error("Failed allocate client port, upto:" + TransportProperties.getClientBasePort() + " Attempts:" + attempts);
				throw new RuntimeException("PORT ALLOC FAILED");
			}
			if (portScanDebug) LOGGER.error("< GOT PORT:" + connected);
			return connected;
		} catch (Throwable t) {
			//LOGGER.warn("Failed to alloc port:" + t.getMessage());
			throw new RuntimeException(t);
		} finally {
		}
	}

	private boolean isStopped() {
		// YUCK - this is crap - but when its port scanning it will keep retrying for ages before giving up - we want to interrupt the 
		// scanForPort which gets stuck on a channel.connect(timeout) etc
		return (state == State.STOPPING || state == State.STOPPED || state == State.SUSPENDED);
	}

	private ClientBootstrap getBootstrap(String host, int port) {
		ClientBootstrap bootstrap = new ClientBootstrap(nettyClientFactory);

		handler = new NettyClientHandler(uri);
		ChannelPipeline pipeline = bootstrap.getPipeline();


		if (uri.getScheme().equals("stcp")) {
			LOGGER.info("NettySender Running SSL connection because secure-tcp/stcp was requested");
			SslContext sslCtx = null;
			try {
				if (new File(TransportProperties.SSL_CERT).exists()) {
					sslCtx = SslContext.newClientContext(SimpleSSLTrustManagerFactory.INSTANCE);
				} else {
					LOGGER.error("Failed to load SSL certs from: " + new File(TransportProperties.SSL_CERT).getAbsolutePath());
				}
			} catch (SSLException e) {
				e.printStackTrace();
				LOGGER.warn("Failed to load SSL configuration" + e, e);
			}

			if (sslCtx != null) {
				LOGGER.warn("NettySender Loading SSL CONTEXT");
			  	pipeline.addLast("ssl", sslCtx.newHandler(host, port));
			} else {
				System.out.println("Failed to find SSL certificates");
			}
		}

        pipeline.addLast(HANDLER, handler);

        bootstrap.setOption(TCP_NO_DELAY, true);
        bootstrap.setOption(KEEP_ALIVE, isKeepAlive);
        bootstrap.setOption(CONNECT_TIMEOUT_MILLIS, connectionEstablishTimeoutSecs * 1000);
		return bootstrap;
	}

	private void handleFailedToStart() {
		
		String toString = this.toString();
		stop();
		if (handler.getException() != null) {
			throw new RuntimeException(String.format("%s\n Failed to establish Connection within %d secs:%s, ex:%s", toString, connectionEstablishTimeoutSecs, uri, handler.getException()), handler.getException());					
		} else {
			throw new RuntimeException(String.format("%s\n Failed to establish Connection within %d secs:%s", toString, connectionEstablishTimeoutSecs, uri));
		}
	}

	private void close() {
		try {
			state = State.STOPPING;
			Channel channel = channelFuture.getChannel();
			if (channel != null) {
				try {
					if (LOGGER.isDebugEnabled()) LOGGER.debug("Closing Channel");
					ChannelFuture close = channel.close();
					boolean success = close.awaitUninterruptibly(1000);
					if (!success) LOGGER.info("Failed to close the connecton:" + this.toString());
				} catch (Throwable t) {
					System.out.println("NettySender, failed to close channel:" + t.getMessage());
				}
			}
			
			// on rare occasions you can get a socket which is left open and the close doesnt do anything.
			try {
			
				DefaultSocketChannelConfig config = (DefaultSocketChannelConfig) channel.getConfig();
				Socket socket = getSocket(config);
				if (TransportProperties.isConnectionDebug()) LOGGER.info("Closing Socket:" + socket + " isClosed:" + socket.isClosed());
				if (!socket.isClosed()) socket.close();
			} catch (Throwable t) {
				LOGGER.warn("Failed to close socket properly", t);
			}
			
		} catch (Throwable e) {
			LOGGER.warn("Cleanup failed:" + e);
		}
	}

	private Socket getSocket(DefaultSocketChannelConfig config) throws NoSuchFieldException, IllegalAccessException {
		Field field = DefaultSocketChannelConfig.class.getDeclaredField("socket");
		field.setAccessible(true);
		Object object = field.get(config);
		Socket socket = (Socket) object;
		return socket;
	}
	public boolean validate() {
		if (isStopped() || !channel.isOpen() || !channel.isConnected()) {
			close();
			stop();
			start();
			return false;
		} else {
			flush();
			return true;
		}
	}

	public void flush() {
		
		try {
			if (writeFuture != null && !writeFuture.isDone()) {
				try {
					writeFuture.await(connectionEstablishTimeoutSecs, SECONDS);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}	
		} catch (Throwable t) {
			LOGGER.warn("Flush failed:" + t);
		}
	}
		public void stop() {
			try {
				
				if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("Stopping: %s => %s", channel.getLocalAddress(), channel.getRemoteAddress()));
				flush();
				close();
				try {
					if (writeFuture != null) writeFuture.cancel();
					if (channelFuture != null) channelFuture.cancel();
					ChannelFuture close = channel.close();
					close.await();
				} catch (Throwable t){
					t.printStackTrace();
					
				};
				
				if (state == State.STOPPED) return;
				state = State.STOPPED;
				if (TransportProperties.isConnectionDebug()) {
					LOGGER.info(String.format("Stopping: %s => %s", channel.getLocalAddress(), channel.getRemoteAddress()));
				}
				
				handler.stop();
				
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				state = State.STOPPED;				
			}
	}
	public String toString() {
		String connectionInfo = " :" + uri + " c:" + handler + " msgs:" + msgsSent;
		try {
			Channel channel2 = channelFuture.getChannel();
			DefaultSocketChannelConfig config = (DefaultSocketChannelConfig) channel2.getConfig();
			boolean connected = false;
			boolean closed = false;
			boolean bound = false;
			try {
				connected = getSocket(config).isConnected();
				closed = getSocket(config).isClosed();
				bound = getSocket(config).isBound();
				connectionInfo += String.format(" :  %s => %s bound[%b] open[%b] connected[%b] s.connected[%b] s.bound[%b] s.closed[%b]", 
						channel2.getLocalAddress(), channel2.getRemoteAddress(), 
						channel2.isBound(), channel2.isOpen(), channel2.isConnected(), connected, bound, closed);
			} catch (Throwable t) {
				LOGGER.info("1:" + t.getMessage(),t);
			}
		} catch (Throwable t) {
			LOGGER.info("2:" + t.getMessage(),t);
			
		}
		return getClass().getSimpleName() + ":" + this.hashCode() + connectionInfo;
	}

	public void setContext(String context) {
		this.context = context;
	}
	public int hashCode() {
		return id.hashCode();
	}
	public boolean equals(Object obj) {
		
		return id.equals(((NettySenderImpl)obj).id);
	}

}