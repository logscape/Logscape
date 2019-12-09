package com.liquidlabs.transport.netty;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.EndPointFactory;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.SenderFactory;
import org.apache.log4j.Logger;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.ThreadRenamingRunnable;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class NettyEndPointFactory implements EndPointFactory {
	
	private static final Logger LOGGER = Logger.getLogger(NettyEndPointFactory.class);

	Map<URI, NettyEndPoint> endPoints = new ConcurrentHashMap<URI, NettyEndPoint>();
	private ExecutorService bossExec1;
	private ExecutorService workExec2;
	private SenderFactory senderFactory;
	private ClientSocketChannelFactory nettyClientFactory;
	private ServerSocketChannelFactory nettyServerFactory;
	private final ScheduledExecutorService scheduler;
	
	private final String serviceName;

	public NettyEndPointFactory(ScheduledExecutorService scheduler, String serviceName) {

		LOGGER.info("CREATED:" + serviceName);
        ThreadRenamingRunnable.setThreadNameDeterminer((currentThreadName, proposedThreadName) -> proposedThreadName.replace(" ","_").replace("#",""));

		this.scheduler = scheduler;
		this.serviceName = serviceName;

		if (bossExec1 == null) {
			bossExec1 = Executors.newCachedThreadPool(new NamingThreadFactory("NTY-BOSS-" + serviceName, true, Thread.MAX_PRIORITY));

			int workerPoolSize = Math.min(Integer.getInteger("netty.client.workers", 8), Runtime.getRuntime().availableProcessors() * 2 );
			LOGGER.info("NettyWorkerPool thread count - set netty.client.workers:"+ workerPoolSize);
			workExec2 = Executors.newCachedThreadPool(new NamingThreadFactory("NTY-WKR-" + serviceName, true, Thread.NORM_PRIORITY + 1));

			nettyServerFactory = new NioServerSocketChannelFactory(bossExec1, workExec2, workerPoolSize);
			nettyClientFactory = new NioClientSocketChannelFactory(bossExec1, workExec2, workerPoolSize);

			if (Boolean.getBoolean("tcp.use.simple.pool")) {
				LOGGER.info("Using NettySimplePool");
				senderFactory = new NettySimpleSenderFactory(nettyClientFactory, scheduler);
			} else {
				LOGGER.info("Using NettyMultiPool");
				senderFactory = new NettyPoolingSenderFactory(nettyClientFactory, scheduler);
			}
		}
	}

	public EndPoint getEndPoint(URI uri, Receiver receiver) {
		LOGGER.info("CREATE ENDPOINT:" + uri);

		try {
			uri = new URI(uri.getScheme(), uri.getUserInfo(), "0.0.0.0", uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
		} catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
        LOGGER.debug("CREATE -- IP:" + uri);

		NettyEndPoint nettyEndPoint = new NettyEndPoint(serviceName, uri, receiver, nettyServerFactory, senderFactory, scheduler);
		nettyEndPoint.start();
		endPoints.put(uri, nettyEndPoint);
		return nettyEndPoint;
	}

	public void start() {
	}

	public void stop() {
		releaseNettyResources();

		LOGGER.info("STOPPING:" + this.serviceName);
		Collection<NettyEndPoint> values = endPoints.values();
		for (NettyEndPoint endPoint : values) {
			endPoint.stop();
		}
		endPoints.clear();
	}

	/**
	 * unbind all channels created by the factory,
	 * close all child channels accepted by the unbound channels, and (these two steps so far is usually done using ChannelGroup.close())
	 * call releaseExternalResources().
	 * Please make sure not to shut down the executor until all channels are closed. Otherwise, you will end up with a RejectedExecutionException and the related resources might not be released properly.
	 * Constructor Summary
	 */
	private void releaseNettyResources() {

		nettyClientFactory.releaseExternalResources();
		nettyClientFactory.shutdown();

		nettyServerFactory.releaseExternalResources();
		nettyServerFactory.shutdown();

		LOGGER.debug("Shutting down pools:" + this.serviceName);
		List<Runnable> runnables = bossExec1.shutdownNow();
		if (runnables.size() > 0) LOGGER.warn("STUCK Boss Threads:" + runnables.size());
		ThreadPoolExecutor exec = (ThreadPoolExecutor) workExec2;
		List<Runnable> runnables1 = exec.shutdownNow();
		if (runnables1.size() > 0) LOGGER.warn("STUCK Worker Threads:" + runnables1.size());
	}
}
