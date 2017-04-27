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
import org.jboss.netty.channel.socket.oio.OioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;

import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;

public class NettyEndPointFactory implements EndPointFactory {
	
	private static final Logger LOGGER = Logger.getLogger(NettyEndPointFactory.class);

	Map<URI, NettyEndPoint> endPoints = new ConcurrentHashMap<URI, NettyEndPoint>();
	// NOTE - these were static - removed now to see if it fixes rabo's stability problem
	private ExecutorService bossExec1;
	private ExecutorService workExec2;
	private SenderFactory senderFactory;
	private ClientSocketChannelFactory nettyClientFactory;
	private ServerSocketChannelFactory nettyServerFactory;
	private final ScheduledExecutorService scheduler;
	
	// this will set properties to change which socket channel factory to use
	private boolean restrictClientPorts = new ClientPortRestrictedDetector().isBootPropertiesSetValueToTrue();

	private final String serviceName;

	public NettyEndPointFactory(ScheduledExecutorService scheduler, String serviceName) {

		LOGGER.info("CREATED:" + serviceName);
        ThreadRenamingRunnable.setThreadNameDeterminer(new ThreadNameDeterminer() {
            @Override
            public String determineThreadName(String currentThreadName, String proposedThreadName) throws Exception {
                return proposedThreadName.replace(" ","_").replace("#","");
            }
        });

		this.scheduler = scheduler;
		this.serviceName = serviceName;

		if (bossExec1 == null) {
			bossExec1 = Executors.newCachedThreadPool(new NamingThreadFactory("NTY-BOSS-" + serviceName, true, Thread.MAX_PRIORITY));

			// use at most 16 worker threads for the client size
			int workerPoolSize = Math.min(Integer.getInteger("netty.client.workers", 16), Runtime.getRuntime().availableProcessors() * 2);
			workExec2 = Executors.newCachedThreadPool(new NamingThreadFactory("NTY-WKR-" + serviceName, true, Thread.NORM_PRIORITY + 1));

			if (Boolean.getBoolean("tcp.use.oio.server") || Boolean.getBoolean("nw.oio")) {
				LOGGER.info("Using OIOServerSockets");
				nettyServerFactory = new OioServerSocketChannelFactory(bossExec1, workExec2);
			} else {
				LOGGER.info("Using NIOServerSockets");
				nettyServerFactory = new NioServerSocketChannelFactory(bossExec1, workExec2, workerPoolSize);
			}
			if (Boolean.getBoolean("tcp.use.oio.client") || Boolean.getBoolean("nw.oio")) {
				LOGGER.info("Using OIOClientSockets");
				nettyClientFactory = new OioClientSocketChannelFactory(bossExec1);
			} else {
				LOGGER.info("Using NIOClientSockets");
				nettyClientFactory = new NioClientSocketChannelFactory(bossExec1, workExec2, workerPoolSize);
			}

			if (Boolean.getBoolean("tcp.use.simple.pool")) {
				LOGGER.info("Using NettySimplePool");
				senderFactory = new NettySimpleSenderFactory(nettyClientFactory);

			} else {
				LOGGER.info("Using NettyMultiPool");
				senderFactory = new NettyPoolingSenderFactory(nettyClientFactory, false);
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
        LOGGER.info("CREATE -- IP:" + uri);

		NettyEndPoint nettyEndPoint = new NettyEndPoint(serviceName, uri, receiver, nettyServerFactory, senderFactory, scheduler);
		nettyEndPoint.start();
		endPoints.put(uri, nettyEndPoint);
		return nettyEndPoint;
	}

	public void start() {
	}

	public void stop() {
		Collection<NettyEndPoint> values = endPoints.values();
		for (NettyEndPoint endPoint : values) {
			endPoint.stop();
		}
		
		releasePools();
	}

	private void releasePools() {
		endPoints.clear();
		bossExec1.shutdownNow();
		ThreadPoolExecutor exey = (ThreadPoolExecutor) workExec2;
		int wait = 0;
		while (exey.getActiveCount() > 0 && wait++ < 2) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(getClass().getSimpleName() + " ACTIVE:" + exey.getActiveCount());
		}
		if (exey.getActiveCount() == 0) {
			nettyClientFactory.releaseExternalResources();
			nettyServerFactory.releaseExternalResources();
		} else {
			System.out.println(getClass().getSimpleName() + " STILL_ACTIVE:" + exey.getActiveCount());
		}
	}
}
