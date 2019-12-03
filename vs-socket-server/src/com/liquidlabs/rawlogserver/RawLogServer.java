package com.liquidlabs.rawlogserver;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.rawlogserver.handler.*;
import com.liquidlabs.rawlogserver.handler.fileQueue.DefaultFileQueuerFactory;
import com.liquidlabs.rawlogserver.handler.fileQueue.FileQueuerFactory;
//import com.liquidlabs.rawlogserver.handler.fileQueue.SAASFileQueuerFactory;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.netty.NettyEndPointFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.lookup.ServiceInfo;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Handles RAW text strings and runs them off to disk using the hostname and rolling
 * LogLines will be 1 per [Host], and of the format [DateTime TEXT]
 */
public class RawLogServer implements Receiver {

    public static final String TAG = "SocketServer";

    private final static Logger LOGGER = Logger.getLogger(RawLogServer.class);
    public static final String SOCKET_SERVER_DUMP_RAW = "socket.server.dump.raw";
    public static final String SOCKET_SERVER_HOSTADDR_ONLY = "socket.server.hostaddr.only";
    static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final String rootDir;
    LifeCycle.State state = LifeCycle.State.STOPPED;


    private StreamHandler handler;

    private static TransportFactoryImpl transportFactoryImpl;

    private static NettyEndPointFactory epFactory;

    boolean dumpRawData = Boolean.valueOf(System.getProperty(SOCKET_SERVER_DUMP_RAW, "true"));
    boolean hostAddressOnly = Boolean.valueOf(System.getProperty(SOCKET_SERVER_HOSTADDR_ONLY,"true"));

    public RawLogServer(String rootDir) {
        this.rootDir = rootDir;
    }

    public byte[] receive(final byte[] payload, String remoteAddress, final String remoteHostname) throws InterruptedException {
        if (this.state != State.RUNNING) return null;
        if (hostAddressOnly && remoteAddress.indexOf("_") > 0) remoteAddress = "";

        try {
            handler.handled(payload, remoteAddress, remoteHostname, "/");
        } catch (Throwable t) {
            LOGGER.warn("Receive Failed:" + t.toString(), t);
        }

        return null;
    }
    public void setHostAddressOnly(boolean hostAddressOnly) {
        this.hostAddressOnly = hostAddressOnly;
    }

    public void start() {
        LOGGER.info("Started");
        if (this.state == State.RUNNING) {
            LOGGER.info("Already Started");
        }
        LOGGER.info("Config Property:" + SOCKET_SERVER_DUMP_RAW + " :" + dumpRawData);
        LOGGER.info("Config Property:" + SOCKET_SERVER_HOSTADDR_ONLY + " :" + hostAddressOnly);

//        if (Boolean.getBoolean("metering.enabled")) {
//        if (true) {
////            SAASFileQueuerFactory saasFileQueuerFactory = new SAASFileQueuerFactory(MeterServiceImpl.getRemoteService("mplex", lookupSpace, proxyFactory, true));
//            ContentFilteringLoggingHandler contentWriter = new ContentFilteringLoggingHandler(new DefaultFileQueuerFactory());
//            StandardLoggingHandler logWriter = new StandardLoggingHandler(proxyFactory.getScheduler());
//            logWriter.setTimeStampingEnabled(false);
//            this.handler = new PerAddressHandler(contentWriter);
//
//        } else if (dumpRawData) {
////            ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool(1, new NamingThreadFactory("SPL-Q"));
////
////
////            // raw - content mapping version
////            ContentFilteringLoggingHandler contentWriter = new ContentFilteringLoggingHandler(new DefaultFileQueuerFactory());
////            StandardLoggingHandler rawLogWriter = new StandardLoggingHandler(scheduler);
////            rawLogWriter.setTimeStampingEnabled(false);
//////            CharChunkingHandler nlChunker = new CharChunkingHandler(	contentWriter);
////            TeeingHandler tee = new TeeingHandler(rawLogWriter, contentWriter);
////            PerAddressHandler rawPerAddrStreamer = new PerAddressHandler(tee);
////            this.handler = rawPerAddrStreamer;
//
//            // splunk version
////			SplunkPacketizer splunker = new SplunkPacketizer(scheduler);
////			CharChunkingHandler splChunker = new CharChunkingHandler(splunker);
////			splChunker.setSplitAtStart(true);
////			splChunker.setCHAR("_raw".toCharArray());
////			PerAddressHandler splunkPerAddressStreamer = new PerAddressHandler(splChunker);
//
//            // content based delegator
////			ContentMappingHandler contentMapper = new ContentMappingHandler();
////			contentMapper.addDefaultHandler(rawPerAddrStreamer);
////			contentMapper.addHandler("_raw", splunkPerAddressStreamer);
////			contentMapper.addHandler("_time", splunkPerAddressStreamer);
////			contentMapper.addHandler("_linebreaker", splunkPerAddressStreamer);
////			this.handler = contentMapper;
//
//        } else {
            ContentFilteringLoggingHandler cfH = new ContentFilteringLoggingHandler(new DefaultFileQueuerFactory());
            CharChunkingHandler nlChunker = new CharChunkingHandler(cfH);
            PerAddressHandler paStreamer = new PerAddressHandler(nlChunker);
            this.handler = paStreamer;
//        }
        this.handler.start();

        LOGGER.info("Using Handler:" + this.handler);
        this.state = State.RUNNING;
    }

    public void stop() {
        LOGGER.info("Stopped");
        handler.stop();
        this.state = State.STOPPED;
    }

    public static void main(String[] args) {
        if (args.length != 6) {
            System.err.println("Expected more arguments, got:" + Arrays.toString(args) + " FOR:" + " port, rootDir, lookupURI, hostname, location, protocol " );
        }
        RawLogServer.boot(args[5], Integer.parseInt(args[0]), args[1], args[2], args[3], args[4]);
        try {
            while (true) {
                Thread.sleep(10 * 1000);
            }
        } catch (InterruptedException e) {
            LOGGER.error(e);
        }
    }

    public static RawLogServer boot(final String protocol, final int port, String rootDir, String lookupURI, String hostname, String location) {
        try {
            LOGGER.info("Starting with port:" + port + " root:" + new File(rootDir).getAbsolutePath() + " protocol:" + protocol + " pid:" + PIDGetter.getPID());



            int foundPort = NetworkUtils.determinePort(port);
            if (port != foundPort) {
                System.err.println("Port-Clash on port:" + port + " cannot start SysLogServer");
                System.exit(1);
            }

            final RawLogServer rawLogServer = new RawLogServer(rootDir);

            final URI uri = new URI(protocol + "://"  + hostname+ ":" + port);
            if (protocol.equalsIgnoreCase("raw") || protocol.equalsIgnoreCase("tcp")) {
                epFactory = new NettyEndPointFactory(ExecutorService.newScheduledThreadPool(3, "RawLogServer"), "logserver");
                epFactory.start();
                EndPoint endPoint = epFactory.getEndPoint(uri, rawLogServer);
            } else {
                LOGGER.error("Cannot start - Unknown protocol:" + protocol);
                return null;
            }
            LOGGER.info("RUNNING SocketServer:" + uri);

            setupServiceInfo(port, new File(rootDir).getCanonicalPath(), lookupURI, location, rawLogServer, uri);

            rawLogServer.start();



            new ResourceProfile().scheduleOsStatsLogging(scheduler, RawLogServer.class, LOGGER);

            // print out the network info for monitoring purposes
            scheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    //To change body of implemented methods use File | Settings | File Templates.
                    LOGGER.info("socket.server.endpoint.uri=\"" + uri.toString() + "\"");

                }
            }, 1L, 60L, TimeUnit.MINUTES);


            return rawLogServer;
        } catch (Throwable t) {
            LOGGER.warn("Failed to start:" + t, t);
            return null;
        }
    }

    static ProxyFactoryImpl proxyFactory = null;
    static LookupSpace lookupSpace = null;

    static void setupServiceInfo(int port, String rootDir, String lookupURI, String location, final RawLogServer rawLogServer, URI uri) throws URISyntaxException {

        try {
            final ServiceInfo serviceInfo = new ServiceInfo(TAG,uri.toString(), null, null, null, TAG, location, VSOProperties.getResourceType());
            serviceInfo.meta = FileUtil.getPath(new File(rootDir));

            proxyFactory = new ProxyFactoryImpl(port+1, ExecutorService.newDynamicThreadPool("worker",TAG), TAG);
            proxyFactory.start();
            lookupSpace = LookupSpaceImpl.getRemoteService(lookupURI, proxyFactory, "RawLogServer");
            lookupSpace.registerService(serviceInfo, -1);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    LOGGER.info("ShutdownHook called");
                    try {
                        lookupSpace.unregisterService(serviceInfo);
                    } catch (Throwable t) {
                        LOGGER.warn(t);
                    }
                    rawLogServer.stop();
                    proxyFactory.stop();
                    if (transportFactoryImpl != null) transportFactoryImpl.stop();
                }
            });
        } catch (Throwable t) {
            LOGGER.warn("Failed to setup service" , t);
        }
    }

    @Override
    public boolean isForMe(Object payload) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public byte[] receive(Object payload, String remoteAddress,
                          String remoteHostname) {
        // TODO Auto-generated method stub
        return null;
    }
}
