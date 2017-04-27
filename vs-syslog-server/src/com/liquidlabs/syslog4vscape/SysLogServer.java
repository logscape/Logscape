package com.liquidlabs.syslog4vscape;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.syslog4vscape.handler.LLFileSyslogServerEventHandler;
import com.liquidlabs.syslog4vscape.handler.MultiplexerHandler;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.thoughtworks.xstream.XStream;
import org.apache.log4j.Logger;
import org.productivity.java.syslog4j.Syslog;
import org.productivity.java.syslog4j.SyslogConstants;
import org.productivity.java.syslog4j.SyslogIF;
import org.productivity.java.syslog4j.server.*;
import org.productivity.java.syslog4j.server.SyslogServerMain.Options;
import org.productivity.java.syslog4j.server.impl.event.printstream.SystemOutSyslogServerEventHandler;

import java.io.File;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * http://www.syslog4j.org/
 *
 */
public class SysLogServer {
    public static final String TAG = "SysLogServer";
    private final static Logger LOGGER = Logger.getLogger(SysLogServer.class);
    static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {


        log("Starting Server PID:" + PIDGetter.getPID() + " args:" + Arrays.toString(args));

//		SyslogIF createInstance = Syslog.getInstance("tcp");
        try {
            if (args.length != 6) {
                //tcpport, udpport, serverRoot, lookupSpaceAddress, hostname, group,
                String errorMsg = "Expecting 6 arguments i.e. tcp 6161 ../syslogserver (tcpport udpport filename), got:" + args.length;
                System.out.println(errorMsg);
                System.err.println(errorMsg);
                LOGGER.error(errorMsg);
                return;
            }
            final String tcpport = args[0];
            final String udpport = args[1];
            String rootDir = new File(args[2]).getCanonicalPath();


            int portInt = Integer.parseInt(tcpport);
            int foundPort = NetworkUtils.determinePort(portInt);
            if (portInt != foundPort) {
                String errorMsg = "Port-Clash on port:" + portInt + " cannot start SysLogServer";
                System.err.println(errorMsg);
                LOGGER.error(errorMsg);
                System.exit(1);
            }

            LOGGER.info("Starting");

            String lookupSpaceAddress = args[3];
            String hostname = args[4];
            String location = args[5];

            setupServiceInfo(lookupSpaceAddress, hostname, location, tcpport, udpport, rootDir);

            /**
             * TCP Setup port:1468
             */

            String[] argz = new String[] { "-a", "-o", rootDir, "-p", tcpport, "tcp"};
            create(args, SyslogServerMain.parseOptions(argz));
            final SyslogIF tcpSyslog = Syslog.getInstance("tcp");
            tcpSyslog.getConfig().setHost("localhost");
            tcpSyslog.getConfig().setPort(Integer.parseInt(tcpport));
            tcpSyslog.getConfig().setFacility(SyslogConstants.FACILITY_USER);
            tcpSyslog.info("server[0]: TCP This is a USER:INFO test - Startup Test Message");
            tcpSyslog.alert("server[0]: TCP This is a USER:ALERT test - Startup Test Message");


            /**
             * UDP Setup port:514
             */
            String[] argz2 = new String[] { "-a", "-o", rootDir, "-p", udpport, "udp"};
            create(args, SyslogServerMain.parseOptions(argz2));

            final SyslogIF udpSyslog = Syslog.getInstance("udp");
            udpSyslog.getConfig().setHost("localhost");
            udpSyslog.getConfig().setPort(Integer.parseInt(udpport));
            udpSyslog.getConfig().setFacility(SyslogConstants.FACILITY_USER);
            udpSyslog.info("server[0]: UDP This is a test - Startup Message");

            log("Main()");

            new ResourceProfile().scheduleOsStatsLogging(scheduler, SysLogServer.class, LOGGER);

            // print out the network info for monitoring purposes
            scheduler.scheduleAtFixedRate(new Runnable() {
                public void run() {
                    //To change body of implemented methods use File | Settings | File Templates.
                    LOGGER.info("syslog.udp.endpoint.uri=\"" + "udp://" + NetworkUtils.getHostname() + ":" + udpport + "/syslog\"");
                    LOGGER.info("syslog.tcp.endpoint.uri=\"" + "tcp://" + NetworkUtils.getHostname() + ":" + tcpport+ "/syslog\"");


                }
            }, 1L, 60L, TimeUnit.MINUTES);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    LOGGER.info("ShutdownHook called");
                    try {
                        tcpSyslog.shutdown();
                    } catch (Throwable t) {
                        LOGGER.error(t);
                    }
                    try {
                        udpSyslog.shutdown();
                    } catch (Throwable t) {
                        LOGGER.error(t);
                    }

                }
            });

            Thread.sleep(Long.MAX_VALUE);
        } catch (Throwable e1) {
            LOGGER.error(e1.toString(), e1);
        }
    }
    static ProxyFactoryImpl proxyFactory = null;
    static LookupSpace lookupSpace = null;
    public static void setupServiceInfo(String lookupSpaceAddress, String hostname, String location, String tcpport, String udpport, String rootDir) throws URISyntaxException {

        try {
            final ServiceInfo serviceInfo = new ServiceInfo(TAG,  "tcp://" + hostname + ":" + tcpport + "?&UDP=" + udpport ,null, null, "vs-syslog",TAG, location, VSOProperties.getResourceType());
            serviceInfo.meta = FileUtil.getPath(new File(rootDir));

            proxyFactory = new ProxyFactoryImpl(VSOProperties.getBasePort()+65, ExecutorService.newDynamicThreadPool("worker", TAG), TAG);
            proxyFactory.start();
            lookupSpace = LookupSpaceImpl.getRemoteService(lookupSpaceAddress, proxyFactory,"SysLogBoot");
            lookupSpace.registerService(serviceInfo, -1);


            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    LOGGER.info("ShutdownHook called");
                    try {
                        lookupSpace.unregisterService(serviceInfo);
                        proxyFactory.stop();
                    } catch (Throwable t) {
                        LOGGER.error(t);
                    }
                }
            });
        } catch (Throwable t) {
            LOGGER.info("Failed to register service", t);
        }
    }
    public static SyslogServerIF create(String[] args, Options options) throws Exception {

        if (Arrays.toString(args).contains("-quiet=false")) {
            options.quiet = false;
        }

        String xml = new XStream().toXML(options);
        log(xml);
        if (options.usage != null) {
            SyslogServerMain.usage(options.usage);
            System.exit(1);
        }

        if (!options.quiet) {
            log("SyslogServer " + SyslogServer.getVersion());
        }

        if (!SyslogServer.exists(options.protocol)) {
            SyslogServerMain.usage("Protocol \"" + options.protocol + "\" not supported");
            System.exit(1);
        }

        SyslogServerIF syslogServer = SyslogServer.getInstance(options.protocol);

        SyslogServerConfigIF syslogServerConfig = syslogServer.getConfig();

        if (options.host != null) {
            syslogServerConfig.setHost(options.host);
            if (!options.quiet) {
                log("Listening on host: " + options.host);
            }
        }

        if (options.port != null) {
            syslogServerConfig.setPort(Integer.parseInt(options.port));
            if (!options.quiet) {
                log("Listening on port: " + options.port);
            }
        }

        if (options.fileName != null) {
            SyslogServerEventHandlerIF eventHandler = null;
//            if (Boolean.getBoolean("metering.enabled")) {
                eventHandler = new MultiplexerHandler(proxyFactory, lookupSpace, options.fileName);
//            } else {
//                eventHandler = new LLFileSyslogServerEventHandler(options.fileName,options.append);
//            }
            syslogServerConfig.addEventHandler(eventHandler);
            if (!options.quiet) {
                log((options.append ? "Appending" : "Writing") + " to file: " + options.fileName);
            }
        }

//        if (!options.quiet) {
//            SyslogServerEventHandlerIF eventHandler = SystemOutSyslogServerEventHandler.create();
//            syslogServerConfig.addEventHandler(eventHandler);
//        }

        if (!options.quiet) {
            System.out.println();
        }

        return SyslogServer.getThreadedInstance(options.protocol);
    }
    private static void log(String msg) {
        System.out.println(msg);
        LOGGER.info(msg);
    }


}
