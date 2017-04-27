package com.liquidlabs.dashboard.server;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.services.AutoDeployer;
import com.liquidlabs.services.ServicesLookup;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import org.apache.log4j.Logger;

import org.eclipse.jetty.deploy.App;
import org.eclipse.jetty.deploy.DeploymentManager;
import org.eclipse.jetty.deploy.bindings.DebugBinding;
import org.eclipse.jetty.deploy.bindings.GlobalWebappConfigBinding;
import org.eclipse.jetty.deploy.providers.WebAppProvider;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.session.SessionHandler;

import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.webapp.WebInfConfiguration;
import org.eclipse.jetty.websocket.WebSocketServlet;


import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import org.joda.time.DateTime;

/**
 * Notes
 * o - to Run in STUB Mode use: -Dtest.mode=true - it will provide the JettyService
 * o - to Run from the DashboardServer directory against port 8100 - i.e. debugging in Eclipse: pass
 * System Property -Ddebug.ide=true
 * - main will see the following:
 * - Program Args:8100 build/dashboard.war
 * - JVM Params	:-Dwar.temp.dir=.
 *
 * @author neil
 */
public class JettyMain {

    private static final Logger LOGGER = Logger.getLogger(JettyMain.class);

    static ServicesLookup servicesLookup = null;

    public static void main(String[] args) throws Exception {

        LOGGER.info("Args:" + Arrays.toString(args));

        if (args.length == 0) {
            System.err.println("Usage > JettyMain port warfile proxied");
            System.exit(1);
        }

        System.setProperty(DashboardProperties.PROXY_TAILERS, args[2]);
        LOGGER.info("ManagerAddress:" + VSOProperties.getLookupAddress());
        try {
            int jmxPort = Integer.parseInt(args[0]) + Integer.getInteger("jetty.jmx.port.offset", 1000);
            LOGGER.info("Dashboard/HttpAccess JMX Management Port:" + jmxPort);
            JmxHtmlServerImpl jmxServer = new JmxHtmlServerImpl(jmxPort, true);
            jmxServer.start();


            System.setProperty("org.mortbay.log.LogFactory.noDiscovery", "false");

            Server server = new Server();
            List<Connector> connectors = getConnector(args);

            for (Connector connector : connectors) {
                server.addConnector(connector);
            }
            Connector[] allConnectors = server.getConnectors();
            for (Connector connector : allConnectors) {
                connector.setRequestHeaderSize(connector.getRequestHeaderSize() * 4);
                connector.setRequestBufferSize(connector.getRequestBufferSize() * 4);
            }

            System.setProperty(WebAppContext.BASETEMPDIR, "work/webapps");
            String webAppDeployFROM = "downloads";

            blatWars(webAppDeployFROM);

            ContextHandlerCollection contextHandlerCollection = new ContextHandlerCollection();
            final DeploymentManager deploymentManager = new DeploymentManager();
            deploymentManager.setContexts(contextHandlerCollection);

            WebAppProvider provider = new WebAppProvider();
            provider.setExtractWars(true);
            provider.setScanInterval(0);
            provider.setParentLoaderPriority(true);
            File directory = new File("work/webapps/temp");
            directory.mkdirs();
            provider.setTempDir(directory);
            provider.setMonitoredDirName(webAppDeployFROM);
            deploymentManager.addAppProvider(provider);
//            deploymentManager.insertLifeCycleNode("deployed", "starting", "customise");
            deploymentManager.addLifeCycleBinding(new MyBinding());
            server.addBean(deploymentManager);

            HandlerCollection hc = new HandlerCollection();
            hc.setHandlers(new Handler[]{contextHandlerCollection});
            server.setHandler(hc);
            JettyMainAdmin admin = new JettyMainAdmin(server);

            // Jetty JMX Support
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
            server.getContainer().addEventListener(mBeanContainer);
            mBeanContainer.start();

            server.setStopAtShutdown(true);

            Thread doStuff = new Thread("LoadServices") {
                @Override
                public void run() {
                    try {
                        servicesLookup = ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD);
                        ScheduledExecutorService scheduler = servicesLookup.getProxyFactory().getScheduler();
                        new ResourceProfile().scheduleOsStatsLogging(scheduler, JettyMain.class, LOGGER);
                        servicesLookup.init(deploymentManager);
                        AutoDeployer autoDeployer = new AutoDeployer(scheduler, servicesLookup.getDeploymentService(), servicesLookup.getLogSpace());
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            };
            doStuff.run();

            LOGGER.info("Starting WebServer");

            server.start();


            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    try {
                        System.out.println("JettyMain shuting down");
                        servicesLookup.shutdown();
                    } catch (Exception e) {
                    }
                }
            });
//            App dashboard = deploymentManager.getAppByOriginId(deployedDashboard.getAbsolutePath());
//            while(dashboard == null) {
//                Thread.sleep(1000);
//                dashboard = deploymentManager.getAppByOriginId(deployedDashboard.getAbsolutePath());
//            }
//            WebAppContext wac  = (WebAppContext) dashboard.getContextHandler();
//            wac.setAttribute("apps-dir", WebInfConfiguration.getCanonicalNameForWebAppTmpDir(wac) + "/logscape-apps");
//            SessionHandler sessionHandler = wac.getSessionHandler();
//            SessionManager sessionManager = sessionHandler.getSessionManager();
//
//            Integer sessionTimeout = Integer.getInteger("session.timeout.mins", 10);
////            sessionManager.setMaxCookieAge(sessionTimeout * 60);
//            sessionManager.setMaxInactiveInterval(sessionTimeout * 60);
//            sessionHandler.addEventListener(new HttpSessionListener(){
//                public void sessionCreated(HttpSessionEvent arg0) {
//                    LOGGER.info("SessionCREATED:" + arg0);
//                }
//                public void sessionDestroyed(HttpSessionEvent arg0) {
//                    LOGGER.info("SessionDESTROYED:" + arg0);
//                }
//            });




            Object object = new Object();
            synchronized (object) {
                object.wait();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            LOGGER.error("Failed to start Jetty:" + t.getMessage(), t);

        }
    }

    private static void blatWars(String webAppDeployFROM) {
        final int today = new DateTime().getDayOfYear();
        File[] modifiedTodayWars = new File(webAppDeployFROM).listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".war") && new DateTime(pathname.lastModified()).getDayOfYear() == today;
            }
        });
        File file = new File("./work");
        if (modifiedTodayWars.length > 0 && file.exists()) {
            File[] warsToDeleter = file.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().contains("jetty-") && pathname.getName().contains(".war");
                }
            });
            if (warsToDeleter != null) {
                for (File file1 : warsToDeleter) {
                    int deleted = FileUtil.deleteDir(file1);
                    LOGGER.warn("Deleting WAR folder:" + file1.getAbsolutePath() + " Deleted:" + deleted);
                }
            }


        }
    }

    private static List<Connector> getConnector(String[] args) {
        List<Connector> results = new ArrayList<Connector>();

        // only add SSL when we are deployed properly
        String hostname = NetworkUtils.getHostname();
        if (!((String) System.getProperty("war.temp.dir", "xx")).equals(".")) {
            //System.setProperty("javax.net.debug","all");
            Properties properties = new Properties();
            FileInputStream inStream = null;
            try {
                if (new File("ssl.properties").exists()) {
                    inStream = new FileInputStream("ssl.properties");
                } else if (new File("ssl/ssl.properties").exists()) {
                    inStream = new FileInputStream("ssl/ssl.properties");
                }
                if (inStream != null) {
                    properties.load(inStream);

                    SslSelectChannelConnector connector = new SslSelectChannelConnector();
                    String sslPort = properties.getProperty("port");
                    DashboardProperties.setHttpsPort(sslPort);
                    DashboardProperties.setHttpsUrl("https://"+ hostname + ":" + sslPort);
                    connector.setPort(Integer.parseInt(sslPort));
                    LOGGER.info("SSL Using PORT:" + sslPort);
                    connector.setMaxIdleTime(Integer.parseInt(properties.getProperty("maxIdleTime")));
                    SslContextFactory sslContextFactory = connector.getSslContextFactory();

                    connector.setKeystore(properties.getProperty("keystore"));
//                    sslContextFactory.setKeyStorePassword(properties.getProperty("password"));
                    connector.setPassword(properties.getProperty("password"));

//                    sslContextFactory.setKeyManagerPassword();
                    connector.setKeyPassword(properties.getProperty("keyPassword"));

//                    sslContextFactory.setTrustStore(properties.getProperty("truststore"));
                    connector.setTruststore(properties.getProperty("truststore"));

//                    sslContextFactory.setTrustStorePassword(properties.getProperty("trustPassword"));
                    connector.setTrustPassword(properties.getProperty("trustPassword"));
                    if (properties.getProperty("certAlias") != null) {
                        sslContextFactory.setCertAlias(properties.getProperty("certAlias"));
                    }


                    results.add(connector);
                } else {
                    LOGGER.warn("Cannot LOAD ssl.properties file - SSL disabled");
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failed to load SSL", e);
            } finally {
                if (inStream != null)
                    try {
                        inStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }
        }

        SelectChannelConnector connector = new SelectChannelConnector();
        DashboardProperties.setHttpPort(args[0]);
        DashboardProperties.setHttpUrl("http://"+hostname+ ":"+args[0]);
        connector.setPort(Integer.valueOf(args[0]));
        connector.setHost("0.0.0.0");
        results.add(connector);
        return results;
    }

}
