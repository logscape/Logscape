package com.liquidlabs.dashboard.server;

import com.liquidlabs.services.ServicesLookup;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Point your browser to
 * Make sure the
 * 1. Working Directory is Logscape/dashboardServer
   2. Browser http://localhost:8100/dashboard/dashboard.html#
 *
 */
public class JettyMainIDE {

    private static final Logger LOGGER = Logger.getLogger(JettyMainIDE.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Args:" + Arrays.toString(args));
        boolean isIDE = true;
        //System.setProperty("vso.lookup.host","10.28.1.150");


        try {
            LOGGER.info("Starting Jetty WD:" + new File(".").getAbsolutePath());
            System.setProperty("dashboard.vscape.port","11100");
            WebAppContext context = new WebAppContext();
            context.setResourceBase("webapp");
            context.setDescriptor("webapp/WEB-INF");
            context.setContextPath("/dashboard");
            context.setParentLoaderPriority(true);

            ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD).init(context);
            Server server = new Server(8100);

            server.setHandler(context);
            server.start();
            LOGGER.info("Started");
            Thread.sleep(Long.MAX_VALUE);
        } catch (Throwable t) {
            t.printStackTrace();
        }


    }

	private static List<Connector> getConnector(String[] args) {
        List<Connector> results = new ArrayList<Connector>();

        // only add SSL when we are deployed properly
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
                    SslSocketConnector connector = new SslSocketConnector();
                    connector.setPort(Integer.parseInt(properties.getProperty("port")));
                    LOGGER.info("SSL Using PORT:" + properties.getProperty("port"));
                    connector.setMaxIdleTime(Integer.parseInt(properties.getProperty("maxIdleTime")));
                    connector.setKeystore(properties.getProperty("keystore"));
                    connector.setPassword(properties.getProperty("password"));
                    connector.setKeyPassword(properties.getProperty("keyPassword"));
                    connector.setTruststore(properties.getProperty("truststore"));
                    connector.setTrustPassword(properties.getProperty("trustPassword"));
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
        connector.setPort(Integer.valueOf(args[0]));
        connector.setHost("0.0.0.0");
        results.add(connector);
        return results;
    }

}
