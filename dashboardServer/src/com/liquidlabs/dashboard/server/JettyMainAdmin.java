package com.liquidlabs.dashboard.server;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.ObjectName;


import com.liquidlabs.common.ThreadUtil;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.services.ServicesLookup;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;

public class JettyMainAdmin implements JettyMainAdminMBean {

    private static final Logger LOGGER = Logger.getLogger(JettyMainAdmin.class);

    private final Server server;

    public JettyMainAdmin(Server server) {
        this.server = server;
        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("com.liquidlabs.vscape.agent.Jetty:type=Admin");
            if (mbeanServer.isRegistered(objectName)) return;
            mbeanServer.registerMBean(this, objectName);
        } catch (Exception e) {
        }

    }
    public String dumpThreads() {
        return ThreadUtil.threadDump(null,"").replaceAll("\n", "<br>\n");
    }

    public String displaySystemProperties() {
        StringBuilder builder = new StringBuilder();
        Properties properties = System.getProperties();
        for (Object o : properties.keySet()){
            builder.append(o).append("=").append(properties.get(o)).append("<br><br>");
        }
        return builder.toString();
    }
    public String reindex(String tags) {

        LOGGER.info("reindex(" + tags + ")");
        String results = "";

        LogSpace logSpace = ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD).getLogSpace();
        List<WatchDirectory> watchDirectories = logSpace.watchDirectories(null, "", false);
        for (WatchDirectory dataSource : watchDirectories) {
            if (dataSource.getTags().contains(tags))  {
                results += dataSource.toString() + "\n";
                logSpace.reindexWatch(dataSource.id());
            }
        }
        LOGGER.info("results:" + results);
        return results;
    }
}
