package com.liquidlabs.transport.netty;

import com.liquidlabs.common.collection.Multipool;
import com.liquidlabs.common.net.URI;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/01/2014
 * Time: 14:15
 * To change this template use File | Settings | File Templates.
 */
public class NettyPoolingSenderFactoryJMX implements NettyPoolingSenderFactoryJMXMBean {

    private NettyPoolingSenderFactory factory;

    public NettyPoolingSenderFactoryJMX(NettyPoolingSenderFactory factory) {

        this.factory = factory;

        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("com.liquidlabs.transport.netty:NettyPoolingSenderFactory=NettyPoolingSenderFactory" + System.currentTimeMillis());
            if (mbeanServer.isRegistered(objectName)) return;
            mbeanServer.registerMBean(this, objectName);
        } catch (Exception e) {
//            LOGGER.error(e);
        }
    }

    public String getStats() {
        return factory.dumpStats();
    }

    public String getMPoolStats(String blank) {
        Multipool<URI,NettySenderImpl> pool = factory.getMpool();

        Map<URI,Multipool.Pool<URI,NettySenderImpl>> map = pool.map();

        StringBuilder sb = new StringBuilder();

        int total = 1;

        for (URI url : map.keySet()) {
            Multipool.Pool<URI, NettySenderImpl> mpool = map.get(url);
            int items = mpool.items;
            sb.append("URI:" + url + " ");
            sb.append("Items:" + items + "<br>");
            Collection<NettySenderImpl> values = mpool.values();
            for (NettySenderImpl value : values) {
                sb.append(" ").append(total++ ).append(") ");
                sb.append(value.toString() + " <br> ");
            }
            sb.append("<br><br>");
        }
        return sb.toString();
    }

}
