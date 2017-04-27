package com.liquidlabs.common.jmx;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 05/02/2015
 * Time: 14:33
 * To change this template use File | Settings | File Templates.
 */
public class MBeanServerInfoJMX implements MBeanServerInfoJMXMBean {

    public MBeanServerInfoJMX(){
        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("com.logscape:Service=MBeanServerInfo");
            if (mbeanServer.isRegistered(objectName)) return;
            mbeanServer.registerMBean(this, objectName);
            System.out.println("MBeanServerPort:" + System.getProperty("com.sun.management.jmxremote.port", "0"));
        } catch (Exception e) {

        }
    }
    @Override
    public int getMBeanPort() {
        return Integer.parseInt(System.getProperty("com.sun.management.jmxremote.port", "0"));
    }
}
