package com.logscape.disco.indexer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 10/01/15
 * Time: 16:20
 * To change this template use File | Settings | File Templates.
 */
public class KvIndexAdmin implements KvIndexAdminMBean {


    public KvIndexAdmin(){
        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("com.logscape:Service=KvIndexAdmin");
            if (!mbeanServer.isRegistered(objectName)) {
                mbeanServer.registerMBean(this, objectName);
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
    public String setProperty(String key, String value) {
        System.setProperty(key, value);
        return "DONE > System.setProperty(" + key + "," + value + ");";
    }
}
