package com.liquidlabs.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.eaio.uuid.UUID;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class UID {
	private static DateTimeFormatter forPattern = DateTimeFormat.forPattern("yyyyMMdd_HHmmss");
    private volatile static long count;
	
	private static String hostname = null;
	
	public static String getUUID() {

        //java.rmi.server.UID uid = new java.rmi.server.UID();
        // http://johannburkard.de/software/uuid/
        UUID u = new UUID();
        return u.toString();



//		return new java.rmi.server.UID().toString();
	}
    public static String getUUID2() {
        return System.currentTimeMillis() + "" + count++;
    }
	public static String getUUIDWithTime() {
		return (String.format("LLABS-%s-%s", getUUID(),forPattern.print(DateTimeUtils.currentTimeMillis()))).replaceAll(":", "-").replaceAll("--", "-").replaceAll("_", "-");
	}
	
	public static String getUUIDWithHostNameAndTime() {
		return String.format("LLABS-%s-%s-%s", getUUID() , getHostname(), forPattern.print(DateTimeUtils.currentTimeMillis()));
	}
    public static String getUUID2WithHostNameAndTime() {
        return String.format("LLABS-%s-%s-%s", getUUID2() , getHostname(), forPattern.print(DateTimeUtils.currentTimeMillis()));
    }

    private static synchronized String getHostname() {
		if (hostname == null)
			try {
				hostname = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		return hostname;
	}
	public static String getSimpleUID(String context) {
		String pid = PIDGetter.getPID();
		String result = "";
		if (!context.contains(pid)) result += pid;
		return result;
	}


    static  InetAddress addr;
    static byte[] ipaddr;

    static {
        try {
            addr = InetAddress.getLocalHost();
            ipaddr = addr.getAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


    }
}
