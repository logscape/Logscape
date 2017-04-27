package com.liquidlabs.transport.netty.handshake;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.file.FileUtil;
import jregex.Pattern;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 13/05/2014
 * Time: 09:56
 * To change this template use File | Settings | File Templates.
 */
public class HostIpFilter {
    private static final Logger LOGGER = Logger.getLogger(HostIpFilter.class);
    public static final String HOSTS_FILE = System.getProperty("endpoint.hosts.file","hosts");
    List<jregex.Matcher> filters = Arrays.asList(new Pattern( (System.getProperty("host.filter", "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+")).trim()).matcher());
    Set<String> ignoredIps = new HashSet<String>();


    public HostIpFilter(){
        String ipAddress = NetworkUtils.getIPAddress();
        ignoredIps.add(ipAddress);
        ignoredIps.add("127.0.0.1");
        ignoredIps.add("localhost");


        // load the contents from the FS
        if (new java.io.File(HOSTS_FILE).exists()) {
            String hosts = FileUtil.readAsString(HOSTS_FILE);
            String[] split = hosts.split("\n");
            List<jregex.Matcher> filters = new ArrayList<jregex.Matcher>();
            for (String pattern : split) {
                try {
                    filters.add(new Pattern(pattern.trim()).matcher());
                } catch (Throwable t) {
                }
            }
            this.filters = filters;
        }
    }
    public boolean isValid(SocketAddress remoteAddress) {
        String hostAddress = ((InetSocketAddress) remoteAddress).getAddress().getHostAddress();
        if (ignoredIps.contains(hostAddress)) return true;
        for (jregex.Matcher filter : filters) {
            if (filter.matches(hostAddress)) return true;
        }

        hostAddress = ((InetSocketAddress) remoteAddress).getHostName();
        if (ignoredIps.contains(hostAddress)) return true;

        for (jregex.Matcher filter : filters) {
            if (filter.matches(hostAddress)) return true;
        }


        LOGGER.warn("Rejecting Connection from:" + remoteAddress);

        return false;
    }
}
