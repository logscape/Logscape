package com.liquidlabs.syslog4vscape.handler;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.rawlogserver.handler.*;
import com.liquidlabs.rawlogserver.handler.fileQueue.SAASFileQueuerFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.logscape.meter.MeterServiceImpl;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.productivity.java.syslog4j.server.SyslogServerEventIF;
import org.productivity.java.syslog4j.server.SyslogServerIF;
import org.productivity.java.syslog4j.server.SyslogServerSessionEventHandlerIF;
import org.productivity.java.syslog4j.util.SyslogUtility;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/10/2014
 * Time: 09:48
 * To change this template use File | Settings | File Templates.
 */
public class MultiplexerHandler  implements SyslogServerSessionEventHandlerIF {
    private final static Logger LOGGER = Logger.getLogger(MultiplexerHandler.class);
    private SAASFileQueuerFactory saasFileQueuerFactory;

    private StreamHandler handler;
    private ProxyFactory pf;
    private String rootDir;
    private boolean isEnriching = System.getProperty("syslog.enrich","true").equals("true");


    public MultiplexerHandler(ProxyFactory pf, LookupSpace luSpace, String rootDir) {
        this.pf = pf;
        this.rootDir = rootDir;
        saasFileQueuerFactory = new SAASFileQueuerFactory(MeterServiceImpl.getRemoteService("mplex", luSpace, pf, true));

        ContentFilteringLoggingHandler contentWriter = new ContentFilteringLoggingHandler(saasFileQueuerFactory);
        StandardLoggingHandler logWriter = new StandardLoggingHandler(pf.getScheduler());
        logWriter.setTimeStampingEnabled(false);
        this.handler = new PerAddressHandler(contentWriter);
    }

    public MultiplexerHandler() {

    }


    @Override
    public void event(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, SyslogServerEventIF event) {
        final String hostname = event.getHost();
        final String address = ((InetSocketAddress) socketAddress).getAddress().getHostAddress();
        String facility = getFacility(event.getRaw(), event.getLevel());

        String msg = event.getMessage();
        if (event.isHostStrippedFromMessage()) {
            msg = event.getHost() + " " + msg;
        }
         // no 2015 && no Jan/Feb etc - then no-timestamp found
        if (!msg.contains(Integer.toString(new DateTime().getYear())) && !startsWithMonth(msg)) {
            if (event.getDate() == null) event.setDate(new Date());
            String time =  DateUtil.shortDateTimeFormat8.print(event.getDate().getTime());
            msg = time + " " + msg;
        }

       if (isEnriching) {
           String levelString = SyslogUtility.getLevelString(event.getLevel());
           if (!levelString.equals("UNKNOWN")) {
               msg += " level:" + levelString;
           }
       }

        handler.handled(msg.getBytes(), address, hostname, "_syslog_/_" + facility + "_");
    }

    static String[] monthString = new String[] { "JAN","FEB","MAR", "APR", "MAY", "JUN","JUL","AUG","SEP","OCT","NOV","DEC" };

    private boolean startsWithMonth(String msg) {
        msg = msg.toUpperCase();
        for (String month : monthString) {
            if (msg.startsWith(month)) return true;
        }
        return false;
    }

    String getFacility(byte[] raw, int level) {
        String source = new String(raw);

        try {
            if (source.contains(">")) {
                String from = source.substring(1, source.indexOf('>'));
                Integer priority = Integer.parseInt(from);

                try {
                    int severity = priority & 7;//   # 7 is 111 (3 bits)
                    int facility = priority >> 3;
                    return facilities[facility];
                } catch (Throwable t) {
                    return "UNKNOWN";
                }
            }
        } catch (Throwable t) {

        }
        return "UNKNOWN";
    }
    String[] facilities = ("kernel,user,mail,system,security,syslog,lpd,nntp,uucp,time,security,ftpd,ntpd,logaudit,logalert,clock,local0,local1,local2,local3,"+
                "local4,local5,local6,local7").toUpperCase().split(",");

    @Override
    public Object sessionOpened(SyslogServerIF syslogServer, SocketAddress socketAddress) {
        LOGGER.info("NewSession:" + socketAddress);
        return null;
    }


    @Override
    public void exception(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, Exception exception) {
        LOGGER.error("Failed", exception);
    }

    @Override
    public void sessionClosed(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, boolean timeout) {
        LOGGER.info("SessionClosed:" + socketAddress);
    }

    @Override
    public void initialize(SyslogServerIF syslogServer) {
        //To change body of implemented methods use File | Settings | File Templates.
        LOGGER.info("initialize:" + syslogServer);
    }

    @Override
    public void destroy(SyslogServerIF syslogServer) {
        LOGGER.info("destroy:");
    }
}
