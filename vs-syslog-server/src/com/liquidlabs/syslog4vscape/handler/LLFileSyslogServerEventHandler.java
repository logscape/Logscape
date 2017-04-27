package com.liquidlabs.syslog4vscape.handler;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.productivity.java.syslog4j.server.SyslogServerEventIF;
import org.productivity.java.syslog4j.server.SyslogServerIF;
import org.productivity.java.syslog4j.server.SyslogServerSessionEventHandlerIF;
import org.productivity.java.syslog4j.util.SyslogUtility;

import java.io.*;
import java.net.SocketAddress;

public class LLFileSyslogServerEventHandler implements SyslogServerSessionEventHandlerIF {
	private final static Logger LOGGER = Logger.getLogger(LLFileSyslogServerEventHandler.class);
	//Aug 19 10:04:28
	static String shortFormat = "MMM dd HH:mm:ss";
	static String shortDateFormat = "yyMMMdd";
	public static DateTimeFormatter shortDateTimeFormatter = DateTimeFormat.forPattern(shortFormat);
	public static DateTimeFormatter shortDateFormatter = DateTimeFormat.forPattern(shortDateFormat);

	private static final long serialVersionUID = 1L;

	public LLFileSyslogServerEventHandler(String rootDir, boolean append) {
		this.rootDir = rootDir;
        new File(rootDir).mkdir();
	}
	private String rootDir;

    public void event(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, SyslogServerEventIF event) {
//        System.out.println("MSGGGG:" + event);

        final DateTime date = (event.getDate() == null ? new DateTime() : new DateTime(event.getDate()));
		final String dateString = shortDateTimeFormatter.print(date.getMillis());
		final String facility = SyslogUtility.getFacilityString(event.getFacility()<<3);
		final String level = SyslogUtility.getLevelString(event.getLevel());
		final String hostname = event.getHost();
		
		// TODO: could make this use Q's for each facility
		synchronized (this) {
			try {
				PrintStream stream = createPrintStream(getCurrentFilename(hostname, date, facility));
//				stream.println(String.format("%s %s %s %s", facility, dateString, level, event.getMessage().replaceAll("\n", "\n ")));
				//Aug 19 10:04:28 db1 sshd(pam_unix)[15979]: session opened for user root by (uid=0)
                String message = event.getMessage();
                if (!message.endsWith("\n")) message += "\n";
                stream.println(dateString + " " + message);
				stream.flush();
				stream.close();
			} catch (Exception e) {
				e.printStackTrace();
				LOGGER.error("Event() Error:" + e.toString(), e);
			}
		}
	}

	protected PrintStream createPrintStream(String filename) throws IOException {
		File file2 = new File(this.rootDir, filename);
		file2.getParentFile().mkdirs();
		OutputStream os = new BufferedOutputStream(new FileOutputStream(file2, true));
		return new PrintStream(os);
	}

	private String getCurrentFilename(String hostname, DateTime dateTime, String facility) {
		return String.format("%s/%s/%s.log", hostname,shortDateFormatter.print(dateTime),facility);
	}
	
	public void destroy(SyslogServerIF server) {
	}
	public void initialize(SyslogServerIF server) {
		System.out.println("");
	}

    public Object sessionOpened(SyslogServerIF syslogServer, SocketAddress socketAddress) {
        LOGGER.info("Client:SysLog Action:ConnectionEstablished Address:" + socketAddress);
        return null;
    }

    public void exception(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, Exception exception) {
    }

    public void sessionClosed(Object session, SyslogServerIF syslogServer, SocketAddress socketAddress, boolean timeout) {
        LOGGER.info("Client:SysLog Action:ConnectionClosed Address:" + socketAddress);
    }
}
