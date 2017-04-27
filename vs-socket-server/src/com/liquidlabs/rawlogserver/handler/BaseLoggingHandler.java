package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.DateUtil;
import org.joda.time.DateTimeUtils;

public class BaseLoggingHandler {
	protected static String eol = System.getProperty("line.separator");

	protected static final String SLASH = "/";
	protected static final String SPACE = " ";
	protected static final String FORMAT_SS_LOG = "%s/%s-%s.log";
	protected static final String FORMAT_SS = "%s/%s";
	protected boolean isTimeStampingEnabled = Boolean.parseBoolean(System.getProperty("socket.server.timestamp", "true"));
	
	protected String tag = "raw";
	
	protected String getNowTimeSting() {
		return DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis());
	}
	protected String getHostPath(String remoteHostname, String remoteAddress) {
        if (remoteHostname != null) {
            if (remoteHostname.equals(remoteAddress)) return remoteHostname;
            return remoteHostname + "_" +  remoteAddress;
        } else {
            return remoteAddress;
        }
	}
	public void setTimeStampingEnabled(boolean isTimeStampingEnabled) {
		this.isTimeStampingEnabled = isTimeStampingEnabled;
	}



}
