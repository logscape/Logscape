package com.liquidlabs.common;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Log4JLevelChanger {
	private static final String KEY = "-DlogLevel=";
	
	/**
	 * 
	 * Usage -DlogLevel=ResourceSpace:DEBUG
	 * 
	 */
	public static void apply(String[] params) {
		for (String param : params) {
			if (param.startsWith(KEY)) {
				String arg = param.substring(KEY.length(), param.length());
				String[] args = arg.split(":");
				if (args.length == 2){
					setLogLevel(args[0], args[1]);
				}
			}
		}
	}
	
	public static void setLogLevel(String loggerName, String level) {
		if ("debug".equalsIgnoreCase(level)) {
			Logger.getLogger(loggerName).setLevel(Level.DEBUG);
		} else if ("info".equalsIgnoreCase(level)) {
			Logger.getLogger(loggerName).setLevel(Level.INFO);
		} else if ("error".equalsIgnoreCase(level)) {
			Logger.getLogger(loggerName).setLevel(Level.ERROR);
		} else if ("fatal".equalsIgnoreCase(level)) {
			Logger.getLogger(loggerName).setLevel(Level.FATAL);
		} else if ("warn".equalsIgnoreCase(level)) {
			Logger.getLogger(loggerName).setLevel(Level.WARN);
		}
	}
}
