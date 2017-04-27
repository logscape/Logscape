package com.liquidlabs.transport;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.log4j.Logger;

public class Config {
	private final static Logger LOGGER = Logger.getLogger(Config.class);
	public static String NETWORK_SPLIT = "&";
	public static String NETWORK_SPLIT_2 = "?";
	
	static {
		initialise();
	}
	public static void initialise() {
		String property = System.getProperty("file.encoding","ISO-8859-1");
		try {
			DEFAULT_CHARSET = Charset.forName(property);
			delimiter = Integer.getInteger("vso.delimiter", 181);
			System.out.println("Loading file.encoding:" + property + " char:" + delimiter);
			OBJECT_DELIM = new String(new byte[] { (byte) ((char)delimiter) }, property);
		} catch (UnsupportedEncodingException e) {
			LOGGER.error(e);
			LOGGER.error("Failed to load encoding:" + property +  " Falling back to ISO-8859-1");
			OBJECT_DELIM = new String(new byte[] { (byte) ((char)delimiter) }, Charset.forName("ISO-8859-1"));
		}		
	}
	
	public static Charset DEFAULT_CHARSET = getISOCharSet(); 
	
	
	public static int delimiter = Integer.getInteger("vso.delimiter", 181);
	// used by Space, Query and LeaseManager
	public static String OBJECT_DELIM = new String(new byte[] { (byte) ((char) delimiter) }, DEFAULT_CHARSET );
	public static String ARG_SPLIT = "&";
	
	
	public static int TEST_PORT = 22000;


	public static Charset getISOCharSet() {
		return Charset.forName(System.getProperty("file.encoding"));
	}


	public static String revertEscapes(String xmlConfig) {
		return xmlConfig.replaceAll("-LSCAPE-", Config.OBJECT_DELIM);
	}


	public static String escapeDelims(String xml) {
		return xml.replaceAll(Config.OBJECT_DELIM, "-LSCAPE-");
	}
	
}
