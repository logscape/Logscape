package com.liquidlabs.admin;

import com.liquidlabs.orm.Id;

public class EmailConfig {
	public static String ID = "EmailConfig";
	@Id
	String id= ID;
	
	public String protocol = System.getProperty("smtp.protocol","smtps");
	public String host = System.getProperty("smtp.host","smtp.gmail.com");
	public int port = Integer.getInteger("smtp.port",465);
	public String username ="";
	public String password ="";

	public EmailConfig() {
	}
	public EmailConfig(String protocol, String host, int port, String username, String password) {
		this.protocol = protocol;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
	}


	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("[EmailConfig:");
		buffer.append(" protocol:");
		buffer.append(protocol);
		buffer.append(" host:");
		buffer.append(host);
		buffer.append(" port:");
		buffer.append(port);
		buffer.append(" username:");
		buffer.append(username);
		buffer.append(" id:");
		buffer.append(id);
		buffer.append("]");
		return buffer.toString();
	}
	

}
