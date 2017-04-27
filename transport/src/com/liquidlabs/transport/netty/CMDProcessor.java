/**
 * 
 */
package com.liquidlabs.transport.netty;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

import java.net.Socket;
import java.util.Date;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.protocol.Type;

/**
 * Handles text commands
 * 
 * @author Neil
 * 
 */
public class CMDProcessor {
	private static final Logger LOGGER = Logger.getLogger(CMDProcessor.class);
	private final String ipaddress;
	private final URI serverEndPoint;

	public CMDProcessor(URI serverEndPoint, Channel channel, String ipaddress) {
		this.serverEndPoint = serverEndPoint;
		this.ipaddress = ipaddress;
	}

	/**
	 * @param string
	 *            i.e. CMD: PINGBACK 15000
	 */
	public void handle(String cmdString) {
		try {
			LOGGER.info("Received:" + cmdString + " From:" + ipaddress);

			// now grab the port
			String[] cmdargs = cmdString.split(" ");
			Integer port = Integer.parseInt(cmdargs[2]);
			Socket socket = new Socket(ipaddress.split("_")[0], port);
			socket.getOutputStream().write((serverEndPoint + " 1 Write from client:" + new Date() + " to:" + ipaddress + "\n").getBytes());
			Thread.sleep(1000);
			socket.getOutputStream().write((serverEndPoint + " 2 Write from client:" + new Date() + " to:" + ipaddress + "\n").getBytes());
			Thread.sleep(1000);
			socket.getOutputStream().write((serverEndPoint + " 3 Write from client:" + new Date() + " to:" + ipaddress + "\n").getBytes());
			Thread.sleep(1000);
			socket.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}