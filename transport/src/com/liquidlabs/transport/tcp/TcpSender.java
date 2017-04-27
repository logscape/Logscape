package com.liquidlabs.transport.tcp;


import static com.liquidlabs.transport.protocol.NetworkConfig.HEADER_BYTES;

import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import com.liquidlabs.common.BinConvertor;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.protocol.Type;

public class TcpSender implements Sender {
	
	static final Logger LOGGER = Logger.getLogger(TcpSender.class);
	
	private Map<URI, Socket> sendSockets = new ConcurrentHashMap<URI, Socket>();
	boolean running = false;

	private URI startPoint;

	public TcpSender(URI address) {
	}

	/**
	 * Send stuff
	 * TODO: there is a bug in Broken Pipe handling in that it can miss the first msg sent after finding a broken pipe - see the {@link SwarmReconnectionTest}
	 */
	public byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowlocalRoute) throws InterruptedException {
		this.startPoint = endPoint;
		if (!running) throw new RuntimeException("Status - running == false");
		
		Socket socket2 = null;
		try {
			if (!sendSockets.keySet().contains(endPoint)) {
				socket2 = addSendSocket(endPoint);
			} else {
				socket2 = sendSockets.get(endPoint);
				if (socket2.isClosed()) {
					socket2 = addSendSocket(endPoint);
				}
				if (!socket2.isConnected()) {
					socket2 = addSendSocket(endPoint);
				}
				socket2.isBound();
			}
			OutputStream os = socket2.getOutputStream();
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream(HEADER_BYTES.length + 4 + bytes.length);
			baos.write(HEADER_BYTES);
			baos.write(BinConvertor.intToByteArray(type.ordinal(), new byte[4], 0));
			baos.write(BinConvertor.intToByteArray(bytes.length, new byte[4], 0));
			baos.write(bytes);
			
			os.write(baos.toByteArray());
			os.flush();
			return new byte[0];
			
		} catch (SocketException sEx) {
			if (sEx.getMessage().contains("Broken pipe")) {
				try {
					System.err.println("Reconnecting to " + endPoint);
					this.sendSockets.remove(endPoint);
					this.send(protocol, endPoint, bytes, type, isReplyExpected, timeoutSeconds, info, allowlocalRoute);
				} catch (Throwable t) {
					handleException(endPoint, bytes, t);
				}
			} else {
				handleException(endPoint, bytes, sEx);				
			}
		} catch (Throwable t) {
			handleException(endPoint, bytes, t);
		}
		return new byte[0];
	}

	private void handleException(URI endPoint, byte[] bytes, Throwable t) {
		System.err.println("Removing EndPoint:" + endPoint);
		sendSockets.remove(endPoint);
		throw new RuntimeException("Failed to send to:" + endPoint + " byteCount:" + bytes.length, t);
	}

	private Socket addSendSocket(URI endPoint) {
		try {
			System.err.println("Adding EndPoint:" + endPoint);
			Socket socket = new Socket(endPoint.getHost(), endPoint.getPort());
			sendSockets.put(endPoint, socket);
			return socket;
		} catch (Throwable e) {
			throw new RuntimeException("Could not connect to:" + endPoint, e);
		}		
	}
	
	public void start() {
		running = true;
	}

	public void stop() {
		running = false;
		Collection<Socket> values = sendSockets.values();
		for (Socket socket : values) {
			try {
				socket.close();
			} catch (Exception e) {
				LOGGER.error(e);
			}
		}
		sendSockets.clear();
	}

	public URI getAddress() {
		return startPoint;
	}

	public Map<URI, Socket> getSendSockets() {
		return sendSockets;
	}

	public void dumpStats() {
	}
}
