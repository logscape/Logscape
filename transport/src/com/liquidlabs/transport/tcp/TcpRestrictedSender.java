package com.liquidlabs.transport.tcp;


import static com.liquidlabs.transport.protocol.NetworkConfig.HEADER_BYTES;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import com.liquidlabs.common.BinConvertor;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Sender;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.protocol.Type;

public class TcpRestrictedSender implements Sender {
	
	static final Logger LOGGER = Logger.getLogger(TcpRestrictedSender.class);
	
	Socket  socket;
	boolean running = false;

	private URI startPoint;

	private final java.util.concurrent.ExecutorService executor;

	public TcpRestrictedSender(URI address, java.util.concurrent.ExecutorService executor) {
		this.executor = executor;
	}

	/**
	 * Send stuff
	 * TODO: there is a bug in Broken Pipe handling in that it can miss the first msg sent after finding a broken pipe - see the {@link SwarmReconnectionTest}
	 */
	public byte[] send(String protocol, URI endPoint, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String info, boolean allowlocalRoute) throws InterruptedException {
		this.startPoint = endPoint;
		if (!running) throw new RuntimeException("Status - running == false");
		
		try {
			connect(endPoint);

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream(HEADER_BYTES.length + 4 + bytes.length);
			baos.write(HEADER_BYTES);
			baos.write(BinConvertor.intToByteArray(type.ordinal(), new byte[4], 0));
			baos.write(BinConvertor.intToByteArray(bytes.length, new byte[4], 0));
			baos.write(bytes);
			
			os.write(baos.toByteArray());
			os.flush();
			if (isReplyExpected) return getReply(is);
			return new byte[0];
			
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
	private byte[] getReply(final InputStream is) {
		try {
			Callable<byte[]> task = new Callable<byte[]>(){
				public byte[] call() throws Exception {
					int available = is.available();
					while (available == 0) {
						available = is.available();
						Thread.sleep(100);
						//Thread.yield();
					}
					System.out.println("A:" + available);
					byte[] result = new byte[available];
					int read = is.read(result);
					System.out.println("READING:" + read);
					return result;
				}
			};
			Future<byte[]> submit = executor.submit(task);
			return submit.get(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			LOGGER.warn("Task Error:" + e.getMessage());
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		return null;
	}

	private void connect(URI endPoint) {
		if (socket != null) return;
		boolean connected = false;
		int attempts = 0;
		int increment = 1;
		Socket socket2 = null;
		InetSocketAddress localAddress = null;
		while (!connected && attempts < TransportProperties.getClientMaxPorts()*3) {
			int portBeingUsed = TransportProperties.getClientBasePort();
			try {
				localAddress = new InetSocketAddress(portBeingUsed);
        		TransportProperties.updateBasePort(localAddress.getPort()+increment);
        		socket2 = new Socket(endPoint.getHost(), endPoint.getPort(), localAddress.getAddress(), portBeingUsed);
        		socket = socket2;
        		connected = true;
        		LOGGER.warn(attempts + " XXXXXXXXXXX GOOD connection again:" + portBeingUsed + " -> " + endPoint);

			} catch (BindException t){
        		LOGGER.warn(attempts + " XXXXXXXXXXX 1 BINDEX connect retry:" + portBeingUsed + " ->" + endPoint, t);
        		attempts++;
			} catch (Throwable t){
				attempts++;
				throw new RuntimeException(t.getMessage(), t);
//				t.printStackTrace();
//        		LOGGER.warn(attempts + " XXXXXXXXXXX 1 BAD connection again:" + portBeingUsed + " ->" + endPoint, t);
				
			}
		}
	}

	public void start() {
		running = true;
	}

	public void stop() {
		running = false;
		try {
			if (socket != null) socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public URI getAddress() {
		return startPoint;
	}

	public void dumpStats() {
	}
}
