package com.liquidlabs.transport.tcp;

import static com.liquidlabs.transport.protocol.NetworkConfig.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import com.liquidlabs.common.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.liquidlabs.common.BinConvertor;
import com.liquidlabs.transport.Receiver;

/**
 * Mega basic tcpServer - need to use NIO to reduce thread count
 */
public class TcpReceiver implements Receiver {
	static final Logger LOGGER = Logger.getLogger(TcpReceiver.class);

	private final Receiver receiver;
	int port = 0;
	private ServerSocket serverSocket;
	private boolean running = false;
	private Thread acceptThread;
	private final ExecutorService executor;

	List<ClientThread> clientConnections = new ArrayList<ClientThread>();

	private final URI address;

	public TcpReceiver(URI address, Receiver receiver, ExecutorService executor) {
		this.address = address;
		this.receiver = receiver;
		this.executor = executor;
		try {
			URI uri = address;
			port = uri.getPort();
			serverSocket = new ServerSocket(port);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void start() {
		running = true;
		final Receiver superReceiver = this;
		acceptThread = new Thread() {
			public void run() {
				while (running) {
					try {
						Socket clientSocket = serverSocket.accept();
						ClientThread clientThread = new ClientThread(clientSocket, superReceiver,address);
						clientConnections.add(clientThread);
						executor.execute(clientThread);
					} catch (Throwable t) {
						if (running)
							t.printStackTrace();
					}
				}
			}
		};
		acceptThread.start();
		Thread.yield();
	}

	public void stop() {
		running = false;
		try {
			if (!serverSocket.isClosed())
				serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (ClientThread connection : this.clientConnections) {
			try {
				connection.stop();
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
	}

	public static class ClientThread implements Runnable {

		private final Socket clientSocket;
		private InputStream inputStream;
		private final Receiver receiver2;
		boolean running = false;
		private final URI address2;

		public ClientThread(Socket clientSocket, Receiver receiver, URI address) {
			address2 = address;
			try {
				LOGGER.debug("Handling client at " + clientSocket.getInetAddress().getHostAddress() + " on port "
						+ clientSocket.getPort());
				this.clientSocket = clientSocket;
				receiver2 = receiver;
				inputStream = clientSocket.getInputStream();
				running = true;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	
		
		public void run() {
			
			byte[] headerBuffer = new byte[HEADER_BYTE_SIZE_2.length];
			
			long readCount = 0;
			while (running) {
				try {
					int read = inputStream.read(headerBuffer);
					if (read == -1) {
						LOGGER.warn("Received -1 Bytes, Closing Client connection:" + address2);
						running = false;
						continue;
					}
					
					readCount++;
					
					if (!isValidHeader(headerBuffer)) {
						LOGGER.warn("readCount:" + readCount + " Received bad packet:" + new String(headerBuffer));
						// enter error recovery - scan until we find a header
						correctInputStream(inputStream, headerBuffer);
					}
					
					int direction = BinConvertor.byteArrayToInt(headerBuffer, HEADER_BYTES.length);
					int incomingPacketSize = BinConvertor.byteArrayToInt(headerBuffer, HEADER_BYTE_SIZE_1.length);
					byte[] incoming = new byte[incomingPacketSize];
					int read2 = inputStream.read(incoming);
					if (read2 == -1) {
						LOGGER.warn("Received -1 Bytes, Closing Client connection:" + address2);
						running = false;
						continue;
					}
					if (read2 != incoming.length) {
						LOGGER.warn("DID NOT READ CORRECT BYTE AMOUNT:" + read2 + " wanted:" + incomingPacketSize);
//						continue;
					}
					receiver2.receive(incoming, null, null);
				} catch (Exception e) {
					if (running) e.printStackTrace();
				}
			}
		}

		/**
		 * scroll through to find the next valid HEADER
		 * @param inputStream
		 * @param headerBuffer
		 */
		private void correctInputStream(InputStream inputStream, byte[] headerBuffer) {
			boolean foundHeader = false;
			try {
				int byteThrowCount = 0;
				while (!foundHeader) {
						byte nextByte = (byte) inputStream.read();
						for (int i = 0; i < headerBuffer.length-1; i++) {
							headerBuffer[i] = headerBuffer[i+1];
						}
						headerBuffer[headerBuffer.length-1] = nextByte;
						if (isValidHeader(headerBuffer)) return;
						byteThrowCount++;
						System.err.println("throw byte:" + byteThrowCount);
						
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private boolean isValidHeader(byte[] headerBuffer) {
			for (int i = 0; i < HEADER_BYTES.length; i ++) {
				if (headerBuffer[i] != HEADER_BYTES[i]) return false;
			}
			return true;
		}

		public void stop() {
			try {
				this.running = false;
				this.clientSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public byte[] receive(final byte[] payload, String remoteAddress, String remoteHostname) throws InterruptedException {
		receiver.receive(payload, remoteAddress, remoteHostname);
		return null;
	}
	public boolean isForMe(Object payload) {
		throw new RuntimeException("Not implemented");
	}
	public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
		throw new RuntimeException("Not implemented");
	}


}
