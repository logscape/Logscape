package com.liquidlabs.transport.netty;

import static com.liquidlabs.transport.protocol.NetworkConfig.HEADER;
import static com.liquidlabs.transport.protocol.NetworkConfig.HEADER_BYTES;
import static com.liquidlabs.transport.protocol.NetworkConfig.HEADER_BYTE_SIZE_1;
import static com.liquidlabs.transport.protocol.NetworkConfig.HEADER_BYTE_SIZE_2;

import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;

import com.liquidlabs.common.BinConvertor;
import com.liquidlabs.transport.protocol.State;
import com.liquidlabs.transport.protocol.Type;


/**
 * Used to assemble incoming packets associated with a SelectorKey/TCP Stream (1 per connected client).
 * Connectionless protocols like UDP would need to associate and assemble based upon
 * individual packets instead of streams (i.e. packet header identified the client).
 *
 */
public class StreamState {
	
	static final Logger LOGGER = Logger.getLogger(StreamState.class);
	
	transient byte[] payload;
	transient byte[] reply;

	public State parseState = State.HEADER;
	public int type;
	public int bodySize;
	public int bodyRemaining;
	public String hostname;
	public String ipAddress;
	public ByteArrayOutputStream parts = new ByteArrayOutputStream();
	public String serverURI;
	transient public Throwable t;


	public StreamState() {
	}
	
	public void readHeader(ChannelBuffer byteBuffer, final CMDProcessor processor) throws IOException {
		
		int bytesToRead = Math.min(HEADER_BYTES.length -  this.parts.size(), byteBuffer.readableBytes());
		byte[] nextPart = new byte[bytesToRead];
		byteBuffer.readBytes(nextPart);
		parts.write(nextPart);

		if (parts.size() >= HEADER.length()) {
			
			if (isValidHeader(parts.toByteArray())) {
				// HEADER is VALID
				parseState = State.TYPE;
			} else {
				// HEADER is stuffed, so try and scroll through to find another one
				if (new String(nextPart).startsWith("CMD:")) {
					handleCMDRequest(byteBuffer, processor, nextPart);
					parseState = State.HEADER;
					
				} else {
					parts.reset();
					byte[] bytes = new byte[HEADER_BYTES.length];
					if (correctInputStream(byteBuffer,bytes)) {
						parts.write(bytes);
						parseState = State.TYPE;
					}
				}
			}
		}
	}

	private void handleCMDRequest(ChannelBuffer byteBuffer, final CMDProcessor processor, byte[] nextPart) {
		final StringBuilder sb = new StringBuilder(new String(nextPart));
		byte[] part = new byte[byteBuffer.readableBytes()];
		byteBuffer.readBytes(part);
		sb.append(new String(part));
		LOGGER.info("Handling CMD:" + sb.toString());

		if (processor != null) processor.handle(sb.toString());
		else LOGGER.warn("Didnt find processor for:" + sb.toString());
	}
	public void readType(ChannelBuffer byteBuffer) throws IOException {
		int bytesToRead = Math.min(HEADER_BYTE_SIZE_1.length -  this.parts.size(), byteBuffer.readableBytes());
		byte[] nextPart = new byte[bytesToRead];
		byteBuffer.readBytes(nextPart);
		parts.write(nextPart);
		
		if (this.parts.size() == HEADER_BYTE_SIZE_1.length) {
			byte[] byteArray = this.parts.toByteArray();
			type = BinConvertor.byteArrayToInt(byteArray, HEADER_BYTES.length);
			parseState = State.SIZE;
		}
	}
	public void readBodySize(ChannelBuffer byteBuffer) throws IOException {
		int bytesToRead = Math.min(HEADER_BYTE_SIZE_2.length -  this.parts.size(), byteBuffer.readableBytes());
		byte[] nextPart = new byte[bytesToRead];
		byteBuffer.readBytes(nextPart);
		parts.write(nextPart);
		
		if (this.parts.size() == HEADER_BYTE_SIZE_2.length) {
			byte[] byteArray = this.parts.toByteArray();
			bodySize = BinConvertor.byteArrayToInt(byteArray, HEADER_BYTE_SIZE_1.length);
			bodyRemaining = bodySize;
			parseState = State.BODY;
		}
	}

	public void readBody(ChannelBuffer byteBuffer) throws IOException {
		int bytesToRead = Math.min(bodyRemaining, byteBuffer.readableBytes());
		byte[] part = new byte[bytesToRead];
		byteBuffer.readBytes(part);
		parts.write(part);
		bodyRemaining -= bytesToRead;
		if (bodyRemaining == 0) {
			parseState = State.CALL_READER;
		}
	}
	
	public byte[] getPayload() {
		if (parts.size()  < HEADER_BYTE_SIZE_2.length + bodySize) {
			throw new RuntimeException("InsufficientBytes, expecting:" + HEADER_BYTES.length + " + " + bodySize + "  bufferSize:" + parts.size());
		}
		byte[] byteArray = parts.toByteArray();
		byte[] payload = new byte[bodySize];
		System.arraycopy(byteArray, HEADER_BYTE_SIZE_2.length, payload, 0, bodySize);
		this.payload = payload;
		return payload;
	}
	public void reset() {
		this.parseState = State.HEADER;
		this.parts.reset();
	}
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("[StreamState:");
		buffer.append(" parseState: ");
		buffer.append(parseState);
		buffer.append("]");
		return buffer.toString();
	}
	boolean correctInputStream(ChannelBuffer inputStream, byte[] headerBuffer) {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int byteThrowCount = 0;
		try {
			boolean foundHeader = false;
			while (!foundHeader && inputStream.readable()) {
				byte nextByte = inputStream.readByte();
				for (int i = 0; i < headerBuffer.length - 1; i++) {
					headerBuffer[i] = headerBuffer[i + 1];
				}
				headerBuffer[headerBuffer.length - 1] = nextByte;
				if (isValidHeader(headerBuffer))
					return true;
				byteThrowCount++;
				baos.write(nextByte);
			}
			return isValidHeader(headerBuffer);
		} finally {
			LOGGER.error("server:" + serverURI + " client:" + ipAddress + " *** Got HEADER:" + new String(headerBuffer));
			LOGGER.error("server:" + serverURI + " client:" + ipAddress + " *** THREW bytes:" + byteThrowCount);
		}
	}

	private boolean isValidHeader(byte[] headerBuffer) {
		for (int i = 0; i < HEADER_BYTES.length; i++) {
			if (headerBuffer[i] != HEADER_BYTES[i])
				return false;
		}
		return true;
	}

	public com.liquidlabs.transport.protocol.Type getType() {
		return Type.values()[type];
	}

}
