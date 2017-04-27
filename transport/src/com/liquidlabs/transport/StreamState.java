package com.liquidlabs.transport;

import static com.liquidlabs.transport.protocol.NetworkConfig.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.liquidlabs.common.BinConvertor;
import com.liquidlabs.transport.LLProtocolParser.State;
import com.liquidlabs.transport.protocol.Type;


/**
 * Used to assemble incoming packets associated with a SelectorKey/TCP Stream (1 per connected client).
 * Connectionless protocols like UDP would need to associate and assemble based upon
 * individual packets instead of streams (i.e. packet header identified the client).
 *
 */
public class StreamState {

	public State parseState = State.HEADER;
	public int type;
	public int bodySize;
	public int bodyRemaining;
	public ByteArrayOutputStream parts = new ByteArrayOutputStream();
	protected String ipAddress;
	protected String hostname;

	public StreamState() {
	}
	
	public void readHeader(ByteBuffer byteBuffer) throws IOException {
		
		int bytesToRead = Math.min(HEADER_BYTES.length -  this.parts.size(), byteBuffer.remaining());
		byte[] nextPart = new byte[bytesToRead];
		byteBuffer.get(nextPart);
		parts.write(nextPart);

		if (parts.size() >= HEADER.length()) {
			
			if (isValidHeader(parts.toByteArray())) {
				// HEADER is VALID
				parseState = State.TYPE;
			} else {
				// HEADER is stuffed, so try and scroll through to find another one
				parts.reset();
				byte[] bytes = new byte[HEADER_BYTES.length];
				if (correctInputStream(byteBuffer,bytes)) {
					parts.write(bytes);
					parseState = State.TYPE;
				}
			}
		}
	}
	public void readType(ByteBuffer byteBuffer) throws IOException {
		int bytesToRead = Math.min(HEADER_BYTE_SIZE_1.length -  this.parts.size(), byteBuffer.remaining());
		byte[] nextPart = new byte[bytesToRead];
		byteBuffer.get(nextPart);
		parts.write(nextPart);
		
		if (this.parts.size() == HEADER_BYTE_SIZE_1.length) {
			byte[] byteArray = this.parts.toByteArray();
			type = BinConvertor.byteArrayToInt(byteArray, HEADER_BYTES.length);
			parseState = State.SIZE;
		}
	}
	public void readBodySize(ByteBuffer byteBuffer) throws IOException {
		int bytesToRead = Math.min(HEADER_BYTE_SIZE_2.length -  this.parts.size(), byteBuffer.remaining());
		byte[] nextPart = new byte[bytesToRead];
		byteBuffer.get(nextPart);
		parts.write(nextPart);
		
		if (this.parts.size() == HEADER_BYTE_SIZE_2.length) {
			byte[] byteArray = this.parts.toByteArray();
			bodySize = BinConvertor.byteArrayToInt(byteArray, HEADER_BYTE_SIZE_1.length);
			bodyRemaining = bodySize;
			parseState = State.BODY;
		}
	}

	public void readBody(ByteBuffer byteBuffer) throws IOException {
		int bytesToRead = Math.min(bodyRemaining, byteBuffer.remaining());
		byte[] part = new byte[bytesToRead];
		byteBuffer.get(part);
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
	boolean correctInputStream(ByteBuffer inputStream, byte[] headerBuffer) {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int byteThrowCount = 0;
		try {
			boolean foundHeader = false;
			while (!foundHeader && inputStream.remaining() > 0) {
				byte nextByte = (byte) inputStream.get();
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
			System.err.println(getClass().getName() + " *** Got HEADER:" + new String(headerBuffer));
			System.err.println(getClass().getName() + " *** THREW bytes:" + byteThrowCount);

		}

	}

	private boolean isValidHeader(byte[] headerBuffer) {
		for (int i = 0; i < HEADER_BYTES.length; i++) {
			if (headerBuffer[i] != HEADER_BYTES[i])
				return false;
		}
		return true;
	}

	public Type getType() {
		return Type.values()[type];
	}

}
