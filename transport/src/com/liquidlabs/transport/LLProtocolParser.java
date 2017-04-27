package com.liquidlabs.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

import com.liquidlabs.transport.protocol.Type;


/**
 * Parses ByteBuffers
 * 
 */
public class LLProtocolParser {
	enum State {
		HEADER, TYPE, SIZE, BODY, CALL_READER
	};
	
	private final Receiver receiver;
	
	// Use different pools for receiver (remote invocations) versus (reply invocations) responses to our own invocations
	private Executor receiverExecutor;
	private Executor responseExecutor;

	public LLProtocolParser(Receiver receiver) {
		this.receiver = receiver;
	}

	public LLProtocolParser(Receiver receiver, Executor receiverExecutor, Executor responseExecutor) {
		this.receiver = receiver;
		this.receiverExecutor = receiverExecutor;
		this.responseExecutor = responseExecutor;
	}

	public StreamState process(ByteBuffer byteBuffer, final StreamState state) throws IOException {

		while (byteBuffer.hasRemaining()) {
			if (state.parseState == State.HEADER) {
				state.readHeader(byteBuffer);
			} else if (state.parseState == State.TYPE) {
				state.readType(byteBuffer);
			} else if (state.parseState == State.SIZE) {
				state.readBodySize(byteBuffer);
			} else if (state.parseState == State.BODY) {
				state.readBody(byteBuffer);
			}
			if (state.parseState == State.CALL_READER) {
				try {
					if (state.parseState == State.CALL_READER) {
						if (receiverExecutor != null) {
							final byte[] payload = state.getPayload();
							Type invocationType = state.getType();
							
							if (invocationType.equals(Type.REQUEST)) {
								receiverExecutor.execute(new Runnable(){
									public void run() {
										try {
											receiver.receive(payload, state.ipAddress, state.hostname);
										} catch (InterruptedException e) {
										}									
									}
								});
							} else {
								responseExecutor.execute(new Runnable(){
									public void run() {
										try {
											receiver.receive(payload, state.ipAddress, state.hostname);
										} catch (InterruptedException e) {
										}									
									}
								});
								
							}
							
						} else {
							receiver.receive(state.getPayload(), state.ipAddress, state.hostname);
						}
					}
				} catch (Throwable t) {
					throw new RuntimeException(t);
				}
				state.reset();
			}
		}
		return state;
	}
}
