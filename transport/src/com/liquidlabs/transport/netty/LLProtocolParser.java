package com.liquidlabs.transport.netty;

import java.io.IOException;
import java.util.concurrent.Executors;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.transport.ProtocolParser;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.netty.NettyReceiver.ReplySender;
import com.liquidlabs.transport.protocol.State;
import com.liquidlabs.transport.protocol.Type;

/**
 * Parses ByteBuffers
 * 
 */
public class LLProtocolParser implements ProtocolParser {
	static final Logger LOGGER = Logger.getLogger(LLProtocolParser.class);
	private final Receiver receiver;

	@Override
	public String protocol() {
		return "tcp";
	}

	// Use different pools for receiver (remote invocations) versus (reply
	// invocations) responses to our own invocations
	volatile int recvdMsgs;
	volatile long recvdBytes;

	public LLProtocolParser(Receiver receiver) {
		this.receiver = receiver;
		replyExecutor = Executors.newCachedThreadPool(new NamingThreadFactory("netty-reply"));
	}

	public StreamState process(final ChannelBuffer byteBuffer, final StreamState state, final ReplySender replySender, final CMDProcessor processor) throws IOException {

		recvdMsgs++;
		recvdBytes +=  byteBuffer.readableBytes();
		if (replyExecutor.isShutdown()) return state;
		
		while (byteBuffer.readable()) {
			if (state.parseState == State.HEADER) {
				state.readHeader(byteBuffer, processor);
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
						final byte[] payload = state.getPayload();
						this.payload = payload;
						Type invocationType = state.getType();
						
						if (invocationType.equals(Type.REQUEST)) {
							receiver.receive(payload, state.ipAddress, state.hostname);
						} else if (invocationType.equals(Type.RESPONSE)) {
							receiver.receive(payload, state.ipAddress, state.hostname);
						} else if (invocationType.equals(com.liquidlabs.transport.protocol.Type.SEND_REPLY)) {
							
							replyExecutor.execute(new Runnable() {
								public void run() {
									try {
										byte[] reply = receiver.receive(payload, state.ipAddress, state.hostname);
										if (reply != null) replySender.sendReply(reply);
									} catch (InterruptedException t) {
									}
								}
							});
						}
					} else {
							try {
								state.t = null;
								state.reply = receiver.receive(state.getPayload(), state.ipAddress, state.hostname);
							} catch (Throwable t) {
								state.t = t;
							}
							this.reply = state.reply;
					}
				} catch (Throwable t) {
					LOGGER.error("msgFailed:" + state.hashCode() + "- ex:" + t.toString());
				}
				state.reset();
			}
		}
		return state;
	}

	public byte[] reply;
	public byte[] payload;
	private java.util.concurrent.ExecutorService replyExecutor;
}
