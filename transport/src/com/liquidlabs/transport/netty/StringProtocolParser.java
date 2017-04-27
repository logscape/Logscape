package com.liquidlabs.transport.netty;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.liquidlabs.transport.ProtocolParser;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.netty.NettyReceiver.ReplySender;


/**
 * Parses ByteBuffers as regular text and allows the Receiver to do stuff with it
 * 
 */
public class StringProtocolParser implements ProtocolParser {
	private final Receiver receiver;
	
	// Use different pools for receiver (remote invocations) versus (reply invocations) responses to our own invocations
	public StringProtocolParser(Receiver receiver) {
		this.receiver = receiver;
	}
//	public byte[] decode(byte[] reply2) throws IOException {
//		 final StreamState state = new StreamState();
//		ChannelBuffer copiedBuffer = ChannelBuffers.copiedBuffer(reply2);
//		process(copiedBuffer, state);
//		return state.payload;
//	}
    public StreamState process(String string, StreamState state) {
        try {
            receiver.receive(string.getBytes(), state.ipAddress, state.hostname);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return state;
    }
	public StreamState process(ChannelBuffer byteBuffer, StreamState state, ReplySender replySender, CMDProcessor processor) throws IOException {

		try {
			while (byteBuffer.readable()) {
				int readableBytes = byteBuffer.readableBytes();
				byte[] bytes = new byte[readableBytes];
				byteBuffer.readBytes(bytes);
				receiver.receive(bytes, state.ipAddress, state.hostname);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		return state;
	}
	public byte[] reply;
	public byte[] payload;
}
