package com.liquidlabs.transport;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.liquidlabs.transport.netty.CMDProcessor;
import com.liquidlabs.transport.netty.StreamState;
import com.liquidlabs.transport.netty.NettyReceiver.ReplySender;

public interface ProtocolParser {

	String protocol();

	StreamState process(ChannelBuffer byteBuffer, StreamState state, ReplySender replySender, CMDProcessor processor) throws IOException;

}
