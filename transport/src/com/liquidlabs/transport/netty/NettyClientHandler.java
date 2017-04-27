/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package com.liquidlabs.transport.netty;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.proxy.RetryInvocationException;


public class NettyClientHandler extends SimpleChannelUpstreamHandler implements Receiver {

    private static final Logger logger = Logger.getLogger(NettyClientHandler.class);

    private ChannelBuffer channelBuffer =  dynamicBuffer();
    private final AtomicLong transferredBytes = new AtomicLong();
    private final AtomicLong msgsReceived = new AtomicLong();
    LLProtocolParser protocolParser = new LLProtocolParser(this);

	private final URI talkingTo;

    /**
     * Creates a server-side handler.
     */
    public NettyClientHandler(URI talkingTo) {
		this.talkingTo = talkingTo;
    }
    
    CountDownLatch latch;

	public void sendAMessage(Channel channel, byte[] msg) throws InterruptedException {
		
		latch = new CountDownLatch(1);
	
		int currentPut = 0;
		for (int p = 0; p < msg.length; p++) {
            channelBuffer.writeByte((byte) msg[p]);
            if (currentPut++ == channelBuffer.capacity()) {
            	channel.write(channelBuffer);
            	currentPut = 0;
            }
		}
		if (currentPut > 0) channel.write(channelBuffer);
	}

    public long getTransferredBytes() {
        return transferredBytes.get();
    }

    byte[] reply;

	private String context;

	private Throwable exception;

	private CountDownLatch connectLatch;

	private State status = LifeCycle.State.STOPPED;

    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    	try {
			if (ctx.getAttachment() == null) ctx.setAttachment(new StreamState());
			StreamState attachment = (StreamState) ctx.getAttachment();
	
	        ChannelBuffer byteBuffer = (ChannelBuffer) e.getMessage();
			StreamState newState = protocolParser.process(byteBuffer, attachment, null, null);
			ctx.setAttachment(newState);
			
		} catch (IOException e1) {
			System.out.println("NettyClientHandler:" + e1.getMessage());
		}
    }
	public void setConnectLatch(CountDownLatch connectLatch) {
		this.connectLatch = connectLatch;
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelConnected(ctx, e);
		if (this.connectLatch != null) this.connectLatch.countDown();
	}
    
    public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) throws InterruptedException {
    	this.reply = payload;
    	latch.countDown();
    	return null;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    	this.exception = e.getCause();
    	if (this.connectLatch != null) this.connectLatch.countDown();
    	if (status == State.STOPPED) return;
    	if (this.latch != null) {
    		latch.countDown();
    		
    		if (e.getCause() != null) {
//    			if (e.getCause().getClass().equals(NotYetConnectedException.class)) {
//    				return ;
//    			}
//    			if (e.getCause().getClass().equals(ClosedChannelException.class)) {
//    				return ;
//    			}
//    			if (e.getCause().getClass().equals(ConnectException.class)) {
//    				return ;
//    			}
    			logger.info(String.format("%s #1 Client[%s] TalkingTo[%s] Channel[%s] Unexpected problem from downstream - will retry, latch[%s] ex[%s]", toString(), context, talkingTo, e.getChannel(), latch, e.getCause()));
    		}
    	} else {
    		if (e.getCause() != null && e.getCause().getClass().equals(ClosedChannelException.class)) {
    			logger.info(String.format("%s #2 Client[%s] TalkingTo[%s] Channel[%s] Unexpected problem from downstream - will retry", toString(), context, talkingTo, e.getChannel()));
    		} else {
    			logger.warn(String.format("%s #3 Client[%s] TalkingTo[%s] Channel[%s] Unexpected exception from downstream, ex:%s", toString(), context, talkingTo, e.getChannel(), e.getCause().getMessage()));
    		}
    	}
    }

	public long getMsgsReceived() {
		return msgsReceived.get();
	}

	public void setExpectsReply() {
		reply = null;
		latch = new CountDownLatch(1);
	}

	public byte[] getReply(long timeoutSeconds, String context) throws InterruptedException, RetryInvocationException {
		this.context = context;
		boolean gotReply = latch.await(timeoutSeconds, TimeUnit.SECONDS);
		if (isException()) throw new RetryInvocationException("GetReply() Exception", getException());
		if (!gotReply) {
			throw new RetryInvocationException(("TimeOut waiting for reply from:" + talkingTo + " context:" + context));
		}
		byte[] result = reply;
		this.reply = null;
		return result;
	}
	
	public ChannelBuffer getChannelBuffer() {
		return dynamicBuffer(); 
	}

	public void start() {
		status = LifeCycle.State.STARTED;
	}

	public void stop() {
		status = LifeCycle.State.STOPPED;
		exception = new InterruptedException("Shutting down");
		if (latch != null) latch.countDown();
	}
	@Override
	public String toString() {
		return super.toString() + " m:" + context;
	}

	public boolean isException() {
		return exception != null;
	}

	public Throwable getException() {
		Throwable result = this.exception;
		this.exception = null;
		return result;
	}
	public boolean isForMe(Object payload) {
		throw new RuntimeException("Not implemented");
	}
	public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
		throw new RuntimeException("Not implemented");
	}



}
