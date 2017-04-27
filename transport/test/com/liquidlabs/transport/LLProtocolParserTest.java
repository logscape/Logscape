package com.liquidlabs.transport;

import static com.liquidlabs.transport.protocol.NetworkConfig.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.jmock.Mockery;

import com.liquidlabs.transport.LLProtocolParser;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.StreamState;
import com.liquidlabs.transport.LLProtocolParser.State;

public class LLProtocolParserTest extends TestCase {
	Mockery mockery = new Mockery();
	private LLProtocolParser parser;
	
	List<String> stuff = new ArrayList<String>();
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		parser = new LLProtocolParser(new MyReceiver());
	}
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testShouldHandlePartialSize() throws Exception {
		
		ByteBuffer allocate = ByteBuffer.allocate(2);
		allocate.put((byte)0);
		allocate.put((byte)0);
//		allocate.put((byte)0);
//		allocate.put((byte)100);
		allocate.flip();
	
		StreamState state = new StreamState();
		state.parseState = State.SIZE;
		
		StreamState state1 = parser.process(allocate, state);
		
		assertEquals(State.SIZE, state1.parseState);
		
		
	}
	
	public void testShouldCorrectInputStream() throws Exception {
		byte[] bytesForHeader = "LL_TCX".getBytes();
		ByteBuffer byteBuffer = getBB("rubbish_" + HEADER);
		
		boolean foundIt = new StreamState().correctInputStream(byteBuffer, bytesForHeader);
		
		assertTrue("Should have found the HEADER", foundIt);
		assertEquals(HEADER, new String(bytesForHeader));
		
	}
	
	public void testShouldHandleRubbishAndFindHeader() throws Exception {
		ByteBuffer byteBuffer = getBB("RUBBISH" + HEADER);
		
		StreamState state = new StreamState();
		
		StreamState endState = parser.process(byteBuffer, state);
		
		assertEquals(LLProtocolParser.State.TYPE, endState.parseState);
		
	}
	
	public void testShouldReadBodyPartialCompleteAndCallReader() throws Exception {
		ByteBuffer byteBuffer = getBB("load");
		
		StreamState state = new StreamState();
		state.bodyRemaining = "load".length();
		state.bodySize = "payload".length();
		state.parts.write(HEADER.getBytes());
		state.parts.write(new byte[] { 0,0,0,7 });
		state.parts.write(new byte[] { 0,0,0,7 });
		state.parts.write("pay".getBytes());
		state.parseState = State.BODY;
		
		StreamState endState = parser.process(byteBuffer, state);
		
		assertEquals(LLProtocolParser.State.HEADER, endState.parseState);
		assertTrue(stuff.toString(), stuff.size() == 1);
	}
	
	public void testShouldReadBodyAndCallReader() throws Exception {
		
		ByteBuffer byteBuffer = getBB("payload");
		
		StreamState state = new StreamState();
		state.bodyRemaining = "payload".length();
		state.bodySize = "payload".length();
		state.parts.write(HEADER.getBytes());
		state.parts.write(new byte[] { 0,0,0,7 });
		state.parts.write(new byte[] { 0,0,0,7 });
		state.parseState = State.BODY;
		
		
		StreamState endState = parser.process(byteBuffer, state);
		
		assertEquals(LLProtocolParser.State.HEADER, endState.parseState);
		
		assertTrue(stuff.size() == 1);
	}
	
	public void testShouldReadSizeRight() throws Exception {
		ByteBuffer headerBuffer = ByteBuffer.allocate( HEADER_BYTE_SIZE_2.length);
		headerBuffer.putInt(128);
		headerBuffer.putInt(128);
		headerBuffer.flip();
		
		StreamState state = new StreamState();
		state.parts.write(HEADER.getBytes());
		state.parseState = State.SIZE;
		
		StreamState endState = parser.process(headerBuffer, state);
		
		assertEquals(LLProtocolParser.State.BODY, endState.parseState);
		assertEquals(128, endState.bodySize);
	}
	
	public void testShouldHandleFullHeader() throws Exception {
		
		ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER.length());
		headerBuffer.put(HEADER.getBytes());
		headerBuffer.flip();
		
		StreamState state = new StreamState();
		
		StreamState endState = parser.process(headerBuffer, state);
		
		assertEquals(LLProtocolParser.State.TYPE, endState.parseState);
	}
	
	
	public void testShouldHandlePartialHeader() throws Exception {
		
		ByteBuffer headerBuffer = getBB("LL_");
		
		StreamState state = new StreamState();
		
		StreamState endState = parser.process(headerBuffer, state);
		
		assertEquals(LLProtocolParser.State.HEADER, endState.parseState);
		
		ByteBuffer headerBuffer1 = getBB("TCP");
		StreamState endState1 = parser.process(headerBuffer1, state);
		
		assertEquals(LLProtocolParser.State.TYPE, endState1.parseState);
		
		
	}
	private ByteBuffer getBB(String contents) {
		ByteBuffer headerBuffer = ByteBuffer.allocate(contents.length());
		headerBuffer.put(contents.getBytes());
		headerBuffer.flip();
		return headerBuffer;
	}
	
	public class MyReceiver implements Receiver {

		public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) {
			stuff.add(new String(payload));
			return null;
		}

		public void start() {
		}

		public void stop() {
		}

		public boolean isForMe(Object payload) {
			throw new RuntimeException("Not implemented");
		}
		public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
			throw new RuntimeException("Not implemented");
		}

	}
	
}
