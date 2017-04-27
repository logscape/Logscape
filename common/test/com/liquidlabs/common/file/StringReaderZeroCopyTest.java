package com.liquidlabs.common.file;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class StringReaderZeroCopyTest {
	
	private StringReaderZeroCopy sb;


	@Before
	public void setup() {
		sb = new StringReaderZeroCopy();
	}
	
	int pullCalls;
	@Test
	public void shouldUsePullReaderToSuckInData() throws Exception {
		
		sb.registerPuller(new StringPuller() {

			public String pull() {
				if (pullCalls++ < 3) {
					System.out.println("Pulled:" + pullCalls);
					return pullCalls + "\n";
				}
				return null;
			}
		});
		
		String line = null;
		
		line = sb.readUntil();
		assertEquals("1", line);
		
		line = sb.readUntil();
		assertEquals("2", line);
		
		line = sb.readUntil();
		assertEquals("3", line);
		
		line = sb.readUntil();
		assertNull(line);
		
		
	}
	
	@Test
	public void shouldObeyMLineRules() throws Exception {
		sb.addString("11\n");
		sb.addString(" indented1\n");
		sb.addString("22\n");
		sb.addString(" indented2\n");
		sb.addString("33\n");
		
		String line = "";
		line = sb.readUntil();
		assertTrue(line.contains("11"));
		assertTrue(line.contains("indented1"));
		
		line = sb.readUntil();
		assertTrue(line.contains("22"));
		assertTrue(line.contains("indented2"));
		
		line = sb.readUntil();
		assertTrue(line.contains("33"));

		
	}
	
	
	@Test
	public void shouldPeekProperly() throws Exception {
		sb.addString("0");
		sb.addString("12");
		sb.addString("3");
		
		byte peek =  '-';
		peek = sb.peek(0);
		assertEquals('0',  peek);
		
		peek = sb.peek(1);
		assertEquals('1',  peek);
		
		peek = sb.peek(2);
		assertEquals('2',  peek);
		
		peek = sb.peek(3);
		assertEquals('3',  peek);
	}
	

	@Test
	public void shouldReadAcrossMultipleLines() throws Exception {
		sb.addString("1");
		sb.addString("");
		sb.addString("2");
		sb.addString("3\n");
		sb.addString("456\n");

		String line = null;
		line = sb.readLine();
		assertEquals("123", line);
		
		line = sb.readLine();
		assertEquals("456", line);
		
		line = sb.readLine();
		assertNull(line);
	}
	
	@Test
	public void shouldReadAcrossNextLines() throws Exception {
		sb.addString("1\n");
		sb.addString("2\n");
		sb.addString("3\n");
		
		String line = null;
		
		line = sb.readLine();
		assertEquals("1", line);
		
		line = sb.readLine();
		assertEquals("2", line);
		
		line = sb.readLine();
		assertEquals("3", line);
		
	}
	
	
	@Test
	public void shouldReadLine() throws Exception {
		sb.addString("1\n2\n3\n");
		String line = null;
		line = sb.readLine();
		assertEquals("1", line);
		
		line = sb.readLine();
		assertEquals("2", line);
		
		line = sb.readLine();
		assertEquals("3", line);
		
		line = sb.readLine();
		assertNull(line);
	}
	
	@Test
	public void shouldRead() throws Exception {
		sb.addString("0");
		sb.addString("12");
		sb.addString("3");
		
		assertEquals("0123", sb.readLine());
	}
	
	@Test
	public void shouldReadAcrossMultipleStrings() throws Exception {
		sb.addString("1");
		sb.addString("2");
		sb.addString("3");
		byte char1 = ' ';
		char1 = sb.readChar();
		assertEquals('1', char1);
		char1 = sb.readChar();
		assertEquals('2', char1);
		char1 = sb.readChar();
		assertEquals('3', char1);
	}
	
	
	@Test
	public void shouldBeAbleToAddStringAndReturnReadLines() throws Exception {
		sb.addString("123");
		byte char1 = ' ';
		char1 = sb.readChar();
		assertEquals('1', char1);
		char1 = sb.readChar();
		assertEquals('2', char1);
		char1 = sb.readChar();
		assertEquals('3', char1);
		
	}

}
