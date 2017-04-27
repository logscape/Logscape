package com.liquidlabs.common;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

public class UnpackerTest {
	
	@Test
	public void shouldDetectValidZip() throws Exception {
		assertTrue(Unpacker.isValidZip(new File("test-data/unpack-test.zip")));
		assertFalse(Unpacker.isValidZip(new File("test-data/unpack-test-bad.zip")));
	}

}
