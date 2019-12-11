package com.liquidlabs.common.util;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

public class HeapDumperTest {
	
	
	@Test
	public void shouldDumpHeap() throws Exception {
		String testFilename = "build/test-dump.hprof";
		new File(testFilename).delete();
		new File("build").mkdir();

		new HeapDumper().dumpHeap(testFilename, true);
		assertTrue(new File(testFilename).exists());
		
	}

}
