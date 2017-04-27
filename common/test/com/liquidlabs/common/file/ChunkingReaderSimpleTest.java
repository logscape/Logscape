package com.liquidlabs.common.file;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.ChunkingRAFReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChunkingReaderSimpleTest {

	private File file;

	@Before
	public void setUp() throws Exception {
		
		file = new File("test-data","mline-braf-test.txt");
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void shouldCountLines() throws Exception {
		ChunkingRAFReader raf = new ChunkingRAFReader(file.getAbsolutePath(), BreakRule.Rule.Year.name());
		List<String> lines = raf.readLines(0);
		assertEquals(13, lines.size());
	}
}
