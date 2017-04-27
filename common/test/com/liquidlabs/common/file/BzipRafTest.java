package com.liquidlabs.common.file;

import static junit.framework.Assert.assertEquals;

import com.liquidlabs.common.file.raf.BzipRaf;
import org.junit.Before;
import org.junit.Test;

public class BzipRafTest {
	String filename = "test-data/system.log.0.bz2";
	
	@Before
	public void setup() {
		
	}

	@Test
	public void testReadLine() throws Exception {
		
		BzipRaf gzin = new BzipRaf(filename);
		String line = "";
		int c = 0;
        long start = System.currentTimeMillis();

        while ((line = gzin.readLine()) != null) {
			c++;
			if (c < 10) System.out.println(c + ": " + line);
		}
        long end = System.currentTimeMillis();
        System.out.println("Elapsed:" + (end - start));
        assertEquals(9042, c);
	}
	
	@Test
	public void testShouldSeekToPosition() throws Exception {
		BzipRaf gzin = new BzipRaf(filename);
		gzin.seek(1024);
		String line = "";
		int c = 0;
		while ((line = gzin.readLine()) != null) {
			c++;
			if (c < 10) System.out.println(c + ": " + line);
		}
		assertEquals(9036, c);
	}
	
	@Test
	public void testSeekGood() throws Exception {
		BzipRaf in = new BzipRaf(filename);
		in.seek(4);
		String oneA1 = in.readLine();
		String oneA2 = in.readLine();
		in.seek(4);
		String oneB1 = in.readLine();
		String oneB2 = in.readLine();
		
		assertEquals(oneA1, oneB1);
		assertEquals(oneA2, oneB2);
	}
}
