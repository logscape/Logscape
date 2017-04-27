package com.liquidlabs.common.file;

import static org.junit.Assert.*;

import java.io.*;

import com.liquidlabs.common.file.raf.ByteBufferRAF;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ByteBufferRAFTest {
	private static final String TEST_FILE = "build/BBRafTest.log";
	int lineLength = 10;
	private ByteBufferRAF raf;
	int rows = 1000;//000;
	
	@Before
	public void setUp() throws Exception {
		OutputStream fos = new BufferedOutputStream(new FileOutputStream(TEST_FILE));
		for (int i = 1; i <= rows; i++) {
			for (int j = 0; j < lineLength-1; j++) {
				fos.write((" " + Integer.toString(i)).getBytes());
			}
			fos.write(FileUtil.EOLN.getBytes());
			
		}
		fos.close();

		raf = new ByteBufferRAF(TEST_FILE);
	}
	@After
	public void teardown() {
		try {
			if (raf != null) raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		new File(TEST_FILE).delete();
	}


    @Test
	public void shouldReadCharSet() throws Exception {
        String test = "ABCDEF";
        byte[] bytes = test.getBytes("ISO-8859-1");
        char c1 = (char) bytes[0];
        char c2 = (char) bytes[1];
        char c3 = (char) bytes[2];
        System.out.print(c1);
        System.out.print(c2);
        System.out.print(c3);
    }

	@Test
	public void testShouldSeekEachEOLPosition() throws Exception {
		long p1 = raf.getFilePointer();
		String line1 = raf.readLine();
		long p2 = raf.getFilePointer();
        assertTrue(" Raf Pointer was at:" + p2, p2 > line1.length());
		String line2 = raf.readLine();
		long p3 = raf.getFilePointer();
		String line3 = raf.readLine();
		
		raf.seek(p1);
		String readLine1 = raf.readLine();
		assertEquals(line1, readLine1);

		raf.seek(p2);
		String readLine2 = raf.readLine();
		assertEquals(line2, readLine2);
		
		raf.seek(p3);
		String readLine3 = raf.readLine();
		assertEquals(line3, readLine3);
	}
	@Test
	public void shouldScanWholeFile() throws Exception {
		
		long start = DateTimeUtils.currentTimeMillis();
		
		String line = "";
		int lines = 0;
		while ((line = raf.readLine()) != null) {
            lines++;
		}
		long end = DateTimeUtils.currentTimeMillis();
		System.out.println("Lines:" + lines + " e:" + (end - start));
		assertEquals(this.rows, lines);
	}
	

}
