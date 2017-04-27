package com.liquidlabs.common.file;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.raf.ByteBufferRAF;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RAFCharSplittingTest {
	private static final String TEST_FILE = "build/RAFCharSplittingTest.log";
	int lineLength = 10;
	private ByteBufferRAF raf;
	int rows = 5 * 10 * 1000;
	
	@Before
	public void setUp() throws Exception {
		OutputStream fos = new BufferedOutputStream( new FileOutputStream(TEST_FILE));
		for (int i = 1; i <= rows; i++) {
//            fos.write((i +"|A|B|C|D|\n").getBytes());
            String line = "line! " + i + "!device_id!WEST_STREET!duration!20!policy_id!33!proto!6!zone!Trust!action!Permit!sent!234!rcvd!0!src!10.181.100.200!dst!10.0.6.211!src_port!16848!dst_port!9080!port!16848!session_id!6393!reason!Close!start_time!2015-01-09 23:59:54!_timestamp!1399781076!\n";
            fos.write(line.getBytes());
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

    /**
     * Performance
     * - Without splitting: 1,510K
     * - Manual split:      540k / now 750K!
     *
     * DirectBuffer Splitting
     * - ArrayByteBuffer:   320K
     * - StringBuilder:     261K
     * - CharArrayWriter:   270K (locks removed)
     * - hppc CAL:          210K
     * - Trove:             290k
     *
     * HeapBuffer Splitting
     * - Array:   245K

     * @throws Exception
     */

	@Test
	public void shouldScanAndSplitLines() throws Exception {

        int lines = 0;
//        for (int i = 0; i < 1000; i++) {
		for (int i = 0; i < 10; i++) {
            raf.seek(0);
            String[] line = new String[0];
            String aa = "";
            lines = 0;
            long start = DateTimeUtils.currentTimeMillis();
            // Version 1 - split on the BB
//            while ((line = raf.readLine('!')) != null) {
            // Version 2 - splon retrieved line
            while ((aa = raf.readLine()) != null) {
               StringUtil.splitFast(aa, '!');
//                assertEquals("Line:" + lines, 5, line.length);
                lines++;
            }
            long end = DateTimeUtils.currentTimeMillis();
            System.out.println("Lines:" + lines + " e:" + (end - start) + " Rate:" + lines/( (end - start)/1000.0) );
        }
		assertEquals(this.rows, lines);
	}
	

}
