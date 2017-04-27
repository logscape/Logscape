package com.liquidlabs.common.file;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.util.zip.GZIPOutputStream;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertThat;

import com.liquidlabs.common.file.raf.*;
import org.hamcrest.Matchers;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

public class GzipMLineRafTest {
	String gzipFilename = "build/testgzipML.gz";
	private RAF gzin;
	
	@Before
	public void setup() {
		
		try {
			GZIPOutputStream fos = new GZIPOutputStream(new FileOutputStream(gzipFilename));
			fos.write("aaa\nbbb\n ccc\nddd\n eee\nfff\neee".getBytes());
			fos.close();
			gzin = new MLineGzip(gzipFilename);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

//    @Test
//    public void shouldReadWebLog() throws Exception {
//        MLineGzip is = new MLineGzip("/WORK/logs/weblogs/logscape.com-Feb-2013.gz");
//        String line = null;
//        int count =0;
//        long start = System.currentTimeMillis();
//        while ((line = is.readLine()) != null) {
//            count++;
//           // if (count % 1000 == 0) System.out.println(count + " " + line);
////            assertThat("Incorrect line at number: " + count, line, Matchers.is(unzipped.readLine()));
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("E:" + (end - start));
//        is.close();
//    }


    @Test
	public void shouldReadConcatenatedFile() throws Exception {
		MLineGzip is = new MLineGzip("test-data/my3.gz");
        final BufferedReader unzipped = new BufferedReader(new FileReader("test-data/my3.txt"));
        String line = null;
        int count =0;
		while ((line = is.readLine()) != null) {
            count++;
            assertThat("Incorrect line at number: " + count, line, Matchers.is(unzipped.readLine()));
        }
		is.close();
	}

//
//    @Test
//    public void testGZipReadLineAEG() throws Exception {
//        int lineNum = 14091;
//
//        GzipRaf gzinS = new GzipRaf("D:\\work\\logs\\Aegon\\MangledData\\AIL.BS.AE_CONTACT_INDEX_02_20140221_srv51014.log.gz");
//        MLineGzip gzinM = new MLineGzip("D:\\work\\logs\\Aegon\\MangledData\\AIL.BS.AE_CONTACT_INDEX_02_20140221_srv51014.log.gz");
//        SnapRaf sraf = new MLineSnapRAF("D:\\work\\logs\\Aegon\\MangledData\\TEMP\\test2.log.snap");
//        MLineByteBufferRAF bbraf = new MLineByteBufferRAF("D:\\work\\logs\\Aegon\\MangledData\\TEMP\\test.log") ;
//
//        String line = "";
//        int c = 1;
////        while ((line = bbraf.readLine()) != null) {
////        while ((line = sraf.readLine()) != null) {
//        String findme = "21-feb-2014 13:45:36.130 INFO: [listContactHistorie]  [OUT-RESP] ";
//         while ((line = gzinM.readLine()) != null) {
//            if (line == null) {
//                System.out.println("BOOM:" + c);
//            }
//            if (!line.startsWith("21-feb-") ) {
//                System.out.println("Readline:" + c + ": " + line);
//            }
//             if (c < 5) {
//                 System.out.println("ITEM:" + c + ": " + line);
//
//             }
//            if (line.startsWith(findme)) {
//                System.out.println("FOUND:" + c + ": " + line);
//
//            }
//             c++;
//
//        }
//        System.out.println("Lines:" + c);
////        assertEquals(5, c);
//    }


    @Test
	public void testGZipReadLine() throws Exception {

		String line = "";
		int c = 0;
		while ((line = gzin.readLine()) != null) {
			c++;
			System.out.println("Readline:" + c + ": " + line);
		}
		assertEquals(5, c);
	}
	
	@Test
	public void testShouldSeekToPosition() throws Exception {
		gzin.seek(4);
		String line = "";
		int c = 0;
		while ((line = gzin.readLine()) != null) {
			c++;
			System.out.println(c + ": " + line);
		}
		assertEquals(4, c);
	}
	
	@Test
	public void testSeekGood() throws Exception {
		gzin.seek(4);
		String oneA1 = gzin.readLine();
		String oneA2 = gzin.readLine();
		gzin.seek(4);
		String oneB1 = gzin.readLine();
		String oneB2 = gzin.readLine();
		
		assertEquals(oneA1, oneB1);
		assertEquals(oneA2, oneB2);
	}
}
