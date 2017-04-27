package com.liquidlabs.common.file;

import static junit.framework.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.liquidlabs.common.file.raf.GzipRaf;
import org.junit.Before;
import org.junit.Test;

public class GzipRafTest {
	String gzipFilename = "build/testgzip.gz";
	
	@Before
	public void setup() {
		
		try {
			GZIPOutputStream fos = new GZIPOutputStream(new FileOutputStream(gzipFilename));
			fos.write("aaa\nbbb\nccc\nddd".getBytes());
			fos.close();
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
//	@Test
//	public void shouldUseMulti() throws Exception {
//		String gzipBig = "/Volumes/Media/logs/sbet/squid-access.log.1.gz";
//		GZIPInputStream gzipIn = new GZIPInputStream(new FileInputStream(gzipBig), 4 * 1024);
//		byte[] chunker = new byte[4 * 1024];
//		int read = 0;
//		int count = 0;
//        int mod = 100 * 1000;
//        long data = 0;
//		long start = System.currentTimeMillis();
//		while ((read = gzipIn.read(chunker)) > 0) {
//            data += read;
//			int newLines = new String(chunker).split("\n").length;
//			for (int i = 0; i < newLines; i++) {
//				count++;
//				if (count > mod && count % mod == 0) {
//					System.out.println("lines:" + count);
//				}
//			}
//		}
//		long end = System.currentTimeMillis();
//        double seconds = (double)((end - start)) / 1000.0;
//        double mb = ((double)data / FileUtil.MEGABYTES);
//        double rate = mb/seconds;
//
//		System.out.println(count + " Elapsed:" + (end - start) + " RateMB/S:" + rate);
//
//	}
//	
//	@Test
//	public void shouldScanBZipFile() throws Exception {
//		String file = "/Volumes/Media/workspace_1.2/temp/sbet-prod/test.log.1.bz2";
//		MLineBzip raf = new MLineBzip(file);
//		try {
//			long start = System.currentTimeMillis();
//			int count = 0;
//			String line = "";
//			while ((line = raf.readLine()) != null) {
//				count++;
////				System.out.println(line);
//				if (count > mod && count % mod == 0) {
//					System.out.println("lines:" + count);
//				}
//			}
//			long end = System.currentTimeMillis();
//			System.out.println("Lines: " + count + " Elapsed:" + (end - start));
//		} finally {
//			if (raf != null) raf.close();
//		}
//		
//	}
//	int mod = 10 * 1000;
//	@Test
//	public void shouldScanACCESS_LOG() throws Exception {
//		String gzipBig = "/Volumes/Media/workspace_1.2/temp/sbet-prod/access.log.1.gz";
//		MLineGzip raf = new MLineGzip(gzipBig);
//		long start = System.currentTimeMillis();
////		RAF raf = RafFactory.getRaf(log.getAbsolutePath(), "");
//		try {
//			int count = 0;
//			String line = "";
//			while ((line = raf.readLine()) != null) {
//				count++;
////				System.out.println(line);
//				if (count > mod && count % mod == 0) {
//					System.out.println("lines:" + count);
//				}
//			}
//			long end = System.currentTimeMillis();
//			System.out.println("Lines: " + count + " Elapsed:" + (end - start));
//		} finally {
//			if (raf != null) raf.close();
//		}
//		
//	}
	@Test
	public void testGZipReadLine() throws Exception {
		
		GzipRaf gzin = new GzipRaf(gzipFilename);
		String line = "";
		int c = 0;
		while ((line = gzin.readLine()) != null) {
			c++;
			System.out.println(c + ": " + line);
		}
		assertEquals(4, c);
	}
	
	@Test
	public void testShouldSeekToPosition() throws Exception {
		GzipRaf gzin = new GzipRaf(gzipFilename);
		gzin.seek(4);
		String line = "";
		int c = 0;
		while ((line = gzin.readLine()) != null) {
			c++;
			System.out.println(c + ": " + line);
		}
		assertEquals(3, c);
	}
	
	@Test
	public void testSeekGood() throws Exception {
		GzipRaf gzin = new GzipRaf(gzipFilename);
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
