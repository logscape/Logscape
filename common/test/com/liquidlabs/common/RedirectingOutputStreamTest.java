package com.liquidlabs.common;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import com.liquidlabs.common.RedirectingFileOutputStream.FilenameGetter;
import com.liquidlabs.common.file.FileUtil;

public class RedirectingOutputStreamTest {
	
	String outFile = "build/testRedirOne.log";
	String outFile2 = "build/testRedirTwo.log";
	String theFile = outFile;

	@Test
	public void shouldWriteStuffOut() throws Exception {
		new File("build").mkdir();
		FilenameGetter getter = new FilenameGetter() {
			public String getFilename() {
				return theFile;
			}
		};
		new File(outFile).delete();
		new File(outFile2).delete();
		
		assertFalse(new File(outFile).exists());
		assertFalse(new File(outFile2).exists());
		
		RedirectingFileOutputStream os = new RedirectingFileOutputStream(getter);


		os.write("one\n".getBytes());
		os.flush();

		assertTrue(new File(outFile).exists());
		assertFalse(new File(outFile2).exists());
		
		theFile = outFile2;
		os.checkOutname();
		os.write("two\n".getBytes());
		os.flush();
		assertTrue(new File(outFile).exists());
		assertTrue(new File(outFile2).exists());
		
		String content1 = FileUtil.readAsString(outFile);
		String content2 = FileUtil.readAsString(outFile2);
		
		assertEquals("one\n", content1);
		assertEquals("two\n", content2);
	}
}
