package com.liquidlabs.rawlogserver;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.rawlogserver.handler.*;
import com.liquidlabs.rawlogserver.handler.fileQueue.DefaultFileQueuerFactory;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

public class StreamingHandlerIntegTest {
	
	String rootDir = "build/StreamingHandlerIntegTest";
	
	@Test
	public void shouldPassThroughDataCorrectly() throws Exception {
		
		System.setProperty("raw.server.fq.delay.secs", "1");
		
		String day = DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis());
		File outDir = new File("build/StreamingHandlerIntegTest/hostOne/127.0.0.0_1/");
		FileUtil.deleteDir(outDir);
		
		ContentFilteringLoggingHandler cfH = new ContentFilteringLoggingHandler(new DefaultFileQueuerFactory());
		StandardLoggingHandler stdH = new StandardLoggingHandler(Executors.newScheduledThreadPool(1));
		stdH.setTimeStampingEnabled(false);
		TeeingHandler tee = new TeeingHandler(stdH, cfH);
		CharChunkingHandler nlChunker = new CharChunkingHandler(tee);
		PerAddressHandler paStreamer = new PerAddressHandler(nlChunker);
		
		paStreamer.handled("one\ntwo\nthree\n".getBytes(), "127.0.0.0_1", "hostOne", rootDir);

		// allow the queues to flush - (after 1 sec - as above)
		Thread.sleep(4000);
		
		
		assertTrue(outDir.exists());
		File ccFile = new File(outDir, "unknown-" + day + ".log");
		assertTrue(ccFile.exists());
		File rawFile = new File(outDir, "raw-" + day + ".log");
		assertTrue(rawFile.exists());
		
		
		//cc-11May09.log
		String ccFileContent = FileUtil.readAsString(ccFile.getAbsolutePath());
		String rawFileContent = FileUtil.readAsString(rawFile.getAbsolutePath());
		
		assertTrue(ccFileContent.contains("one"));
		assertTrue("CCFile:" + ccFileContent, ccFileContent.contains("three"));
		
		assertTrue(rawFileContent.contains("one"));
		assertTrue(rawFileContent.contains("three"));
		
		
	}

}
