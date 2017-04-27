package com.liquidlabs.syslog4vscape;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.liquidlabs.common.DateUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.productivity.java.syslog4j.Syslog;
import org.productivity.java.syslog4j.SyslogConfigIF;
import org.productivity.java.syslog4j.SyslogConstants;
import org.productivity.java.syslog4j.SyslogIF;
import org.productivity.java.syslog4j.server.SyslogServerIF;
import org.productivity.java.syslog4j.server.SyslogServerMain;
import org.productivity.java.syslog4j.server.SyslogServerMain.Options;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.file.FileUtil;

public class SysLogTest {
	
	
	private SyslogServerIF sysLogServer;

	@Before
	public void setup() {
		FileUtil.deleteDir(new File("build","user"));
		try {
			String[] argz = new String[] { "-a", "-o", "build/TEST", "-p", "2222", "tcp"};
			Options parseOptions = SyslogServerMain.parseOptions(argz);
			parseOptions.quiet = false;

			//SysLogServer.setupServiceInfo("stcp://localhost:11000", "localhost", "", "2222", "3333",".");
			sysLogServer = SysLogServer.create(argz, parseOptions);
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	@Test
	public void shouldReceiveAMessage() throws Exception {

		String print = DateUtil.shortDateTimeFormat8.print(System.currentTimeMillis());

//		Runnable runnable = new Runnable() {
//			@Override
//			public void run() {
//				try {
//					SyslogServerMain.main(new String[]{"-p", "5555", "tcp"});
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//
//			}
//		};
//
//		ExecutorService pool = Executors.newCachedThreadPool();
//		pool.submit(runnable);

		//Thread.sleep(1000);

		SyslogIF syslog = Syslog.getInstance("tcp");
		SyslogConfigIF config = syslog.getConfig();
		config.setSendLocalTimestamp(true);
		config.setSendLocalName(true);
		config.setUseStructuredData(false);

		config.setHost("localhost");
//		config.setPort(2222);
				config.setPort(1468);
		config.setFacility("MAIL");

//		String message = "boom!! Mail_facility Message Sent";
		String message = "rtkit-daemon[1942]: Successfully made thread 27601 of process 27601 (n/a) owned by '110' high priority at nice level -11.";
		syslog.warn(message);


//				-p 5555
		//Thread.sleep(1000 * 1000);

	}

	@After
	public void teardown() {
		sysLogServer.shutdown();
	}
	
	@Test
	public void shouldTryAndWriteAMessage() throws Exception {
		SyslogIF syslog = Syslog.getInstance("udp");
        syslog.getConfig().setSendLocalTimestamp(false);
        syslog.getConfig().setSendLocalName(false);
		SyslogConfigIF config = syslog.getConfig();
        config.setFacility(SyslogConstants.FACILITY_MAIL);
		config.setHost("localhost");
		config.setPort(1514);
		String message = "boom!! Mail_facility Message Sent:" + new Date();

		syslog.error(message);
        syslog.log(1, message);
        syslog.emergency(message);
        syslog.flush();
		
		Thread.sleep(4000);
		
		File[] listFiles = new File("build/TEST").listFiles();
		
		System.out.println("Found files:" + Arrays.asList(listFiles));
		assertNotNull("No files found on:build/" + NetworkUtils.getIPAddress() + " all:" + Arrays.asList(listFiles), listFiles);
		assertEquals(1, listFiles.length);
		File[] hostFiles = listFiles[0].listFiles();
		assertEquals("No HostFiles", 1, hostFiles.length);
		File[] userFile = hostFiles[0].listFiles();
		assertEquals("No userfiles", 1, userFile.length);
		String readAsString = FileUtil.readAsString(userFile[0].getAbsolutePath());
		assertTrue(readAsString.contains(message));
	}
}
