package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.rawlogserver.handler.ContentFilteringLoggingHandler.TypeMatcher;
import com.liquidlabs.rawlogserver.handler.fileQueue.DefaultFileQueuer;
import com.liquidlabs.rawlogserver.handler.fileQueue.DefaultFileQueuerFactory;
import com.liquidlabs.rawlogserver.handler.fileQueue.FileQueuer;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClientContentHandlerTest {
	
	
	
	private ClientContentHandler handler;
	private ContentFilteringLoggingHandler parent;
	private String rootDir = "build/ClientContentHandlerTest";
    private String sourceHost = "host";

    @Before
	public void setup() {
		FileUtil.deleteDir(new File("build/ClientContentHandlerTest"));
		List<String> lineData = null;
		try {
			lineData = FileUtil.readLines("mapping.csv", 100);
		} catch (IOException e) {
			e.printStackTrace();
		}
		parent = new ContentFilteringLoggingHandler(new DefaultFileQueuerFactory());
		List<TypeMatcher> matchers = parent.populate(lineData, new ArrayList<TypeMatcher>());
		
		handler = new ClientContentHandler("HOST", matchers , Executors.newScheduledThreadPool(1), new DefaultFileQueuerFactory());
		parent.start();
	}
	
	@Test
	public void shouldHandleMixedContent() throws Exception {
		
		handler.handleContent(sourceHost, ossecMsg, parent, rootDir);
		handler.handleContent(sourceHost, aSAMsg_ONE, parent, rootDir);
		handler.handleContent(sourceHost, ossecMsg2, parent, rootDir);
		handler.handleContent(sourceHost, aSAMsg_TWO, parent, rootDir);
		Map<String, FileQueuer> fileQueuers = handler.fileQueue;
		for (FileQueuer queue : fileQueuers.values()) {
			queue.flush();
		}
		// 2 ASA files and 1 OSSEC file
		assertEquals("Files:" + fileQueuers, 3, fileQueuers.size());
	}
	
	@Test
	public void shouldPickupWMIType() throws Exception {
		handler.handleContent(sourceHost, wmiLog1, parent, rootDir);
	}
	
	
	@Test
	public void shouldHandleContentsFromMLineType() throws Exception {

		handler.handleContent(sourceHost, ossecMsg, parent, rootDir);
		handler.handleContent(sourceHost, ossecMsg2, parent, rootDir);
		
		Map<String, FileQueuer> fileQueue = handler.fileQueue;
		
		assertEquals("Qs:" + fileQueue.values(), 1, fileQueue.size());
		String next = fileQueue.keySet().iterator().next();
		System.out.println("KEY:" + next);
		assertTrue("Got:" + next, next.contains("OSSEC"));
		assertTrue("GOT:" + next, next.startsWith("/build/ClientContentHandlerTest/OSSEC-alert"));
		
		FileQueuer fileQueuer = fileQueue.values().iterator().next();
		fileQueuer.flush();

// cannot work because its using an absolute path without perms
//		assertTrue("FileNotFound:", new File(((DefaultFileQueuer)fileQueuer).destinationFile).exists());
//		String fileContent = FileUtil.readAsString(((DefaultFileQueuer)fileQueuer).destinationFile);
//		System.out.println("File Was:[" + fileContent + "]");
//		assertTrue(fileContent.contains("Windows Logon Success"));
//		assertTrue(fileContent.contains("'Windows User Logoff."));
		
	}
//
//	@Test
//	public void shouldWriteContentsOfSingleLineKnownType() throws Exception {
//		handler.handleContent(sourceHost, aSAMsg_ONE, parent, rootDir);
//		Map<String, FileQueuer> fileQueue = handler.fileQueue;
//		assertEquals(1, fileQueue.size());
//		String next = fileQueue.keySet().iterator().next();
//		System.out.println("KEY:" + next);
//		assertTrue(next.contains("ASA"));
//		assertTrue("got next:" + next, next.startsWith("build/ClientContentHandlerTest/HOST/ASA-session/192.168.111.111"));
//	}

	@Test
	public void shouldExtractGoodPathWhenRecognised() throws Exception {
		String root = handler.mkRootDirString("root", new String[0], "hostName/hostIp");
		assertEquals("/root/", root);
	}
	
	String wmiLog1 = "20110926235930.000000";
	String wmiLog2 = "Category=12810";
	
	String syslogMsg = "Apr 25 16:25:01 ubuntu-02 CRON[1771]: (root) CMD (command -v debian-sa1 > /dev/null && debian-sa1 1 1)";
	String syslogMsg2 = "Apr 25 17:11:10 ubuntu-02 avahi-daemon[766]: Invalid query packet.";
	
	String aSAMsg_ONE = "18-Feb-11 11:19:34 Feb 18 11:20:21 192.168.111.111 :Feb 18 10:49:41 EST: %ASA-session-6-302021: Teardown ICMP connection for faddr 172.16.100.162/0 gaddr 192.168.2.112/10738 laddr 192.168.2.112/10738";
	String aSAMsg_TWO = "18-Feb-11 11:19:34 Feb 18 11:20:21 192.168.222.222 :Feb 18 10:49:41 EST: %ASA-session-6-302021: Teardown ICMP connection for faddr 172.16.100.162/0 gaddr 192.168.2.112/10738 laddr 192.168.2.112/10738";

	String ossecMsg = "** Alert 1300400980.1557486: - windows,authentication_success,\n" + 
	"2011 Mar 17 18:29:40 (CLONWWMAIL1) 192.168.3.100->WinEvtLog\n" + 
	"Rule: 18107 (level 3) -> 'Windows Logon Success.'\n" + 
	"Src IP: (none)\n" + 
	"User: tnianios\n" + 
	"WinEvtLog: Security: AUDIT_SUCCESS(4624): Microsoft-Windows-Security-Auditing: tnianios: CLONE-SYSTEMS: CLONWWMAIL1.clone-systems.com: An account was successfully logged on. Subject:  Security ID:  S-1-0-0  Account Name:  -  Account Doma$\n\n";
	
	String ossecMsg2 = "** Alert 1300400980.1558608: - windows,\n" + 
	"2011 Mar 17 18:29:40 (CLONWWMAIL1) 192.168.3.100->WinEvtLog\n" + 
	"Rule: 18149 (level 3) -> 'Windows User Logoff.'\n" + 
	"Src IP: (none)\n" + 
	"User: tnianios\n" + 
	"WinEvtLog: Security: AUDIT_SUCCESS(4634): Microsoft-Windows-Security-Auditing: tnianios: CLONE-SYSTEMS: CLONWWMAIL1.clone-systems.com: An account was logged off. Subject:  Security ID:  S-1-5-21-2650515781-1991746861-2225058118-1126  Acc$\n\n";


}
