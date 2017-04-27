package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.rawlogserver.handler.ContentFilteringLoggingHandler.TypeMatcher;
import com.liquidlabs.rawlogserver.handler.fileQueue.DefaultFileQueuerFactory;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ContentFilteringLoggingHandlerTest {
	
	private ContentFilteringLoggingHandler handler;

	@Before
	public void setup() {
		handler = new ContentFilteringLoggingHandler(new DefaultFileQueuerFactory());
		System.setProperty("raw.server.fq.delay.secs", "1");
	}
	
	
	@Test
	public void shouldGoFast() throws Exception {
		
		FileUtil.deleteDir(new File("build/" + getClass().getSimpleName()));
		handler.matchers.add(new TypeMatcher("MSWinEventLog,timestamp=true,mline=false,appendNL=false,,serverGroup=1,(*) AND MSWinEventLog\\s+*\\s+(*)"));
		handler.matchers.add(new TypeMatcher("ASA-session,timestamp=true,mline=false,appendNL=false,,serverGroup=1,(\\d+.\\d+.\\d+.\\d+) AND ASA-session"));
		handler.matchers.add(new TypeMatcher("ASA-vpn,timestamp=true,mline=false,appendNL=false,,serverGroup=1,(\\d+.\\d+.\\d+.\\d+) AND ASA-vpn" ));
		handler.matchers.add(new TypeMatcher("ASA-ip,timestamp=true,mline=false,appendNL=false,,serverGroup=1,(\\d+.\\d+.\\d+.\\d+) AND ASA-ip"));
		handler.matchers.add(new TypeMatcher("OSSEC-alert,timestamp=false,mline=true,appendNL=true,,serverGroup=-1,** Alert"));

		handler.matchers.add(new TypeMatcher("MSWinWMILog,	timestamp=false,mline=true,appendNL=\n_EOL_\n,postProcessFileName=.*?ComputerName=(\\S+).*Logfile=(\\S+).*,serverGroup=-1,20[1-9][1-9][0-1][0-9][0-3][0-9][0-2][0-3][0-5][0-9][0-5][0-9]\\.000000"));

		long start = DateTimeUtils.currentTimeMillis();
		int count = 0;

//		for (int i = 0; i < Integer.MAX_VALUE; i++) {
			RAF raf = RafFactory.getRafSingleLine("test-data/mixed-logs.log");
			String line = "";
			while ((line = raf.readLine()) != null) {
				handler.handled(line.getBytes(), "addr_port", "hostname", "build/" + getClass().getSimpleName());
				count++;
			}
			raf.close();
//		}

		long end = DateTimeUtils.currentTimeMillis();
		System.out.println(count + " Elapsed:" + (end - start));
		Thread.sleep(2000);
	}

	@Test
	public void miniIntegrationTest() throws Exception {

		if (new File("build").exists()) FileUtil.copyFile(new File("mapping.csv"), new File("build/mapping.csv"));
		if (new File("bin").exists()) FileUtil.copyFile(new File("mapping.csv"), new File("bin/mapping.csv"));
		List<String> mapping = handler.loadMappingContent();
		assertNotNull("Failed to find mapping on the classpath", mapping);
		handler.populate(mapping, handler.matchers);

		// test - check the mapping was loaded and maps properly
		assertEquals(22, handler.matchers.size());
		TypeMatcher type = handler.getTypeMatcher("18-Feb-11 11:19:34 Feb 18 11:20:21 192.168.3.1 :Feb 18 10:49:41 EST: %ASA-session-6-302021: Teardown ICMP connection for faddr 172.16.100.162/0 gaddr 192.168.2.112/10738 laddr 192.168.2.112/10738");
		assertEquals("ASA-session", type.type);

		TypeMatcher type2 = handler.getTypeMatcher("CLONWWADDC1.clone-systems.com	MSWinEventLog	1	Security	59048	Sun Feb 20 16:42:27 2011	4624	Microsoft-Windows-Security-Auditing	CLONWWADDC1$	N/A	Success Audit	CLONWWADDC1.clone-systems.com	None		An account was successfully logged on.    Subject:   Security ID:  S-1-0-0   Account Name:  -   Account Domain:  -   Logon ID:  0x0    Logon Type:   3    New Logon:   Security ID:  S-1-5-18   Account Name:  CLONWWADDC1$   Account Domain:  CLONE-SYSTEMS   Logon ID:  0x12d1a01   Logon GUID:  {A04306B6-0CE3-E26E-A3D9-B36ECE532D11}    Process Information:   Process ID:  0x0   Process Name:  -    Network Information:   Workstation Name:    Source Network Address: fe80::e0b3:f2c4:a963:2c09   Source Port:  65308    Detailed Authentication Information:   Logon Process:  Kerberos   Authentication Package: Kerberos   Transited Services: -   Package Name (NTLM only): -   Key Length:  0    This event is generated when a logon session is created. It is generated on the computer that was accessed.    The subject fields indicate the account on the local system which requested the logon. This is most commonly a service such as the Server service, or a local process such as Winlogon.exe or Services.exe.    The logon type field indicates the kind of logon that occurred. The most common types are 2 (interactive) and 3 (network).    The New Logon fields indicate the account for whom the new logon was created, i.e. the account that was logged on.    The network fields indicate where a remote logon request originated. Workstation name is not always available and may be left blank in some cases.    The authentication information fields provide detailed information about this specific logon request.   - Logon GUID is a unique identifier that can be used to correlate this event with a KDC event.   - Transited services indicate which intermediate services have participated in this logon request.   - Package name indicates which sub-protocol was used among the NTLM protocols.   - Key length indicates the length of the generated session key. This wi");
		assertEquals("MSWinEventLog", type2.type);

		TypeMatcher type3 = handler.getTypeMatcher("Audit:[timestamp=03-23-2011 23:25:24.892, user=n/a, action=search, info=completed, search_id='scheduler__nobody__search_VG9wIGZpdmUgc291cmNldHlwZXM_at_1300937100_427deba94505fbfa', total_run_time=0.14, event_count=0, result_count=0, available_count=0, scan_count=0, drop_count=0, exec_time=1300937124, api_et=1300850700.000000000, api_lt=1300937100.000000000, search_et=1300850700.000000000, search_lt=1300937100.000000000, is_realtime=0, savedsearch_name=\"Top five sourcetypes\"][n/a]\r\n");
		assertEquals("Audit", type3.type);

		new File("build/mapping.csv").delete();
		new File("bin/mapping.csv").delete();
	}

	@Test
	public void shouldLoadTextDataCorrectly() throws Exception {
		List<String> textFileContent = new ArrayList<String>();
		textFileContent.add("MSWinEventLog,timestamp=true, mline=false,appendNL=false,,serverGroup=1, (*) AND MSWinEventLog\\\\s+*\\\\s+(*)");
		textFileContent.add("ASA-session,timestamp=true, mline=false,appendNL=false,,serverGroup=1, (\\d+.\\d+.\\d+.\\d+) AND ASA-session");
		textFileContent.add("ASA-vpn,  timestamp=true, mline=false,appendNL=false,,serverGroup=1, (\\d+.\\d+.\\d+.\\d+) AND ASA-vpn");
		textFileContent.add("ASA-ip, timestamp=true, mline=false,appendNL=false,,serverGroup=1,  (\\d+.\\d+.\\d+.\\d+) AND ASA-ip");
		handler.populate(textFileContent, handler.matchers);


		assertEquals(4, handler.matchers.size());
		TypeMatcher type = handler.getTypeMatcher("18-Feb-11 11:19:34 Feb 18 11:20:21 192.168.3.1 :Feb 18 10:49:41 EST: %ASA-session-6-302021: Teardown ICMP connection for faddr 172.16.100.162/0 gaddr 192.168.2.112/10738 laddr 192.168.2.112/10738");
		assertEquals("ASA-session", type.type);
	}


	@Test
	public void shouldExtractMultipleLevelsFromContentWhenMatching() throws Exception {
		String logLine = " CLONWWADDC1.clone-systems.com	MSWinEventLog	1	Security	20282	Fri Feb 18 00:00:27 2011	4634	Microsoft-Windows-Security-Auditing	CLONWWADDC1$	N/A	Success Audit	CLONWWADDC1.clone-systems.com	None		An account was logged off.    Subject:   Security ID:  S-1-5-18   Account Name:  CLONWWADDC1$   Account Domain:  CLONE-SYSTEMS   Logon ID:  0xc8f7b7    Logon Type:   3    This event is generated when a logon session is destroyed. It may be positively correlated with a logon event using the Logon ID value. Logon IDs are only unique between reboots on the same computer.	20120 ";
		handler.matchers.add(new TypeMatcher("MSWinEventLog,timestamp=true,mline=false,appendNL=false,,serverGroup=1, (*) AND MSWinEventLog\\s+*\\s+(*)"));
		TypeMatcher type = handler.getTypeMatcher(logLine);
		assertEquals("_MSWinEventLog_", type.parts[0]);
		assertEquals("_SERVER_/CLONWWADDC1.clone-systems.com", type.parts[1]);
		assertEquals("Security", type.parts[2]);
	}

    @Test
    public void shouldMatchCollectd() throws Exception {
        String line = "QA-UK-MG-UB0 .cpu-1.cpu-interrupt.value 303417 1416476220";
        String pattern = "(\\S+) \\.(\\S+)\\.\\S+.\\S+ \\S+ \\d+";
        boolean matches = line.matches(pattern);

        String collectd = "collectd,       timestamp=false,mline=false,appendNL=false,,serverGroup=1,(\\S+) \\.(\\S+)\\.\\S+.\\S+ \\S+ \\d+";
        handler.matchers.add(new TypeMatcher(collectd));
        TypeMatcher type = handler.getTypeMatcher(line);
        assertEquals("_collectd_", type.parts[0]);

    }

	@Test
	public void shouldMatchThisType() throws Exception {
		String line = "Jun 17 06:02:32 172.16.218.248 2011 Jun 17 11:14:34 store118.qchek <50000> Dropped Inbound packet (Custom rule) Src:172.16.218.20 SPort:138 Dst:172.16.218.255 DPort:138 IPP:17 Rule:14 Interface:WAN (Internet)";
		handler.matchers.add(new TypeMatcher("FWall,		timestamp=true,mline=false,appendNL=false,,serverGroup=1,(\\d+\\.\\d+\\.\\d+\\.\\d+) AND \\s+\\S+\\.\\S+ <\\d+>"));
		TypeMatcher type = handler.getTypeMatcher(line);
		assertEquals("_FWall_", type.parts[0]);
		assertEquals("_SERVER_/172.16.218.248", type.parts[1]);
//		assertEquals("store118.qchek", type.parts[2]);

	}
	@Test
	public void shouldMatchThisSynmantecServer() throws Exception {
		String line = "String line = \"Jun 20 23:02:57 10.3.32.56 Jun 21 00:00:00 SymantecServer ANTIVIRUS1: Site: ANTIVIRUS1,Server: ANTIVIRUS1,Domain: Default,0,z025904,,\\r\\n\";";
		handler.matchers.add(new TypeMatcher("SYMANTEC,		timestamp=false,mline=false,appendNL=false,,serverGroup=1,SymantecServer (*): Site"));
		TypeMatcher type = handler.getTypeMatcher(line);
		assertEquals("_SYMANTEC_", type.parts[0]);
		assertEquals("_SERVER_/ANTIVIRUS1", type.parts[1]);
	}
	@Test
	public void shouldMatchThisFWSM() throws Exception {
		String line = "Jun 20 07:33:15 10.2.243.10 Jun 20 2011 08:31:20: %FWSM-5-304001: mbeatti@10.6.111.116 Accessed URL 98.27.88.8:http://www.bing.com/sa/0812172936/SerpNotifications_c.js\r\n";
		handler.matchers.add(new TypeMatcher("FWSM,		timestamp=true,mline=false,appendNL=false,,serverGroup=1,(\\d+\\.\\d+\\.\\d+\\.\\d+) AND FWSM-\\d+-\\d+"));
		TypeMatcher type = handler.getTypeMatcher(line);
		assertEquals("_FWSM_", type.parts[0]);
		assertEquals("_SERVER_/10.2.243.10", type.parts[1]);
	}
	@Test
	public void shouldExtractMultipleLevelsFromContentWhenMatchingWindowsEvtLog() throws Exception {
		String logLine = "somerubbish CLONWWADDC1.clone-systems.com	MSWinEventLog	1	Security	20282	Fri Feb 18 00:00:27 2011	4634	Microsoft-Windows-Security-Auditing	CLONWWADDC1$	N/A	Success Audit	CLONWWADDC1.clone-systems.com	None		An account was logged off.    Subject:   Security ID:  S-1-5-18   Account Name:  CLONWWADDC1$   Account Domain:  CLONE-SYSTEMS   Logon ID:  0xc8f7b7    Logon Type:   3    This event is generated when a logon session is destroyed. It may be positively correlated with a logon event using the Logon ID value. Logon IDs are only unique between reboots on the same computer.	20120 ";
		handler.matchers.add(new TypeMatcher("MSWinEventLog,timestamp=true,mline=false,appendNL=false,,serverGroup=1, (*)\\s+MSWinEventLog\\s+*\\s+(*)"));
		TypeMatcher type = handler.getTypeMatcher(logLine);
		assertEquals("_MSWinEventLog_", type.parts[0]);
		assertEquals("_SERVER_/CLONWWADDC1.clone-systems.com", type.parts[1]);
		assertEquals("Security", type.parts[2]);
	}

	@Test
	public void shouldExtractFilePathFromContentWithMapping() throws Exception {
		String logLine = "18-Feb-11 11:19:34 Feb 18 11:20:21 192.168.3.1 :Feb 18 10:49:41 EST: %ASA-session-6-302021: Teardown ICMP connection for faddr 172.16.100.162/0 gaddr 192.168.2.112/10738 laddr 192.168.2.112/10738";
		System.out.println("Match:" + logLine.matches(".*?(\\d+.\\d+.\\d+.\\d+).*ASA-session.*"));
		handler.matchers.add(new TypeMatcher("ASA-session,timestamp=true,mline=false,appendNL=false,,serverGroup=1,(\\d+.\\d+.\\d+.\\d+) AND ASA-session"));
		TypeMatcher type = handler.getTypeMatcher(logLine);
		assertNotNull(type);
		assertEquals(2, type.parts.length);
		assertEquals("_ASA-session_", type.parts[0]);
		assertEquals("_SERVER_/192.168.3.1", type.parts[1]);
	}

	@Test
	public void shouldExtractFilePathWhereNoGroupsSpecified() throws Exception {
		String logLine = "Server: CLONWLLOGC1, ID: 000, Name: CLONWLLOGC1 (server), IP: 127.0.0.1, Active/Local";
		handler.matchers.add(new TypeMatcher("OSSEC-alert,	timestamp=false,mline=true,appendNL=false,,serverGroup=-1,Server: AND ID:  AND Name: AND IP:"));
		TypeMatcher type = handler.getTypeMatcher(logLine);
		assertNotNull("Should have matched", type);
		assertEquals("OSSEC-alert", type.type);
		assertEquals("_OSSEC-alert_", type.parts[0]);
	}

    @Test
    public void shouldExtractASAWithGroup() throws Exception {
        String logLine = "Nov 11 06:54:58 Nov 11 2014 06:47:11 EXCELIAN-F01-LONDON : %ASA-6-305011: Built dynamic TCP translation from EXCELIAN-INSIDE:10.28.0.73/53063 to EXCELIAN-OUTSIDE:217.20.25.200/53063";
        handler.matchers.add(new TypeMatcher("ASA,	timestamp=false,mline=false,appendNL=false,,serverGroup=1,(**) : %ASA-\\d+"));
        TypeMatcher type = handler.getTypeMatcher(logLine);
        assertNotNull("Should have matched", type);
        assertEquals("ASA", type.type);
//        assertEquals("OSSEC-alert", type.parts[0]);
    }
}


