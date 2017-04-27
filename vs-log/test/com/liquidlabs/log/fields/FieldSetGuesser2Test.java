package com.liquidlabs.log.fields;

import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.fields.field.LiteralField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;


public class FieldSetGuesser2Test {
	
	private FieldSetGuesser2 guesser;

	@Before
	public void setUp() throws Exception {
		guesser = new FieldSetGuesser2();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void shouldExtractGoodSynths() throws Exception {
		String[] lines = new String[] { 
		 "2012-06-08 09:52:24,503 INFO PF-AgentLogService-104-2 (LoggerStatsLogger)	LS_EVENT:STATS Files:1273 DwFiles:24\n",
		 "2012-06-08 09:52:24,503 INFO PF-AgentLogService-104-2 (LoggerStatsLogger)	LS_EVENT:DATA_MB Total:1907.23 Today:93.6 Yesterday:171.27\n",
		 "2012-06-08 09:52:24,503 INFO PF-AgentLogService-104-2 (LoggerStatsLogger)	LS_EVENT: tag:logscape-logs mb:675.78 items:69\n",
		 "2012-06-08 09:52:24,503 INFO PF-AgentLogService-104-2 (LoggerStatsLogger)	LS_EVENT: tag:unixApp mb:929.92 items:829\n",
		 "2012-06-08 09:52:24,503 INFO PF-AgentLogService-104-2 (LoggerStatsLogger)	LS_EVENT: tag:mitsubishi mb:7.78 items:5\n" 
		};
		FieldSet result = guesser.guessFieldSet(lines, null, null);
		System.out.println("EXPR:" + result.expression);
		
		FieldSet guessSynthFields = new SynthGuesser().guessSynthFields(result, lines);
		System.out.println("Fields:" + result.fields());
		assertNotNull("Failed to get Tag", guessSynthFields.getField("data-tag"));
		
		String test = FieldSetUtil.test(result,lines);
		System.out.println(test);
		
	}
	// CISCO MARS
//	@Test
//	public void shouldRecogniseLabeledFields() throws Exception {
//		String[] lines = new String[] { 
//				"1308851957011816000 eventid=\"1284014623658443633\" hostId=\"AAASJ-IPS\" sig_created=\"20010202\" sig_type=\"anomaly\" severity=\"lowo\" app_name=\"sensorApp\" appInstanceId=\"24890\" signature=\"2100\" subSigid=\"0\" description=\"ICMP Network Sweep w/Echo\" sig_version=\"S211\" mars_category=\"Probe/HostSweep/Non-stealth\" attacker=\"10.1.2.71111\" attacker_port=\"0\" attacker_locality=\"OUT\"  target=\"10.1.5.111\" target_port=\"0\" target_locality=\"OUT\"  target=\"10.1.5.2\" target_port=\"0\" target_locality=\"OUT\"  target=\"10.1.5.250\" target_port=\"0\" target_locality=\"OUT\"  target=\"10.14.45.3\" target_port=\"0\" target_locality=\"OUT\"  target=\"10.14.43.33\" target_port=\"0\" target_locality=\"OUT\"  target=\"10.1.5.5\" target_port=\"0\" target_locality=\"OUT\"  protocol=\"icmp\" attack_relevance_rating=\"relevant\"  risk_rating=\"60\" threat_rating=\"60\" target_value_rating=\"medium\" interface=\"ge0_1\" interface_group=\"vs0\" vlan=\"0\" protocol=\"icmp\"",
//				"1308851953722021000 eventid=\"1284007670379002128\" hostId=\"TC-IPS100\" sig_created=\"20050304\" sig_type=\"anomaly\" severity=\"lowo\" app_name=\"sensorApp\" appInstanceId=\"44200\" signature=\"1306\" subSigid=\"0\" description=\"TCP Option Other Detected\" sig_version=\"S272\" mars_category=\"Info/Misc------------------\" attacker=\"10.6.123.129\" attacker_port=\"0\" attacker_locality=\"OUT\"  target=\"0.0.0.0111\" target_port=\"0\" target_locality=\"OUT\"  protocol=\"tcp\" attack_relevance_rating=\" \"  risk_rating=\"50\" threat_rating=\"50\" target_value_rating=\"medium\" interface=\"ge2_0\" interface_group=\"vs0\" vlan=\"0\" protocol=\"tcp\"\n",
//				"1308851954056910000 eventid=\"1284007670379002132\" hostId=\"TC-IPS100\" sig_created=\"20010202\" sig_type=\"anomaly\" severity=\"info\" app_name=\"sensorApp\" appInstanceId=\"44200\" signature=\"3030\" subSigid=\"0\" description=\"TCP SYN Host Sweep DEXTTT\" sig_version=\"S211\" mars_category=\"Probe/SpecificPorts--------\" attacker=\"12.150.2.254\" attacker_port=\"0\" attacker_locality=\"OUT\"  target=\"69.9.45.37\" target_port=\"0\" target_locality=\"OUT\"  target=\"208.89.14.135\" target_port=\"0\" target_locality=\"OUT\"  target=\"216.137.41.13\" target_port=\"0\" target_locality=\"OUT\"  target=\"74.125.226.163\" target_port=\"0\" target_locality=\"OUT\"  target=\"207.178.170.231\" target_port=\"0\" target_locality=\"OUT\"  target=\"66.241.42.59\" target_port=\"0\" target_locality=\"OUT\"  target=\"98.142.82.39\" target_port=\"0\" target_locality=\"OUT\"  target=\"74.125.91.103\" target_port=\"0\" target_locality=\"OUT\"  target=\"74.200.247.187\" target_port=\"0\" target_locality=\"OUT\"  target=\"138.108.6.20\" target_port=\"0\" target_locality=\"OUT\"  target=\"66.235.143.121\" target_port=\"0\" target_locality=\"OUT\"  target=\"64.154.85.107\" target_port=\"0\" target_locality=\"OUT\"  target=\"64.136.44.18\" target_port=\"0\" target_locality=\"OUT\"  target=\"74.125.91.106\" target_port=\"0\" target_locality=\"OUT\"  target=\"74.125.226.162\" target_port=\"0\" target_locality=\"OUT\"  target=\"69.171.224.11\" target_port=\"0\" target_locality=\"OUT\"  protocol=\"tcp\" attack_relevance_rating=\"relevant\"  risk_rating=\"31\" threat_rating=\"31\" target_value_rating=\"medium\" interface=\"ge2_2\" interface_group=\"vs0\" vlan=\"0\" protocol=\"tcp\"\n"
//		};
//		
//		FieldSet result = guesser.guessFieldSet(lines, null, null);
//		System.out.println("EXPR:" + result.expression);
//		System.out.println("Fields:" + result.fields);
//		String test = result.test(lines);
//		System.out.println("TestResult:" + test);
//		assertEquals(7, result.fields.size());
//		
//		assertTrue(test.contains("success:4"));
//	}
	
	@Test
	public void shouldExtractSynthFieldWithColonSpace() throws Exception {
		String fieldValue = " some stuff: 100 and other stuff";
		List<FieldI> extracted = new SynthGuesser().guessSynthField("myField", fieldValue, ": ", new HashSet<String>(), true);
        FieldSet fieldSet = new FieldSet();
        fieldSet.fields = extracted;

        assertEquals(1, extracted.size());
		FieldI field = extracted.get(0);
		assertEquals("myField-stuff", field.name());


        ArrayList<FieldI> fields = new ArrayList<FieldI>();
        fields.add(new LiteralField("myField",1,true,true,fieldValue,"count()"));
        fieldSet.fields = fields;
        assertEquals("100", field.get(new String[]{fieldValue}, new HashSet<String>(), fieldSet));
	}
	
	@Test
	public void shouldExtractFieldLabelsWithTABDelimiter() throws Exception {
		String firstLine = "#Batch	ReportTime	ServiceCacheName	Tier	TotalPuts	TotalPutsMillis	TotalGets	TotalGetsMillis	TotalHits	TotalHitsMillis	TotalMisses	TotalMissesMillis	TotalWrites	TotalWriteMillis	TotalReads	TotalReadMillis	TotalFailures	TotalQueue	evictions	CachePrunes	CachePrunesMillis\n";
		List<String> fieldNames = guesser.getFieldNames(firstLine);
		System.out.println("Fields:" + fieldNames);
		assertEquals(21, fieldNames.size());
		
		assertEquals("Batch", fieldNames.get(0));		
	}
	
	@Test
	public void shouldExtractFieldLabelsWithCOMMADelimiter() throws Exception {
		String firstLine = "#Batch,ReportTime,ServiceCacheName,Tier,TotalPuts,TotalPutsMillis,TotalGets,TotalGetsMillis,TotalHits,TotalHitsMillis,TotalMisses,TotalMissesMillis,TotalWrites,TotalWriteMillis,TotalReads,TotalReadMillis,TotalFailures,TotalQueue,evictions,CachePrunes,CachePrunesMillis";
		List<String> fieldNames = guesser.getFieldNames(firstLine);
		System.out.println("Fields:" + fieldNames);
		assertEquals(21, fieldNames.size());
		
		assertEquals("Batch", fieldNames.get(0));		
	}
	@Test
	public void shouldExtractFieldLabelsWithCOMMASPACEDelimiter() throws Exception {
		String firstLine = "#Batch, ReportTime, ServiceCacheName, Tier, TotalPuts, TotalPutsMillis, TotalGets, TotalGetsMillis, TotalHits, TotalHitsMillis, TotalMisses, TotalMissesMillis, TotalWrites, TotalWriteMillis, TotalReads, TotalReadMillis, TotalFailures, TotalQueue, evictions, CachePrunes, CachePrunesMillis";
		List<String> fieldNames = guesser.getFieldNames(firstLine);
		System.out.println("Fields:" + fieldNames);
		assertEquals(21, fieldNames.size());
		
		assertEquals("Batch", fieldNames.get(0));		
	}
	@Test
	public void shouldExtractFieldLabelsWithPIPEDelimiter() throws Exception {
//		"2010/09/13 12:59:59|feuh|debug|tr-feuh-dc-05|req|/datapair?net=bzo&id=5e8f2545-2b48-4b65-b6cc-67f6ab9c1716&segs=A8P,S8P,B6R,I2O,C9Q,1L1GH26||0.000566|E|",
		String firstLine = "#TimeStamp|app|level|host|type|request|stuff|number|E|";
		List<String> fieldNames = guesser.getFieldNames(firstLine);
		System.out.println("Fields:" + fieldNames);
		assertEquals(9, fieldNames.size());
		
		assertEquals("TimeStamp", fieldNames.get(0));		
	}
	
	@Test
	public void shouldGetLastWordFromValue() throws Exception {
		String result2 = new SynthGuesser().getLastWordInToken("this is a word-that");
		assertEquals("that", result2);

		String result = new SynthGuesser().getLastWordInToken("this is a word&that");
		assertEquals("that", result);
		
	}
	
	private String removeHTML(String test) {
		return test.replaceAll("<b>", "").replaceAll("</b>", "");
	}
	
	@Test
	public void shouldExtractNetScreenLogs() throws Exception {
		String[] lines = new String[] {
				"Apr 24 21:08:21 127.0.0.1 ns204: NetScreen device_id=HOST_NETSCREEN system-critical-00032: Malicious URL has been detected! From AAA.BBB.CCC.DDD:3562 to AAA.BBB.CCC.EEE:80, using protocol TCP,",
				"May 18 15:59:26 192.168.10.1 ns204: NetScreen device_id=-0029012002000170 system notification-0025(traffic): start_time=\"2001-04-29 16:46:16\" duration=88 policy id=2 service=icmp proto=1 src zone=Trust dst zone=Untrust action=Tunnel(VPN_3 03) sent=102 rcvd=0 src=192.168.10.10 dst=10.10.10.1 srcPort=(1254) dstPort=(80) icmp type=8 srcXlated ip=192.168.10.10 port=1991 dstXlated ip=1.1.1.1 port=200",
				"Feb 5 19:39:42 10.1.1.1 ns25: Netscreen device_id=00351653456 system-notification-00257(traffic): start_time=\"2003-02-05 19:39:04\" duration=0 policy_id=320001 service=1434 proto=17 src zone=Untrust dst zone=Trust action=Deny sent=0 rcvd=40 ",
				"Apr 4 15:12:51 127.0.0.1 HOST_NETSCREEN: NetScreen device_id=HOST_NETSCREEN [No Name]system-notification-00257(traffic): start_time=\"2006-04-04 15:12:51\" duration=0 policy_id=320001 service=icmp proto=1 src zone=Null dst zone=self action=Deny sent=0 rcvd=28 src=AAA.BBB.CCC.DDD dst=AAA.BBB.CCC.DDD icmp type=8 session_id=0",
		};
		FieldSet result = guesser.guessFieldSet(lines, null, null);
		System.out.println("EXPR:" + result.expression);
		System.out.println("Fields:" + result.fields());
		String test = FieldSetUtil.test(result, lines);
		System.out.println("TestResult:" + test);
		assertEquals(15, result.fields().size());
		
		assertTrue(removeHTML(test).contains("Success:4"));
		
		
		// try for synthfields
		new SynthGuesser().guessSynthFields(result, lines);
		String test2 = FieldSetUtil.test(result, lines);
		System.out.println("SyNTH-TestResult:" + test2);
		
	}
	
	@Test
	public void shouldExtractIISLog() throws Exception {
//		#Fields: date time cs-ip cs-method cs-uri sc-status sc-bytes time-taken cs(Referer) cs(User-Agent) cs(Cookie)

		String[] lines = new String[] {
				"2010-11-21	09:26:03	58.48.110.165	GET	/origin-www.accuweather.com/adrequest/adrequest.asmx/getAdIPhoneCode?strAppID=iphone&stf1f4a1c4c43	200	1327	0	\"-\"	\"Mozilla/5.0%20(IPhone: U; CPU% iPhone%20OS%202_1 like Mac%20OS X; en-us) AppleWebKit/525.18.1 (KHTML, like%20Gecko) Version/3.1.1 Mobile/5F136 Safari/525.20\"	\"-\"", 
				"2010-11-21	09:26:10	220.249.103.228	GET	/origin-www.accuweather.com/adrequest/adrequest.asmx/getAdIPhoneCode?s	200	520	1	\"-\"	\"Mozilla/5.0%20(IPhone:%20U;%20CPU.1.1%20Mobile/5F136%20Safari/525.20\"	\"-\"",
				"2010-11-21	09:45:53	117.204.172.216	GET	/origin-www.accuweather.com/us-city-list.asp?zipcode=dombivli,%20india	200	12989	112	\"-\"	\"Mozilla/4.0(compatible;MSIE7.0;0729;InfoPath.2;.NETCLR3.0.30729)\"	\"-\"",
				"2010-11-21	09:41:31	121.251.216.188	GET	/origin-www.accuweather.com/NWO/WebResource.axd?d=YQDHA1&t=63402009629	200	373	0	\"http://www.accuweather.com/en-us/cn/shandong/qingdao/hourly.aspx\"	\"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.12) Gecko/20101026 AskTbFXTV5/3.9.1.14019 Firefox/3.6.12\"	\"-\""
				
		};
		FieldSet result = guesser.guessFieldSet(lines,null,null);
		System.out.println("EXPR:" + result.expression);
		System.out.println("Fields:" + result.fields());
		String test = FieldSetUtil.test(result, lines);
		System.out.println("TestResult:" + test);
		assertEquals(19, result.fields().size());
		
		assertTrue(removeHTML(test).contains("Success:4"));
		
		
		// now check synthetic fields are OK - need to extract request params
		FieldSet guessSynthFields = new SynthGuesser().guessSynthFields(result, lines);
		String test2 = FieldSetUtil.test(guessSynthFields,lines);
		System.out.println("GUESS with Synth:" + test2);
		
		
	}
	
	@Test
	public void shouldWorkWithSpaceStuff() throws Exception {
		String[] lines = new String[] { 
				"1048576.0	487565.7	74743.9	67	25	19.617	11.043	30.661	19.617	11.043	30.661	0",
				"1048576.0	487565.7	74743.9	67	25	19.617	11.043	30.661	19.617	11.043	30.661	0",
				"1048576.0	487565.7	74743.9	67	25	19.617	11.043	30.661	19.617	11.043	30.661	0",
		};
		FieldSet result = guesser.guessFieldSet(lines, null, "\t");
		assertNotNull(result);
		System.out.println("EXPR:" + result.expression);
		assertEquals(12, result.fields().size());
		
	}
	
	
	@Test
	public void shouldExtractFromRealData() throws Exception {
		
		String[] lines = new String[] { "2009-04-22 18:40:24,109 WARN main (ORMapperFactory.java:100) - Service:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234",  
				"2009-04-22 18:40:30,906 ERROR thread1 (JmxHtmlServerImpl.java:44)	CPU:99 - adaptor:type=html", 
				"2009-04-22 18:40:30,906 INFO main (JmxHtmlServerImpl.java:44)	 - adaptor:type=html\n nextLine" };
		FieldSet result = guesser.guessFieldSet(lines,null,null);
		assertNotNull(result);
		System.out.println("EXPR:" + result.expression);
//		assertEquals(3, result.fields.size());
		for (FieldI field : result.fields()) {
			System.out.println(">>" + field);
		}
		
		String test = FieldSetUtil.test(result, lines);
		System.out.println(test);
		assertTrue(removeHTML(test).contains("Success:3 Fail:0"));
	}
	
	@Test
	public void shouldGuessIPAddressAndMatch() throws Exception {
		String[] lines = new String[] { "#word ip msg","one 192.168.1.1 msg" , "two 192.168.1.2 msg" };
		FieldSet result = guesser.guess(lines);
		System.out.println("Expression:" + result.expression);
		assertTrue(result.expression.contains("(\\d+\\.\\d+\\.\\d+\\.\\d+)"));
		
		String fieldValue = result.getFieldValue("ip", new String[] { "one", "192.168.1.1", "msg" });
		assertEquals("192.168.1.1",fieldValue);
	}
	
	@Test
	public void shouldGetNumberOfItems() throws Exception {
		String[][] lines = new String[][] { "one two three".split(" ") , "four five six seven".split(" ") };
		int results = guesser.getNumberOfItems(lines);
		assertEquals(3, results);
	}
	
	@Test
	public void shouldGetSameLengthRule() throws Exception {
		String[][] lines = new String[][] { "one two 1234".split(" ") , "xxx five 1234 seven".split(" ") };
		assertTrue(guesser.isSameLength(0, lines));
		assertFalse(guesser.isSameLength(1, lines));
		assertTrue(guesser.isSameLength(2, lines));
		assertFalse(guesser.isSameLength(3, lines));
	}
	
	@Test
	public void shouldFindSpecialChars() throws Exception {
		String[][] lines = new String[][] { "one x-x 1.234".split(" ") , "xxx five 1234 seven".split(" ") };
		assertFalse(guesser.isSpecialChars(0, lines));
		assertTrue(guesser.isSpecialChars(1, lines));
		assertTrue(guesser.isSpecialChars(2, lines));
		assertTrue(guesser.isSpecialChars(3, lines));
	}

}
