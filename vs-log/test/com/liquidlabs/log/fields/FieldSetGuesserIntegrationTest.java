package com.liquidlabs.log.fields;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FieldSetGuesserIntegrationTest {
	FieldSetGuesser2 guesser;

	@Before
	public void setup(){
		System.setProperty("kv.index","build/TEST_INT_TEST");
		guesser = new FieldSetGuesser2();

	}
	
	@Test
	public void shouldExtractThisCoherenceDataCorrectlyWPipes() throws Exception {
		
		String[] lines = new String[] {
				"#BatchCounter|tReportTime|RefreshTime|ServiceName|NodeId|RemoteAddress",
				"4|Wed Apr 20 10:00:10 BST 2011|Wed Apr 20 10:00:10 BST 2011|ExtendTcpProxyService|12|10.13.22.92	49611	0	0	0x0000012F6853EFD20A0D2E0D76AEC3757F4BEF139BD55B9AC3E85C985F16C925	1.4412322883E10	6.3003837E7	1763783.0	59110.0",
				"4|Wed Apr 20 10:00:10 BST 2011|Wed Apr 20 10:00:10 BST 2011|ExtendTcpProxyService|12|10.13.22.92	49611	0	0	0x0000012F6853EFD20A0D2E0D76AEC3757F4BEF139BD55B9AC3E85C985F16C925	1.4412322883E10	6.3003837E7	1763783.0	59110.0"
		};
			
		FieldSet fieldSet = guesser.guess(lines);
		String test = FieldSetUtil.test(fieldSet,lines);
		assertTrue(test.contains("<b>Success:</b>2"));

	}

	@Test
	public void shouldExtractThisCoherenceDataCorrectlyWTabbs() throws Exception {
		
		String[] lines = new String[] {
				"#BatchCounter\\tReportTime\\tRefreshTime\\tServiceName\\tNodeId\\tRemoteAddress\\tRemotePort\\tOutgoingByteBacklog\\tOutgoingMessageBacklog\\tUID\\tBytesSent\\tBytesRecieved\\tMessagesSent\\tMessagesRecieved",
				"4	Wed Apr 20 10:00:10 BST 2011	Wed Apr 20 10:00:10 BST 2011	ExtendTcpProxyService	12	10.13.22.92	49611	0	0	0x0000012F6853EFD20A0D2E0D76AEC3757F4BEF139BD55B9AC3E85C985F16C925	1.4412322883E10	6.3003837E7	1763783.0	59110.0",
				"4	Wed Apr 20 10:00:10 BST 2011	Wed Apr 20 10:00:10 BST 2011	ExtendTcpProxyService	12	10.13.22.92	49611	0	0	0x0000012F6853EFD20A0D2E0D76AEC3757F4BEF139BD55B9AC3E85C985F16C925	1.4412322883E10	6.3003837E7	1763783.0	59110.0"
		};
			
		FieldSet fieldSet = guesser.guess(lines);
		String test = FieldSetUtil.test(fieldSet,lines);
		assertTrue(test.contains("<b>Success:</b>2"));

	}

	@Test
	public void shouldExtractThisCoherenceDataCorrectly() throws Exception {
		
		String[] lines = new String[] {
				"#BatchCounter	ReportTime	RefreshTime	ServiceName	NodeId	RemoteAddress	RemotePort	OutgoingByteBacklog	OutgoingMessageBacklog	UID	BytesSent	BytesRecieved	MessagesSent	MessagesRecieved",
				"4	Wed Apr 20 10:00:10 BST 2011	Wed Apr 20 10:00:10 BST 2011	ExtendTcpProxyService	12	10.13.22.92	49611	0	0	0x0000012F6853EFD20A0D2E0D76AEC3757F4BEF139BD55B9AC3E85C985F16C925	1.4412322883E10	6.3003837E7	1763783.0	59110.0",
				"4	Wed Apr 20 10:00:10 BST 2011	Wed Apr 20 10:00:10 BST 2011	ExtendTcpProxyService	12	10.13.22.92	49611	0	0	0x0000012F6853EFD20A0D2E0D76AEC3757F4BEF139BD55B9AC3E85C985F16C925	1.4412322883E10	6.3003837E7	1763783.0	59110.0"
		};
			
		FieldSet fieldSet = guesser.guess(lines);
        String test = FieldSetUtil.test(fieldSet,lines);
		assertTrue(test.contains("<b>Success:</b>2"));

	}
	
	
	@Test
	public void shouldExtractIIS() throws Exception {
		String[] lines = new String[] {
				"",
				"#date time cs-ip cs-method cs-uri sc-status sc-bytes time-taken cs_ref cs_agent cs_cookie",
				"2010-11-21	09:26:03	58.48.110.165	GET	/origin-www.accuweather.com/adrequest/adrequest.asmx/getAdIPhoneCode?strAppID=iphone&strPartnerCode=iphonecurr&strIPAddress=59.71.189.55&strUserAgent=Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20&strCurrentZipCode=ASI%7CCN%7CCH013%7CWuhan&strWeatherIcon=11&strUUID=6ab4597d2ce494deadbeef4b2f164f1f4a1c4c43	200	1327	0	\"-\"	\"Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20\"	\"-\"",
				"2010-11-21	09:26:03	58.48.110.165	GET	/origin-www.accuweather.com/adrequest/adrequest.asmx/getAdIPhoneCode?strAppID=iphone&strPartnerCode=iphonecurr&strIPAddress=59.71.189.55&strUserAgent=Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20&strCurrentZipCode=ASI%7CCN%7CCH013%7CWuhan&strWeatherIcon=11&strUUID=6ab4597d2ce494deadbeef4b2f164f1f4a1c4c43	200	1327	0	\"-\"	\"Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20\"	\"-\"",
		};
		FieldSet fieldSet = guesser.guess(lines);
        String test = FieldSetUtil.test(fieldSet,lines);
		System.out.println(test);
		assertTrue(test.contains("<b>Success:</b>2"));
		assertEquals("Should Have Fields", 10 + FieldSet.DEF_FIELDS.values().length, fieldSet.fields().size());

	}
	
	
	@Test
	public void shouldDoNetStatoutput() throws Exception {
		String[] lines = getNetStatLines();
		FieldSet fieldSet = guesser.guess(lines);

        String test = FieldSetUtil.test(fieldSet,lines);
		System.out.println(test);
		assertTrue(test.contains("<b>Success:</b>4"));
		assertEquals("Should Have Fields", 4 + FieldSet.DEF_FIELDS.values().length, fieldSet.fields().size());
	}
	

	@Test
	public void log4JShouldDoIt() throws Exception {
		String[] lines = getLog4jLines();
		FieldSet fieldSet = guesser.guess(lines);
		new SynthGuesser().guessSynthFields(fieldSet, lines);

        String test = FieldSetUtil.test(fieldSet,lines);
		System.out.println(test);
		
		assertEquals("Should Have Fields: got:" + fieldSet.fields().size(), 10 + FieldSet.DEF_FIELDS.values().length, fieldSet.fields().size());
		assertTrue(test.contains("<b>Success:</b>4"));
		assertTrue("didnt get CPU synth field", test.contains("CPU\t="));
	}
	
	@Test
	public void shouldDoCoherenceMEMUSE() throws Exception {
		List<String> linesL = FileUtil.readLines("test-data/logexample/coherence-3.6/2010121613-memory-status.txt", 5);
		FieldSet fieldSet = guesser.guess(Arrays.toStringArray(linesL));
		
		assertEquals("Should Have Fields", 15, fieldSet.fields().size());
		String test = FieldSetUtil.test(fieldSet,Arrays.toStringArray(linesL));
		System.out.println(test);
		assertTrue(test.contains("<b>Success:</b>9"));
	}
	@Test
	public void shouldDoCoherenceCACHEUSE() throws Exception {
		List<String> linesL = FileUtil.readLines("test-data/logexample/coherence-3.6/2010121613-cache-usage.txt", 5);
		FieldSetGuesser2 guesser = new FieldSetGuesser2();
		
		FieldSet fieldSet = guesser.guess(Arrays.toStringArray(linesL));
		
		assertEquals("Should Have Fields", 22, fieldSet.fields().size());
		String test = FieldSetUtil.test(fieldSet,Arrays.toStringArray(linesL));
		System.out.println(test);
		assertTrue(test.contains("<b>Success:</b>9"));
	}

	/**
	 * =====================================
	 * 
	 * Data Section
	 * 
	 * =====================================
	 */
	
	private String[] getNetStatLines() {
		return new String[] {
				"#  Proto  LocalAddress          ForeignAddress        State", 
				"1  TCP    127.0.0.1:1468         envy14:62608           ESTABLISHED", 
				"2  TCP    127.0.0.1:5354         envy14:49173           ESTABLISHED", 
				"3  TCP    127.0.0.1:8080         envy14:63881           ESTABLISHED", 
				"4  TCP    127.0.0.1:27015        envy14:49171           ESTABLISHED"
		};
	}

	
	private String[]  getLog4jLines() {
		String[] results = new String[] {
				"#date time level thread package msg",
				"2009-04-22 18:40:24,109 WARN main (ORMapperFactory) - Service:WorkAllocator available on:stcp://hal2:11000/WorkAllocator_startTime=1240422023234", 
				"2009-04-22 18:40:30,906 ERROR thread1 (JmxHtmlServerImpl)	 - CPU:99 adaptor:type=html",
				"2009-04-22 18:40:30,906 INFO main (JmxHtmlServerImpl)	 - CPU:99 adaptor:type=html", 
				"2010-09-04 11:56:23,422 WARN main (vso.SpaceServiceImpl)	 LogSpaceBoot All Available Addresses:[ServiceInfo name[AdminSpace] zone[stcp://192.168.0.2:11013?serviceName=AdminSpace&?" 
		};
		
		
		return results;
	}

}

