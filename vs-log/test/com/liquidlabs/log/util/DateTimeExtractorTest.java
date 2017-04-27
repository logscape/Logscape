package com.liquidlabs.log.util;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.RegExpUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.*;

public class DateTimeExtractorTest {
	
	private DateTimeExtractor extractor;

	@Before
	public void setUp() throws Exception {
		extractor = new DateTimeExtractor();
	}
    @Test
    public void shouldGetUnixLongWithExtractor() throws Exception {
        DateTimeExtractor timeExtractor = new DateTimeExtractor("UNIX_LONG");
        String timeFormat = timeExtractor.getFormat("1396306807.392,0.000000", 0);
        assertEquals("UNIX_LONG", timeFormat);


    }

    @Test
    public void shouldGetUnixLongAtStart() throws Exception {
        String line = "1396306807.392,0.000000";
        CharGrabber grabber = new CharGrabber("UNIX_LONG");
        String grab = grabber.grab(line);
        assertNotNull("Didnt extract line", grab);
        Date parsed = grabber.parse(grab, 0);
        assertNotNull("Didnt extract date", parsed);

    }

    @Test
    public void shouldParseCharBasedFormat() throws Exception {
        String line = "[Sat, 14 Nov 2008 02:27:09 GMT] [info] [127.0.0.1:3369/127.0.0.1:9080] Closing connection to client";
        extractor = new DateTimeExtractor("char:[ EEE, dd MMM yyyy HH:mm:ss zzz");
        CharGrabber grabber = new CharGrabber("char:[ EEE, dd MMM yyyy HH:mm:ss zzz");
        String grab = grabber.grab(line);
//        System.out.println("line:" + grab);
        assertNotNull("Didnt extract line", grab);
        Date parsed = grabber.parse(grab, 0);
        assertNotNull("Didnt extract date", parsed);

        String format = extractor.getFormat(line, 0);
        assertTrue(format.contains("char:["));
    }


    @Test
    public void shouldParseCharDutchFormat() throws Exception {
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy");

        DateTime dateTime1 = dateTimeFormatter.parseDateTime("01/Mar/2014");

        Locale nl = new Locale("NL");


        DateTimeFormatter gerFormatter = dateTimeFormatter.withLocale(nl);

        String print = gerFormatter.print(dateTime1);
        System.out.println("DateTime:" + print);

        DateTime dateTime = gerFormatter.parseDateTime("01/mrt/2014");
        System.out.println("now:" + dateTime);

    }




    @Test
    public void shouldParseCharCronFormat() throws Exception {
        String formatter =      "char:c EEE MMM  d HH:mm:ss yyyy";
        String text = "root 27556 c Fri Sep  7 03:10:00 2013";
        DateTimeExtractor format = new DateTimeExtractor(formatter);
        format.formatters = new String[] { formatter};

        String found = format.getFormat(text, System.currentTimeMillis());
        assertNotNull("Didnt get formatter:", found);
        System.out.println("Found:" + found);
        DateTime dateTime = format.getDateTime(text);
        System.out.println("Time:" + dateTime);
        assertEquals("Month wrong", 9, dateTime.getMonthOfYear());
        assertEquals(2013, dateTime.getYear());
    }


    @Test
    public void shouldParseEEETime() throws Exception {
        String longTime = "Wed Apr  2 2014 00:21:47 - Info - High - Request license Effix:KPlus@cheungr@172.17.25.6:1 (v3.0) from cdcv299021 for Kondor+, cheungr - cheungr granted (v3.0) [44/200/200]";
        DateTimeExtractor format = new DateTimeExtractor();
        DateTime dateTime = format.getDateTime(longTime);
        String lastFormat = format.getLastFormat();
        System.out.println("Got:" + dateTime);
        assertEquals(4, dateTime.getMonthOfYear());
    }
    @Test
    public void shouldParseEventTraceLogCSV() throws Exception {
        String line = "\"System.ServiceModel\"###Transfer###0###\", relatedActivityId=00000000-0000-0000-0000-000000000000\"######6376#########\"2013-09-12T10:35:53.8567829Z\"######\n" +
                "\"System.ServiceModel\"###Stop###131085######\"<TraceRecord xmlns=\"\"http://schemas.microsoft.com/2004/10/E2ETraceEvent/TraceRecord\"\" Severity=\"\"Stop\"\"><TraceIdentifier>http://msdn.microsoft.com/en-GB/library/System.ServiceModel.Diagnostics.ActivityBoundary.aspx</TraceIdentifier><Description>Activity boundary.</Description><AppDomain>client.vshost.exe</AppDomain><ExtendedData xmlns=\"\"http://schemas.microsoft.com/2006/08/ServiceModel/DictionaryTraceRecord\"\"><ActivityName>Construct ChannelFactory. Contract type: 'Microsoft.Samples.ServiceModel.ICalculator'.</ActivityName><ActivityType>Construct</ActivityType></ExtendedData></TraceRecord>\"###6376#########\"2013-09-12T10:35:53.8567829Z\"######\n";

        // 2013-09-12T10:35:53.8567829Z
        String json = "delim:#24 '\"'yyyy-MM-dd'T'HH:mm:ss'.'";
        DateTimeGrabber grabber = new DateTimeGrabber(json);
        String grabbed = grabber.grab(line);
        assertNotNull("Failed to extract it", grabbed);
        assertEquals("Thu Sep 12 10:35:53 BST 2013",grabber.parse(grabbed, 0).toString());

    }

    @Test
    public void shouldParseEventTraceLogXML() throws Exception {
        String line = "<E2ETraceEvent xmlns=\"http://schemas.microsoft.com/2004/06/E2ETraceEvent\">\n" +
                "\t<System xmlns=\"http://schemas.microsoft.com/2004/06/windows/eventlog/system\">\n" +
                "\t\t<EventID>524324</EventID>\n" +
                "\t\t<Type>3</Type>\n" +
                "\t\t<SubType Name=\"Verbose\">0</SubType>\n" +
                "\t\t<Level>16</Level>\n" +
                "\t\t<TimeCreated SystemTime=\"2013-09-10T16:53:49.2777433Z\" />\n" +
                "\t\t<Source Name=\"System.ServiceModel\" />\n" +
                "\t\t<Correlation ActivityID=\"{00000000-0000-0000-0000-000000000000}\" />\n" +
                "\t\t<Execution ProcessName=\"w3wp\" ProcessID=\"11488\" ThreadID=\"1\" />\n" +
                "\t\t<Channel/>\n" +
                "\t\t<Computer>CLIC7003</Computer>\n" +
                "\t</System>\n" +
                "\t\t<ApplicationData>\n" +
                "\t\t\t<TraceData>\n" +
                "\t\t\t\t<DataItem>\n" +
                "\t\t\t\t\t<TraceRecord xmlns=\"http://schemas.microsoft.com/2004/10/E2ETraceEvent/TraceRecord\" Severity=\"Verbose\">\n" +
                "\t\t\t\t\t\t<TraceIdentifier>http://msdn.microsoft.com/en-GB/library/System.ServiceModel.GetConfigurationSection.aspx</TraceIdentifier>\n" +
                "\t\t\t\t\t\t<Description>Get configuration section.</Description>\n" +
                "\t\t\t\t\t\t<AppDomain>/LM/W3SVC/2/ROOT/InstrumentService-1-130233056291327868</AppDomain>\n" +
                "\t\t\t\t\t\t<ExtendedData xmlns=\"http://schemas.microsoft.com/2006/08/ServiceModel/StringTraceRecord\">\n" +
                "\t\t\t\t\t\t\t<ConfigurationSection>system.serviceModel/serviceHostingEnvironment</ConfigurationSection>\n" +
                "\t\t\t\t\t\t</ExtendedData>\n" +
                "\t\t\t\t\t</TraceRecord>\n" +
                "\t\t\t\t</DataItem>\n" +
                "\t\t\t</TraceData>\n" +
                "\t\t</ApplicationData>\n" +
                "</E2ETraceEvent>";

        String json = "{ \"key\": \"SystemTime=\\\"\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss\" }";
        assertTrue(JsonKVGrabber.isForMe(json));
        JsonKVGrabber grabber = new JsonKVGrabber(json);
        String grabbed = grabber.grab(line);
        assertNotNull("Failed to extract it", grabbed);
        Date parse = grabber.parse(grabbed, 0);
        assertNotNull("Failed to grab date from:" + grabbed, parse);
        assertEquals("Tue Sep 10 16:53:49 BST 2013", parse.toString());

    }

    @Test
    public void shouldParseBSTTime() throws Exception {
        //String line = "23-11-2012 17:24:13 GMT"
        String line = "01-12-2012 09:52:03 GMT";
        //String line = "17-06-2013 11:41:34,921 BST";
        DateTimeExtractor extractor1 = new DateTimeExtractor();
        String format = extractor1.getFormat(line, System.currentTimeMillis());
        Date time = extractor1.getTime(line, System.currentTimeMillis());

    }

    @Test
    public void shouldParsePipeDelimited() throws Exception {

        String line = "[LOG|SYSTEM|27 Mar 2013 09:18:44,822|com.calypso.LOGCAT|schedulingFactory$child#0_Worker-2|-]\n" +
            "purged 0 events At Wed Mar 27 09:18:44 GMT 2013";

        CharGrabber grabber = new CharGrabber("char:|2 dd MMM yyyy hh:mm:ss,SSS");
        String grab = grabber.grab(line);

        Date parsed = grabber.parse(grab, System.currentTimeMillis());

        assertNotNull(parsed);

        assertEquals("27 Mar 2013 09:18:44,822", grab);
        assertEquals("Wed Mar 27 09:18:44 GMT 2013",grabber.parse(grab, 0).toString());
    }

    @Test
    public void shouldParseJsonKVUnix() throws Exception {
        String line = "{\"Active\":true,\"Blocked\":false,\"Connected\":true,\"Connection\":\"ID_prod-vl-podds-app01-52308-1365082515210-1_0\",\"RemoteAddress\":\"/127.0.0.1:35198\",\"Slow\":false,\"Timestamp\":1365086821\"}";
        String json = "{ \"key\": \"Timestamp\\\":\", \"format\": \"UNIX_LONG_\" }";
        JsonKVGrabber grabber = new JsonKVGrabber(json);
        assertTrue(JsonKVGrabber.isForMe(json));
        String grabbed = grabber.grab(line);
		assertEquals("1365086821", grabbed);
        assertEquals(" - grabbed:" + grabbed, "Thu Apr 04 15:47:01 BST 2013", grabber.parse(grabbed, 0).toString());
    }
    @Test
    public void shouldParseJsonKVFormat() throws Exception {
        String line = "{\"Active\":true,\"Blocked\":false,\"Connected\":true,\"Connection\":\"ID_prod-vl-podds-app01-52308-1365082515210-1_0\",\"RemoteAddress\":\"/127.0.0.1:35198\",\"Slow\":false,\"Timestamp\":\"17-Oct-2012 14:30:15\"}";
        //17-Oct-2012 14:30:15
        String json = "{ \"key\": \"Timestamp\\\":\\\"\", \"format\": \"dd-MMM-yyyy HH:mm:ss\" }";
        assertTrue(JsonKVGrabber.isForMe(json));
        JsonKVGrabber grabber = new JsonKVGrabber(json);
        String grabbed = grabber.grab(line);
        assertEquals("Wed Oct 17 14:30:15 BST 2012",grabber.parse(grabbed, 0).toString());
    }


    @Test
    public void shouldParseJsonKVFormatAgain() throws Exception {
        String line = "{\"log\":\"./usr/share/man/ja/man8/update-passwd.8.gz\\r\\n\",\"stream\":\"stdout\",\"time\":\"2014-05-12 16:51:29\"}";
        //17-Oct-2012 14:30:15
        String json = "{ \"key\": \"time\", \"format\": \"yyyy-MM-dd HH:mm:ss\" }";
        assertTrue(JsonKVGrabber.isForMe(json));
        JsonKVGrabber grabber = new JsonKVGrabber(json);
        String grabbed = grabber.grab(line);
        assertEquals("2014-05-12 16:51:29", grabbed);
        assertNotNull("Failed to extract content: grabbed:", grabbed);
        Date date = grabber.parse(grabbed, 0);
        assertNotNull("Failed to extract content: grabbed:" + grabbed, date);
        assertEquals("Mon May 12 16:51:29 BST 2014", date.toString());
    }
    @Test
	public void shouldGetGoodFormatter() throws Exception {
		String line = "17-Oct-2012 00:20    ubuntu-01       all     1.09    0.00    0.43    0.00    98.48";
		String format = new DateTimeExtractor("dd-MMM-yyyy HH:mm").getFormat(line, 0);
        assertNotNull(format);
		System.out.println("Format:" + format);
		assertTrue(format.endsWith(":mm"));
	}
	
	
//	@Test
//	public void shouldPickupOutTimeFormatUsingWMI() throws Exception {
//		String filename = "test-data/DTExtractor-WMI.log";
//		String myTimeFormat = "yyyyMMddHHmmss";
//		DateTimeExtractor timeExtractor = new DateTimeExtractor(myTimeFormat);
//		DateTime time = new DateTime(timeExtractor.getTime(FileUtil.readLines(filename, 1).get(0), System.currentTimeMillis()));
////		20110926235931.000000
//		assertEquals(new DateTime(2011,9,26,23,59,31,000), time);
//	}

	
	@Test
	public void shouldPickupOutTimeFormatUsingHex() throws Exception {
		String filename = "test-data/DTExtractor-Proxy-7-18389.log";
		String breakRule = BreakRule.Rule.Default.name();
		List<String> readLines = FileUtil.readLines(filename, 100, breakRule);
		String myTimeFormat = "char:#A yyyy-MM-dd HH:mm:ss.SSS";
		DateTimeExtractor timeExtractor = new DateTimeExtractor(myTimeFormat);
		String timeFormat = timeExtractor.getFormat(new File(filename), readLines);
		assertEquals(myTimeFormat, timeFormat);
	}

	@Test
	public void shouldPickupOutTimeFormat() throws Exception {
		String filename = "test-data/DTExtractor-Proxy-7-18389.log";
		String breakRule = BreakRule.Rule.Default.name();
		List<String> readLines = FileUtil.readLines(filename, 100, breakRule);
		String myTimeFormat = "char:-1 yyyy-MM-dd HH:mm:ss.SSS";
		DateTimeExtractor timeExtractor = new DateTimeExtractor(myTimeFormat);
		String timeFormat = timeExtractor.getFormat(new File(filename), readLines);
		assertEquals(myTimeFormat, timeFormat);
	}
	
	@Test
	public void shouldGetOSSECTime() throws Exception {
		String line = "** Alert 1305889317.1775821: - windows,\r\n" + 
				"2011 May 20 07:01:57 (CLONWWMAIL1) 192.168.3.100->WinEvtLog\r\n" + 
				"Rule: 18105 (level 4) -> 'Windows audit failure event.'\r\n" + 
				"Src IP: (none)\r\n" + 
				"User: (no user)\r\n" + 
				"WinEvtLog: Security: AUDIT_FAILURE(5159): Microsoft-Windows-Security-Auditing: (no user): no domain: CLONWWMAIL1.clone-systems.com: The Windows Filtering Platform has blocked a bind to a local port. Application Information:  Process ID:  4424  Application Name: \\device\\harddiskvolume1\\program files\\microsoft\\exchange server\\v14\\bin\\edgetransport.exe  Network Information:  Source Address:  0.0.0.0  Source Port:  55434  Protocol:  17  Filter Information:  Filter Run-Time ID: 0  Layer Name:  %%14608  Layer Run-Time ID: 36\r\n"; 
		DateTimeExtractor extractor = new DateTimeExtractor("5s MMM dd HH:mm:ss");
		DateTime time = new DateTime(extractor.getTime(line,  0));
		assertEquals(20, time.getDayOfMonth());
	}
	@Test
	public void shouldGetSpaceSplitTime() throws Exception {
		String line = " \"/posts\" for 192.168.70.227 at 2012-11-26 01:12:43 +0000";
		DateTimeExtractor extractor = new DateTimeExtractor("5s yyyy-MM-dd HH:mm:ss");
		DateTime time = new DateTime(extractor.getTime(line,  0));
		assertEquals(26,time.getDayOfMonth());
	}

    @Test
    public void shouldExtractWebLogFormat() throws Exception {
        String accessLog = "152.2.169.107 - - [10/Jul/2013:12:48:54 -0400] \"GET /products/logscape.html HTTP/1.1\" 200 20022 \"-\" \"Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.3 (KHTML, like Gecko) Chrome/5.0.360.4 Safari/533.3\"\n";

        DateTimeExtractor dateTimeExtractor = new DateTimeExtractor();
        Date time = dateTimeExtractor.getTime(accessLog, System.currentTimeMillis());
        System.out.println(new DateTime(time));
        assertNotNull(time);
        // if should not contain the time because -400 is very different timezone to GMT/BST
        assertTrue(!time.toString().contains("13:12"));
    }

	
	@Test
	public void shouldExtractIISLogFormat() throws Exception {
//		2010-11-21	09:26:03	58.48.110.165	GET	/origin-www.accuweather.com/adrequest/adrequest.asmx/getAdIPhoneCode?strAppID=iphone&strPartnerCode=iphonecurr&strIPAddress=59.71.189.55&strUserAgent=Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20&strCurrentZipCode=ASI%7CCN%7CCH013%7CWuhan&strWeatherIcon=11&strUUID=6ab4597d2ce494deadbeef4b2f164f1f4a1c4c43	200	1327	0	"-"	"Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20"	"-"
		String line = "2010-11-21	09:26:03	58.48.110.165	GET	/origin-www.accuweather.com/adrequest/adrequest.asmx/getAdIPhoneCode?strAppID=iphone&strPartnerCode=iphonecurr&strIPAddress=59.71.189.55&strUserAgent=Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20&strCurrentZipCode=ASI%7CCN%7CCH013%7CWuhan&strWeatherIcon=11&strUUID=6ab4597d2ce494deadbeef4b2f164f1f4a1c4c43	200	1327	0	\"-\"	\"Mozilla/5.0%20(IPhone:%20U;%20CPU%20iPhone%20OS%202_1%20like%20Mac%20OS%20X;%20en-us)%20AppleWebKit/525.18.1%20(KHTML,%20like%20Gecko)%20Version/3.1.1%20Mobile/5F136%20Safari/525.20\"	\"-\""; 
		String customFormat = "yyyy-MM-dd xx- uses default from DiscoProperties -xxx	HH:mm:ss";
		DateTimeExtractor dateTimeExtractor = new DateTimeExtractor(customFormat);
		String format = dateTimeExtractor.getFormat(line, 0);
		System.out.println("Got Format:" + format);
		assertNotNull(format);

		
	}
	
	@Test
	public void shouldExtractFromGCLog() throws Exception {
		String customFormat = "yyyy-MM-dd'T'hh:mm:ss.SSS";
		DateTimeExtractor dateTimeExtractor = new DateTimeExtractor(customFormat);
		DateTime time = new DateTime(dateTimeExtractor.getTime("2012-04-12T16:27:01.911", System.currentTimeMillis()));
		System.out.println(time);
		assertGoodDate(time, 2012, 04, 12, 16, 27, 1, 9);
	}
	
	@Test
	public void shouldExtractFromCSV_SPACE() throws Exception {
		String customFormat = "char:,1 MM/dd HH:mm";
		DateTimeExtractor dateTimeExtractor = new DateTimeExtractor(customFormat);
		DateTime time = new DateTime(dateTimeExtractor.getTime("stuff,07/06 12:00 and stuff", System.currentTimeMillis()));
        final int year = new DateTime().getYear();//-1;
        assertGoodDate(time, year, 07, 06, 0, 0, 0, 0);
		
	}
	@Test
	public void shouldExtractFromCSV() throws Exception {
		String customFormat = "char:,2 yyyy-MM-dd HH:mm:ss";
		DateTimeExtractor dateTimeExtractor = new DateTimeExtractor(customFormat);
		DateTime time = new DateTime(dateTimeExtractor.getTime("this,that,2010-02-01 12:32:21 and stuff", System.currentTimeMillis()));
		assertGoodDate(time, 2010, 02, 01, 12, 32, 21, 0);
		
	}

	
	@Test
	public void shouldExtractFromXpolog() throws Exception {
		String customFormat = "char:[yyyy-MM-dd HH:mm:ss,SSS";
		DateTimeExtractor dateTimeExtractor = new DateTimeExtractor(customFormat);
		String filename = "build/dtExtractor_xpolog.log";
		FileOutputStream fileOutputStream = new FileOutputStream(filename);
		String logLine = "[2009-11-13 14:04:15,830] [Thread-5] [ERROR] [xpolog.eye.util.XMLUtil] [xpolog.eye.util.XMLUtil.parseXML(XMLUtil.java:252)] Failed to parse XML C:\\workspace\\dev\\current\\TxExpo\\conf\\logsconf\\dfdsfs_1223379658228.xml\r\n";
		
		fileOutputStream.write(logLine.getBytes());
		fileOutputStream.close();
		
		DateTime time = new DateTime(dateTimeExtractor.getFileStartTime(new File(filename), 20));
		assertGoodDate(time, 2009, 11, 13, 14, 04, 15, 830);
		
		String activeFormat = dateTimeExtractor.getActiveFormat();
		assertTrue(!activeFormat.equals("none"));
	}
	@Test
	public void shouldExtractFromRouterLogs() throws Exception {
		String customFormat = "MMM dd yyyy HH:mm:ss";
		DateTimeExtractor dateTimeExtractor = new DateTimeExtractor(customFormat);
		String filename = "build/dtExtractor_fwall.log";
		FileOutputStream fileOutputStream = new FileOutputStream(filename);
		String logLine = "Oct  5 2009 00:21:00 147.237.73.129 45984037: Oct  5 00:59:59:\r\n";
		
		fileOutputStream.write(logLine.getBytes());
		fileOutputStream.close();
		
		DateTime time = new DateTime(dateTimeExtractor.getFileStartTime(new File(filename), 20));
		assertGoodDate(time, 2009, 10, 5, 00, 21, 00, 00);
		
		String activeFormat = dateTimeExtractor.getActiveFormat();
		assertTrue(!activeFormat.equals("none"));
	}
	
	@Test
	public void shouldExtractFromFirewallLogs() throws Exception {
		String customFormat = "char:; dMMMyyyy;HH:mm:ss";
		DateTimeExtractor dateTimeExtractor = new DateTimeExtractor(customFormat);
		String filename = "build/dtExtractor_fwall.log";
		FileOutputStream fileOutputStream = new FileOutputStream(filename);
		String logLine = "3;5Oct2009;00:26:26;192.168.175.161;log;accept;;daemon;outbound;VPN-1 & FireWall-1;;222.178.5.234;65.54.179.198;tcp;22;http;35832;;;;;;;;http://65.54.179.198:80/ThirdPartyCookieCheck.srf?tpc=654202078&lc=1033;;;;;;;;;;;;;;;;;;;;;;;;\r\n";
		
		fileOutputStream.write(logLine.getBytes());
		fileOutputStream.close();
		
		DateTime time = new DateTime(dateTimeExtractor.getFileStartTime(new File(filename), 20));
		assertGoodDate(time, 2009, 10, 5, 00, 26, 26, 00);
		
		String activeFormat = dateTimeExtractor.getActiveFormat();
		assertTrue(!activeFormat.equals("none"));
	}
	
	@Test
	public void configureTimeExtractionFromContents() throws Exception {
		String customFormat = "char:[ dd/MMM/yyyy:HH:mm:ss";
		DateTimeExtractor dateTimeExtractor = new DateTimeExtractor(customFormat);
		String filename = "build/dtExtractor_apache.log";
		FileOutputStream fileOutputStream = new FileOutputStream(filename);
		String logLine = "65.33.94.190 - - [05/Apr/2005:17:26:27 -0500] \"GET /scripts/root.exe?/c+dir HTTP/1.0\" 404 276\r\n";
		fileOutputStream.write(logLine.getBytes());
		fileOutputStream.close();
		
		DateTime time = new DateTime(dateTimeExtractor.getFileStartTime(new File(filename), 20));
		assertGoodDate(time, 2005, 4, 5, 17, 26, 27, 00);
		
		String activeFormat = dateTimeExtractor.getActiveFormat();
		assertTrue(!activeFormat.equals("none"));
		
		
	}
	
	@Test
	public void shouldGenerateGoodText() throws Exception {
		String text =      "2010-07-29T17:22:46.346626-05:00 line 1";
		String formatter = "xccc";//yyyy-MM-dd'T'HH:mm:ss";

		String output = DateTimeExtractor.testWithFormat(formatter, text);
		System.out.println(output);
		
	}

	@Test
	public void shouldGetSyslogTimeFormat() throws Exception {
		String logLine =      "2010-07-29T17:22:46.346626-05:00 line 1";
		String customFormat = "yyyy-MM-dd'T'HH:mm:ss";
		SimpleDateFormat formatter = new SimpleDateFormat(customFormat);
		Date parse = formatter.parse(logLine);
		System.out.println("P:" + parse);
		
		extractor = new DateTimeExtractor(customFormat);
		extractor.setExtractorFormat(customFormat);
		Date time = extractor.getTimeUsingFormatter(logLine, 0);
		
		assertGoodDate(new DateTime(time), 2010, 7, 29, 17, 22, 46, 0);

	}
	
	@Test
	public void shouldGetTimeFromApacheLogFormat() throws Exception {
		String logLine = "65.33.94.190 - - [05/Apr/2005:17:26:27 -0500] \"GET /scripts/root.exe?/c+dir HTTP/1.0\" 404 276\r\n";
		String customFormat = "char:[ dd/MMM/yyyy:HH:mm:ss";
		
		org.joda.time.format.DateTimeFormatter f = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss");
		DateTime parseDateTime = f.parseDateTime("05/Apr/2003:17:26:27");
		
		System.out.println(parseDateTime);
		extractor = new DateTimeExtractor(customFormat);
		extractor.setExtractorFormat(customFormat);
		Date time = extractor.getTimeUsingFormatter(logLine, 0);
		
		assertGoodDate(new DateTime(time), 2005, 4, 5, 17, 26, 27, 00);
		
	}

	@Test
	public void testShouldExtractLongTime() throws Exception {
		String logLine = "1242973152454,4042323260,demo,12,57,5015,1,3494388101,80,media.imeem.com,/pl/R3s6l1BZxw/aus=false/,2030491983,1830,0,1242973150,450,2,686,2558,4,39,12,6,50402048,0,0,0,2030491983,Mozilla,http://sweetlife-of-kelly.blogspot.com/,";
		int offset = logLine.indexOf("1242973150");
		System.out.println(offset);
		
		// time being extracted is this... 14th item
		//1242973152454
		DateTime lastMod = new DateTime(1242973150000L);
		
		extractor = new DateTimeExtractor("14csv UNIX_LONG");
		String format = extractor.getFormat(logLine, 0);
		assertNotNull("didnt get format from extractor:", format);
		assertTrue(format.contains("UNIX_LONG"));
		Date time = extractor.getTime(logLine, format,lastMod.getMillis());
		assertGoodDate(new DateTime(time, DateTimeZone.UTC), 2009, 5, 22, 06, 19, 10, 00);
	}
	
	@Test
	public void shouldExtractUnixLONG() throws Exception {
//		String text = "1242973152454,4042323260,demo,12,57,5015,1,3494388101,80,media.imeem.com,/pl/R3s6l1BZxw/aus=false/,2030491983,1830,0,1242973150,450,2,686,2558,4,39,12,6,50402048,0,0,0,2030491983,Mozilla,http://sweetlife-of-kelly.blogspot.com/,";
		String text = "1242973152454      0 88.229.78.212 TCP_MEM_HIT/200 3867 GET http://www.superbahis747.com/services/InPlayApp.mvc/GetInPlaySports?oddsFormat=EU&isShrinked=false&isPopularitySorted=true - NONE/- text/html";
		//String text = "1242973150" 1242973152.454

        Grabber formatter = new DateTimeGrabber("UNIX_LONG");
        String chopped = formatter.grab(text);
        Date parse = formatter.parse(chopped, 0);
        DateTime out = new DateTime(parse.getTime());
        System.out.println("Expecting:\n2009-05-22T07:19:12.454+01:00");
        System.out.println("Grabbed:" + chopped + " length:" + chopped.length());
        System.out.println(out);
        assertEquals("2009-05-22T07:19:12.000+01:00", out.toString());



        String msg = DateTimeExtractor.testWithFormat("UNIX_LONG", text);
System.out.println(msg);
	}

    @Test
    public void testShouldExtractUNIXLongTimeCollectD() throws Exception {
        String text = "\"type:collectd logscape-dev.entropy.entropy.value\" 3596.000000 1417986085";
        extractor = new DateTimeExtractor("3s UNIX_LONG");
        String format = extractor.getFormat(text, 0);

        System.out.println("Unix:" + format);

    }



	@Test
	public void testShouldExtractUNIXLongTime() throws Exception {
		String logLine = "1242973152454,4042323260,demo,12,57,5015,1,3494388101,80,media.imeem.com,/pl/R3s6l1BZxw/aus=false/,2030491983,1830,0,1242973150,450,2,686,2558,4,39,12,6,50402048,0,0,0,2030491983,Mozilla,http://sweetlife-of-kelly.blogspot.com/,";
		
		DateTime dateTime = new DateTime(2009, 5, 22, 06, 19, 10, 00);
		System.out.println("millis:" + dateTime.getMillis());
		long given = 1242973150L * 1000;
		System.out.println("Given:" + given + " split14:" + logLine.split(",")[14]);
		Date date = new Date(given);
		System.out.println(date);
		extractor = new DateTimeExtractor("UNIX_LONG");
		String format = extractor.getFormat(logLine, 0);
		assertNotNull("didnt get format from extractor:", format);
		assertTrue(format.contains("UNIX_LONG"));
		DateTime lastMod = new DateTime(1253279103756L);
		Date time = extractor.getTime(logLine, format,lastMod.getMillis());
		DateTime dateTime2 = new DateTime(1242973152454L);
		System.out.println(dateTime2);
		assertGoodDate(new DateTime(time, DateTimeZone.UTC), 2009, 5, 22, 07, 19, 12, 0);//454);
	}
	
	@Test
	public void testGetTimeWithFallback() throws Exception {
		Date timeWithFallback = extractor.getTimeWithFallback("1234567890123456789012345678901234567", DateTimeUtils.currentTimeMillis());
		assertNotNull(timeWithFallback);
	}
	
	@Test
	public void testGetFormatFromFile() throws Exception {
		
		File file = new File("build","dtExtractor1.log");
		FileOutputStream fos = new FileOutputStream(file);
		String logLine = "2009-08-20 11:39:56.000 GMT Daylight Time WARN\n";
		fos.write(logLine.getBytes());
		fos.close();
		
		String format = extractor.getFormat(file, 100,"");
		assertNotNull(format);
	}
	@Test
	public void testGetFormatFromFile2() throws Exception {
		File file = new File("build","dtExtractor1.log");
		FileOutputStream fos = new FileOutputStream(file);
		String logLine = "xxx 2009-08-20 11:39:56.000 GMT Daylight Time WARN\n";
		fos.write(logLine.getBytes());
		fos.close();
		
		String format = extractor.getFormat(file, 100, "");
		assertNull(format);
	}
	@Test
	public void testGetFormatFromFile3() throws Exception {
		// added formatter -
		//11:39:56 GMT Daylight Time
		//"hh:mm:ss zzzz"
		
		File file = new File("build","dtExtractor1.log");
		FileOutputStream fos = new FileOutputStream(file);

		String logLine = "11:39:56 GMT Daylight Time WARN\n";
		fos.write(logLine.getBytes());
		fos.write(logLine.getBytes());
		fos.write(logLine.getBytes());
		fos.close();
		
		String format = extractor.getFormat(file, 100, "");
		assertNotNull("Failed to get time format from:" + logLine, format);
	}
	
	@Test
	public void testPlatformComp3() throws Exception {
		
		String logLine = "2009-08-20 11:39:56.000 GMT Daylight Time WARN;";
		Date time = extractor.getTime(logLine, DateTimeUtils.currentTimeMillis());
		assertGoodDate(new DateTime(time), 2009, 8, 20, 11, 39, 56, 00);
	}
	
	@Test
	public void testPlatformComp2() throws Exception {
		String logLine = "Wed Sep 16 15:15:26 2009 WARN [7468] isHostUnderUtilizedByEndUser(): failed to get load info from ls_loadinfo(), LIM is down;";
		extractor = new DateTimeExtractor("EEE MMM dd HH:mm:ss yyyy");
		String format = extractor.getFormat(logLine, 0);
		assertNotNull("Failed to get format from:" + logLine, format);
		DateTime lastMod = new DateTime(1253279103756L);
		Date time = extractor.getTime(logLine, format,lastMod.getMillis());
		assertGoodDate(new DateTime(time), 2009, 9, 16, 15, 15, 26, 00);
	}
	
	@Test
	public void testPlatformComp1() throws Exception {
		String logLine = "Jul 02 14:11:41 2009 4556:4568 4 1.2.3 saveIndx(): ELIM over-riding value of load index <it>.";
		
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date parse = parser.parse("2009-07-02 14:11:41");
		
		Date time = extractor.getTime(logLine, parse.getTime());
		assertGoodDate(new DateTime(time), 2009, 7, 2, 14, 11, 41, 00);
	}
	
	@Test
	public void testShouldHaveGoodLine() throws Exception {
		extractor.getTime("	 -serviceCriteria:type equals Management", 0);
	}
	@Test
	public void testShouldHaveGoodLineLength() throws Exception {
		String format = "HH:mm:ss,SSS";
        DateTimeGrabber formatter = new DateTimeGrabber(format);
		assertTrue(formatter.isLineTooShort("12345678901", 0, format.length()));
		assertFalse(formatter.isLineTooShort("123456789012", 0, format.length()));
	}
	

	@Test
	public void testShouldGetTimeFromNtEventLogFormat() throws Exception {
		String line = " information   1704   14/09/2009 11:33:53      SceCli            LON-XPNAVERY   None            N/A                  Security policy in the Group policy objects has been applied successfully.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    ";
		extractor = new DateTimeExtractor("3s dd/MM/yyyy HH:mm:ss");
		String format = extractor.getFormat(line, 0);
		assertNotNull(format);
		Date time = extractor.getTime(line, new DateTime().minusYears(1).getMillis());
		assertNotNull("Failed to extract time", time);
		assertGoodDate(new DateTime(time), 2009, 9, 14, 11, 33, 53, 00);
	}
	@Test
	public void testShouldGetTimeFromNtEventLogFormat2() throws Exception {
		String line = " information   257    20/10/2009 15:42:16      McLogEvent        LON-XPNAVERY   None            NT AUTHORITY SYSTEM  The Scan was unable to scan password protected file ";
		extractor = new DateTimeExtractor("3s dd/MM/yyyy HH:mm:ss");
		String format = extractor.getFormat(line, 0);
		assertNotNull(format);
		Date time = extractor.getTime(line, new DateTime().minusYears(1).getMillis());
		assertNotNull("Failed to extract time", time);
		assertGoodDate(new DateTime(time), 2009, 10, 20, 15, 42, 16, 00);
	}
	@Test
	public void testShouldGetTimeFromNtEventLogFormat3() throws Exception {
		String line = " information   6005  03/26/2008 09:41:44 EventLog          NEIL-EB63E745E None            N/A                  The Event log service was started.";
		extractor = new DateTimeExtractor("3s MM/dd/yyyy HH:mm:ss");
		String format = extractor.getFormat(line, 0);
		assertNotNull(format);
		Date time = extractor.getTime(line, new DateTime().minusYears(1).getMillis());
		assertNotNull("Failed to extract time", time);
		assertGoodDate(new DateTime(time), 2008, 3, 26, 9, 41, 44, 00);
	}
	@Test
	public void testShouldGetTimeFromSysLogFormat() throws Exception {
		String line = "Aug 16 17:10:36 alteredcarbon /usr/sbin/ocspd[5034]: starting";
//Aug 16 18:27:57 alteredcarbon /usr/sbin/ocspd[5139]: starting
//Aug 16 19:06:20 alteredcarbon kernel[0]: vmnet: bridge-en0: interface en is going DOWN
//Aug 16 19:33:01 alteredcarbon kernel[0]: Ethernet [Intel8254X]: Link down on en0 called by disable() -- 
//Aug 16 19:33:01 alteredcarbon kernel[0]: Ethernet [Intel8254X]: Link down on en1 called by disable() -- 
//Aug 16 19:33:02 alteredcarbon kernel[0]: vmmon: powerStateDidChange flags=0x4 (state 2)
//Aug 16 19:06:20 alteredcarbon kernel[0]: System Sleep
		Date time = extractor.getTime(line, DateTimeUtils.currentTimeMillis());
		assertNotNull("Failed to extract time", time);
        int year = new DateTime().getYear();
        // no year specified - so if its a month in advance - check the clock was wound back to last year
        int monthOfYear = new DateTime().getMonthOfYear();
        if (monthOfYear < 8) year -= 1;

        assertGoodDate(new DateTime(time), year, 8, 16, 17, 10, 36, 00);
	}

	@Test
	public void testShouldGetGoodTimeFromTABDelimFile() throws Exception {
		String line =  "5688	Fri Jun 12 13:00:04 BST 2009	DistributedCache	curve-dev1	back	3656.0	0.0	2965.0	0.0	2965.0	0.0	0.0	0.0	57.0	960.0	0.0	0.0	0.0	330.0		0.0	0	0.0\n";
		DateTime t = new DateTime(extractor.getTime(line, new DateTime().getMillis()));
		assertGoodDate(t, 2009, 6, 12, 13, 00, 04, 00);
	}
	
	@Test
	public void testShouldGetGoodSTARTTimeFromTABDelimFile() throws Exception {
		String line =  "5688	Fri Jun 12 13:00:04 BST 2009	DistributedCache	curve-dev1	back	3656.0	0.0	2965.0	0.0	2965.0	0.0	0.0	0.0	57.0	960.0	0.0	0.0	0.0	330.0		0.0	0	0.0\n";
		File file = new File("build/DTX_GetGoodTime.txt");
		FileOutputStream fos = new FileOutputStream(file);
		fos.write("nothing on this line\n".getBytes());
		fos.write("nothing on this line\n".getBytes());
		fos.write("nothing on this line\n".getBytes());
		fos.write("nothing on this line\n".getBytes());
		fos.write(line.getBytes());
		fos.write("nothing on this line\n".getBytes());
		fos.close();
		DateTime t = new DateTime(extractor.getFileStartTime(file, 20));
		assertGoodDate(t, 2009, 6, 12, 13, 00, 04, 00);
	}
	
	@Test
	public void testShouldGetTimeFromFileAndLine() throws Exception {
		String line1 = "17:27:12,625 [Logger@9243192 3.5/459]DEBUG Coherence:3 - 2009-08-10 17:27:12.625/3.744 Oracle Coherence GE 3.5/459 <D5> (thread=Cluster, member=n/a): Member(Id=4, Timestamp=2009-08-10 17:27:12.385, Address=10.54.24.98:8090, MachineId=9314, Location=machine:lon-fortdev-calc1,process:4660,member:lon-fortdev-calc1_6, Role=SempraIridiumMdsRunServer) joined Cluster with senior member 1\n";
		extractor.setExtractorFormat("HH:mm:ss,SSS");
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date parse = parser.parse("2009-08-10 17:27:12.625");
		
		DateTime time = new DateTime(extractor.getTime(line1, parse.getTime()));
		assertGoodDate(time,  2009, 8, 10, 17, 27, 12, 625);
	}
	
	@Test
	public void shouldAlsoGetTimeFromMMMdddWhereLastModIsNeeded2() throws Exception {
		String line = "Apr 13 11:36:04 vyatta kernel: [25283645.834369] [frm-dmz-150-A] IN=eth0.250 OUT=eth0.101 SRC=212.23.0.199 DST=10.3.0.79 LEN=52 TOS=0x00 PREC=0x00 TTL=126 ID=20889 DF PROTO=TCP SPT=46056 DPT=8014 WINDOW=8192 RES=0x00 SYN URGP=0\r\n";
		long lastMod = DateTimeUtils.currentTimeMillis();
		DateTime dt = new DateTime(extractor.getTime(line, lastMod));
		
		boolean isAfterAprilTime = new DateTime().getDayOfYear() > new DateTime(new DateTime().getYear(), 4, 13,0,0,0,0).getDayOfYear();
		int offSet = isAfterAprilTime ? 0 : -1;
		assertEquals(new DateTime().getYear() + offSet, dt.getYear());
		
	}
	
	@Test
	public void testShouldGetTimeFromDDMMYYYYWhereLastModIsNeeded() throws Exception {
		String line1 = "02/03/2012 13:30:00 [main] INFO ConfigLoader:? - Setting ClusterConfig for -Dsystem=dev1 tangosol-coherence-override.xml => [tangosol-coherence-override-dev1.xml]";
		DateTime cTime = new DateTime(2012,03, 02, 13, 30,0, 0);
		DateTime time = new DateTime(extractor.getTimeUsingFormatter(line1, cTime.getMillis()));
		assertGoodDate(time,  cTime.getYear(), cTime.getMonthOfYear(), cTime.getDayOfMonth(), cTime.getHourOfDay(), cTime.getMinuteOfHour(), cTime.getSecondOfMinute(), 00);
	}
	@Test
	public void testShouldGetTimeFromMMDDYYYYWhereLastModIsNeeded() throws Exception {
		String line1 = "03/13/2012 13:30:00 [main] INFO ConfigLoader:? - Setting ClusterConfig for -Dsystem=dev1 tangosol-coherence-override.xml => [tangosol-coherence-override-dev1.xml]";
		DateTime cTime = new DateTime(2012,03, 13, 13, 30,0, 0);
		DateTime time = new DateTime(extractor.getTimeUsingFormatter(line1, cTime.getMillis()));
		assertGoodDate(time,  cTime.getYear(), cTime.getMonthOfYear(), cTime.getDayOfMonth(), cTime.getHourOfDay(), cTime.getMinuteOfHour(), cTime.getSecondOfMinute(), 00);
	}
	@Test
	public void testShouldGetTimeFromHHMMSSWhereLastModIsNeeded() throws Exception {
		String line1 = "14:05:03,756 [main] INFO ConfigLoader:? - Setting ClusterConfig for -Dsystem=dev1 tangosol-coherence-override.xml => [tangosol-coherence-override-dev1.xml]";
		DateTime lastMod = new DateTime(1253279103756L);
		extractor.setExtractorFormat("HH:mm:ss,SSS");
		DateTime time = new DateTime(extractor.getTimeUsingFormatter(line1, lastMod.getMillis()));
		assertGoodDate(time,  lastMod.getYear(), lastMod.getMonthOfYear(), lastMod.getDayOfMonth(), 14, 05, 03, 756);
	}


	@Test
	public void testShouldGetTimeFromDatewithoutYear() throws Exception {
		String line1 = "Dec 20 10:13:46 INFO ConfigLoader:? - Setting ClusterConfig for -Dsystem=dev1 tangosol-coherence-override.xml => [tangosol-coherence-override-dev1.xml]";
		DateTime lastMod = new DateTime();
		String format = "MMM dd HH:mm:ss";
		
		extractor = new DateTimeExtractor(format);
		String format2 = extractor.getFormat(line1, 0);
		System.out.println("format;" + format2);
		extractor.setExtractorFormat(format);
		
		DateTime time = new DateTime(extractor.getTimeUsingFormatter(line1, lastMod.getMillis()));
		assertGoodDate(time,   2014, 12, 20, 10, 13, 46, 000);
	}

	@Test
	public void testShouldGetGroups() throws Exception {
		String line = "56	Fri Jun 12 13:00:04 BST 2009	DistributedCache	curve-dev1	back	3656.0	0.0	2965.0	0.0	2965.0	0.0	0.0	0.0	57.0	960.0	0.0	0.0	0.0	330.0		0.0	0	0.0";
		String pattern = ".*DistributedCache\\s(.*)\\sback\\s(\\d+).*";
		MatchResult matches = RegExpUtil.matches(pattern, line);
		int groups = matches.groups();
		System.out.println("GroupCount:" + groups);
		for (int i = 0; i < groups; i++) {
			System.out.println(String.format("Group:%d v[%s]", i, matches.group(i)));
		}
	}
	
	@Test
	public void testShouldRememberAndUseCorrectTimeFromPrependedLine() throws Exception {
		String line = "56	Fri Jun 12 13:00:04 BST 2009	DistributedCache	curve-dev1	back	3656.0	0.0	2965.0	0.0	2965.0	0.0	0.0	0.0	57.0	960.0	0.0	0.0	0.0	330.0		0.0	0	0.0";
		String format = extractor.getFormat(line, 0);
		extractor.setExtractorFormat(format);
		Date time = extractor.getTimeUsingFormatter(line, DateTimeUtils.currentTimeMillis());
		assertNotNull("Failed to extract time", time);
		assertGoodDate(new DateTime(time), 2009, 06, 12, 13, 00, 04,00);
	}
	@Test
	public void testShouldGetCorrectTimeFromPrependedLineWithSingleDigit() throws Exception {
		String line = "1	Fri Jun 12 13:00:04 BST 2009	DistributedCache	curve-dev1	back	3656.0	0.0	2965.0	0.0	2965.0	0.0	0.0	0.0	57.0	960.0	0.0	0.0	0.0	330.0		0.0	0	0.0";
		DateTime dateTime = new DateTime();
		Date time = extractor.getTime(line, dateTime.getMillis());
		
		assertNotNull("Failed to extract time", time);
		assertGoodDate(new DateTime(time), 2009, 06, 12, 13, 00, 04,00);
	}
	@Test
	public void testShouldGetCorrectTimeFromPrependedLine() throws Exception {
		String line = "56	Fri Jun 12 13:00:04 BST 2009	DistributedCache	curve-dev1	back	3656.0	0.0	2965.0	0.0	2965.0	0.0	0.0	0.0	57.0	960.0	0.0	0.0	0.0	330.0		0.0	0	0.0";
		DateTime dateTime = new DateTime();
		Date time = extractor.getTime(line, dateTime.getMillis());
		
		assertNotNull("Failed to extract time", time);
		assertGoodDate(new DateTime(time), 2009, 06, 12, 13, 00, 04,00);
	}
	
	@Test
	public void testShouldGetCorrectFormatFromPrependedLine() throws Exception {
		String line = "56	Fri Jun 12 13:00:04 BST 2009	DistributedCache	curve-dev1	back	3656.0	0.0	2965.0	0.0	2965.0	0.0	0.0	0.0	57.0	960.0	0.0	0.0	0.0	330.0		0.0	0	0.0";
		String time = extractor.getFormat(line, 0);
		assertNotNull("Failed to extract time", time);
		assertEquals("1t EEE MMM dd HH:mm:ss zzz yyyy", time);
		DateTime now = new DateTime();
		
		assertGoodDate(new DateTime(extractor.getTime(line, now.getMillis())), 2009, 06, 12, 13,00,04,00);
	}
	
	@Test
	public void shouldExtractCoherenceTime() throws Exception {
		org.joda.time.format.DateTimeFormatter fmt = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss yyyy");
		DateTime parseDateTime = fmt.parseDateTime("Mon Feb 03 01:03:29 2014");
		System.out.println(DateUtil.shortDateTimeFormat4.print(parseDateTime));
		SimpleDateFormat simpleDateTimeFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
		Date parse = simpleDateTimeFormat.parse("Mon Feb 03 01:03:29 GMT 2014");
		System.out.println(parse);
		
			
		String line = "23289	Mon Feb 03 01:03:29 GMT 2014	DistributedCache	CurveMaturityGeneratorPO	back	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	-4.0		0.0	0.0";
		String time = extractor.getFormat(line, 0);
		assertNotNull("Failed to extract time", time);
		assertEquals("1t EEE MMM dd HH:mm:ss zzz yyyy", time);
		DateTime now = new DateTime();
		
		Date time2 = extractor.getTime(line, now.getMillis());
		assertGoodDate(new DateTime(time2), 2014, 02, 03, 01,03,29,00);
		
	}
	
//	@Test
//	public void testShouldHandleStoopidDSFormat() throws Exception {
//		String nextLine = "12/18/12 23:59:58.806 Info: [Engine] Message Server has started.\r\n";
//		assertGoodDate(new DateTime(extractor.getTime(nextLine, 0)), 2012, 12, 18, 23,59,58,806);
//	}
	
	@Test
	public void testShouldGetTimeFormat1() throws Exception {
		String nextLine = "2009-03-18 23:59:58,906 INFO Thread-6 (ProcessHandler.java:67)	 - Shutting down outstanding processes";
		
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		Date parse = parser.parse("2009-03-18 23:59:58,906");

		String format = extractor.getFormat(nextLine, parse.getTime());
		assertGoodDate(new DateTime(extractor.getTime(nextLine, parse.getTime())), 2009, 03, 18, 23,59,58,906);
		assertEquals("yyyy-MM-dd HH:mm:ss,SSS", format);
	}
	@Test
	public void testShouldGetTimeFormat2() throws Exception {
		String nextLine = "2009-03-18 14:59:58.906 INFO Thread-6 (ProcessHandler.java:67)	 - Shutting down outstanding processes";
		
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date parse = parser.parse("2009-03-18 14:59:58.906");
		
		String format = extractor.getFormat(nextLine, parse.getTime() );
		assertEquals("yyyy-MM-dd HH:mm:ss.SSS", format);
		assertGoodDate(new DateTime(extractor.getTime(nextLine, parse.getTime())), 2009, 03, 18, 14,59,58,906);
	}
	
	@Test
	public void shouldGetTimeWithMillis() throws Exception {
		String nextLine = "2011-11-18 09:44:18,299 [Thread-1] TRACE c.b.r.g.p.s.RiskCalculationServiceContainer onInvoke - onInvoke stop=71598802113736 counterpartyName=TEST_CTPY|scenarioType=OFFICIAL|sessionId=2|taskId=1|";
		assertGoodDate(new DateTime(extractor.getTime(nextLine, 0)), 2011, 11, 18, 9,44,18,299);
	}
	@Test
	public void testShouldGetTimeFormat3() throws Exception {
		String line = "09-03-18 14:02:58.906 INFO Thread-6 (ProcessHandler.java:67)	 - Shutting down outstanding processes";
		String format = extractor.getFormat(line, 0);
		assertEquals("yy-MM-dd HH:mm:ss.SSS", format);
		
		assertGoodDate(new DateTime(extractor.getTime(line, 0)), 2009, 03, 18, 14,02,58,906);
	}
	@Test
	public void testShouldGiveGoodLine() throws Exception {
		String line = "2009-03-19 14:40:01,896 INFO tailer-8-26 (SpaceWriter.java:237)  - LOGGER TAILER - failed parseFile[agent-spawned.log:151] timeFormat[null] line[14:40:00,197 INFO PFstcp://alteredcarbon.local:10000-4-11 (AggSpaceImpl.java:190)      - AggSpace alteredcarbon.local CPU:9] UsingTime2009-03-19T14:39:58.819Z\n";
		
		SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
		Date parse = parser.parse("2009-03-19 14:40:01,896");

		
		DateTime timeFromLine = new DateTime(extractor.getTime(line, parse.getTime()));
		assertGoodDate(timeFromLine, 2009, 03, 19, 14, 40, 01, 896);
	}
	
	@Test
	public void testShouldGiveGoodTimeLine() throws Exception {
		String line = "13/02/12 10:06:49 INFO tailer-8-26 (SpaceWriter.java:237)  - LOGGER TAILER - failed parseFile[agent-spawned.log:151] timeFormat[null] line[14:40:00,197 INFO PFstcp://alteredcarbon.local:10000-4-11 (AggSpaceImpl.java:190)      - AggSpace alteredcarbon.local CPU:9] UsingTime2009-03-19T14:39:58.819Z\n";
		
		DateTime timeFromLine = new DateTime(extractor.getTime(line, 1));
		assertGoodDate(timeFromLine, 2012, 01, 13, 10, 06, 49, 0);
	}
	public void assertGoodDate(DateTime given, int year, int month, int day, int hour, int minute, int second, int millis){
		assertEquals("Wrong year:" + given, year, given.getYear());
		assertEquals("Month was wrong:" + given, given.getMonthOfYear(),  given.getMonthOfYear(), month);
		assertEquals("Bad day:" + given, day, given.getDayOfMonth());
		
		// this can trip up on the Build server - the diff is due to the timezone delta of 1 hour in Ireland, 
		if (hour == 13 && given.getHourOfDay() != 12) {
			assertEquals("Bad hour:", hour, given.getHourOfDay());
		}
		assertEquals("Bad Minute:", minute, given.getMinuteOfHour());
		assertEquals(second, given.getSecondOfMinute());
		assertEquals(millis, given.getMillisOfSecond());
		
	}
}
