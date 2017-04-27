package com.liquidlabs.log.util;

import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DateTimeExtractorIntegrationTest {
	String root = "test-data/logexample/";
	
	private DateTimeExtractor extractor;

	@Before
	public void setup() {
		extractor = new DateTimeExtractor();
	}


    @Test
    public void shouldExtractCorrelation() throws Exception {
        String corr = "20-jan-2013 00:01:00.516.8 INFO: [getRegistrations] [OUT-RESP] [USER = UNAUTHENTICATED :: AILHEADER/CLIENTID = 1386432338638 :: AILHEADER/CORRELATIONID = ##BS_AE_MEDISCHE_GENT##Sat Dec 15 00:00:00 GMT 2013## :: PROCES/STATUS = null :: PROCES/STATUST = OK :: /PROCES/STATUS = null :: /PROCES/STATUST = OK]";
        DateTime dateTime = extractor.getDateTime(corr);
        assertEquals(20, dateTime.getDayOfMonth());
        assertEquals(01, dateTime.getMonthOfYear());
        assertEquals(00, dateTime.getHourOfDay());
        String format = extractor.getFormat(corr, System.currentTimeMillis());
        assertTrue(format.startsWith("dd"));
    }

	
	@Test
	public void shouldExtractCoherence() throws Exception {
		assertItWorks(root, "coherence-3.6/2010121613-network-health-detail.txt");
	}
	@Test
	public void shouldExtractIIS1() throws Exception {
		assertItWorks(root, "IIS.log");
	}
	
	@Test
	public void shouldExtractIIS2() throws Exception {
		assertItWorks(root, "IIS-2.log");
	}
	@Test
	public void shouldExtractAccess() throws Exception {
        //      10.0.0.61 - - [13/Nov/2008:12:01:47 +0200] "GET /index.jsp HTTP/1.1" 200 8306
        //199.188.204.157 - - [31/Mar/2013:08:15:01 -0400] "GET / HTTP/1.1" 200 16509 "-" "curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.13.6.0 zlib/1.2.3 libidn/1.18 libssh2/1.4.2

        assertItWorks(root, "access_1227433674895/data/Location_0/access.log00002");
//		assertItWorks(root, "/WORK/logs/weblogs/logscape.com-Apr-2013.log");
	}
	
	@Test
	public void shouldDoWLServerServer() throws Exception {
		assertItWorks(root, "AdminServer_1227433677708/data/Location_0/AdminServer.log");
	}
	
	@Test
	public void examplesServer() throws Exception {
		assertItWorks(root, "examplesServer_1227433673895/data/Location_0/examplesServer.log");
	}
	
	@Test
	public void shouldDoHttpAccess() throws Exception {
		assertItWorks(root, "http_access_1227433684848/data/Location_0/http_access.log");
	}
	@Test
	public void shouldDoHttpError() throws Exception {
		assertItWorks(root, "http_error_1227433684723/data/Location_0/http_error.log");
	}
	@Test
	public void shouldDoLocalhost() throws Exception {
		assertItWorks(root, "localhost_log_1227433673051/data/Location_0/localhost_log.2008-11-13.txt");
	}
	@Test
	public void shouldDoWebSphere2() throws Exception {
		assertItWorks(root, "SystemErr_1227433684973/data/Location_0/SystemErr.log");
	}
	
	@Test
	public void shouldDoWebSphere() throws Exception {
//        [11/15/08 9:18:48:657 IDT] 0000000a  I UOW=null source=com.ibm.ejs.ras.ManagerAdmin org=IBM prod=WebSphere component=Application Server thread=[main]

        assertItWorks(root, "SystemOut_1227433685083/data/Location_0/SystemOut.log");
	}
	@Test
	public void shouldDoWLServer() throws Exception {
		assertItWorks(root, "wl_server_1227433675020/data/Location_0/wl_server.log");
	}
	@Test
	public void shouldDoAccessLogs() throws Exception {
		assertItWorks(root, "access.log");
	}
	@Test
	public void shouldDoFireWall() throws Exception {
		assertItWorks(root, "fw.log");
	}
	@Test
	public void shouldDoIIS() throws Exception {
		assertItWorks(root, "IIS.log");
	}
	@Test
	public void shouldDoRouter() throws Exception {
		assertItWorks(root, "router.log");
	}
	@Test
	public void shouldDoSnort() throws Exception {
		assertItWorks(root, "snort.log");
	}
	@Test
	public void shouldDoSysLog() throws Exception {
		assertItWorks(root, "syslog.log");
	}
	@Test
	public void shouldDoSysLog2() throws Exception {
		assertItWorks(root, "syslog2.log");
	}
	@Test
	public void shouldDoWebServer() throws Exception {
		assertItWorks(root, "webserver.log");
	}
	@Test
	public void shouldDoXpolog() throws Exception {
		assertItWorks(root, "xpologlog.log");
	}
	private void assertItWorks(String dir, String filename) {
		File logFile = filename.startsWith("/") ? new File(filename)  : new File(root, filename);
		if (!logFile.exists()) throw new RuntimeException("File not found:" + logFile);
		String format = extractor.getFormat(logFile, 20,"");
		System.out.println(getClass().getSimpleName() + "Got Format:" + format);
		String line = "";
		if (format == null) {
			try {
				RAF raf = RafFactory.getRaf(logFile.getAbsolutePath(), BreakRule.Rule.SingleLine.name());
				line = raf.readLine();
				raf.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		assertNotNull("Failed:" + filename + "\n line:" + line, format);

	}
	
	

}
