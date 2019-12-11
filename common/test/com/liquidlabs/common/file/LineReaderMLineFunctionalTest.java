package com.liquidlabs.common.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.MLineByteBufferRAF;
import com.liquidlabs.common.file.raf.RAF;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Before;
import org.junit.Test;

public class LineReaderMLineFunctionalTest {

    private static final String TEST_FILE = "build/ML-ReaderTest.log";

    RAF raf;

    @Before
    public void setup() {

        new File(TEST_FILE).delete();
        new File("build").mkdir();
    }

    @After
    public void tearDown() {
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.gc();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public void shouldRead_HPAngles() throws Exception {
        File file = new File("build/hp-angles-mline.log");
        file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(hp_lines.getBytes());
        fos.close();

        MLineByteBufferRAF raf = new MLineByteBufferRAF(file.getAbsolutePath());
        //# Time:
        raf.setBreakRule("Explicit:<");
        String readLine1 = raf.readLine();
        assertTrue(readLine1.startsWith("<"));

        String readLine2 = raf.readLine();
        assertTrue(readLine2.startsWith("<"));

    }

    @Test
    public void shouldReadEXPLICIT_TIME_() throws Exception {
        File file = new File("build/ML-AS_TEST.log");
        file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        int events = 100;
        for (int i = 0; i < events; i++) {
            String[] ll = this.explicit_TIME;
            for (String line : ll) {
                fos.write((line + "\n").getBytes());
            }
        }
        fos.close();
        MLineByteBufferRAF raf = new MLineByteBufferRAF(file.getAbsolutePath());
        //# Time:
        raf.setBreakRule("Explicit:# Time: ");
        String readLine1 = raf.readLine();
        String readLine2 = raf.readLine();
        String readLine3 = raf.readLine();
        String readLine4 = raf.readLine();
        assertTrue(readLine1.contains("LINE:1"));
        assertTrue(readLine2.contains("LINE:2"));
        assertTrue(readLine3.contains("LINE:3"));
        assertTrue(readLine4.contains("LINE:4"));
    }
    @Test
    public void shouldReadEXPLICIT_AS_() throws Exception {
        File file = new File("build/ML-AS_TEST.log");
        file.delete();
        FileOutputStream fos = new FileOutputStream(file);
        int events = 100;
        for (int i = 0; i < events; i++) {
            String[] ll = this.explicit_AS;
            for (String line : ll) {
                fos.write((line + "\n").getBytes());
            }
        }
        fos.close();
        MLineByteBufferRAF raf = new MLineByteBufferRAF(file.getAbsolutePath());
        raf.setBreakRule("Explicit:Processing");
        String readLine1 = raf.readLine();
        String readLine2 = raf.readLine();
        String readLine3 = raf.readLine();
        assertTrue(readLine1.contains(" at "));
        assertTrue(readLine2.contains(" at "));
        assertTrue(readLine3.contains(" at "));
    }


    @Test
    public void shouldReadMaxNumberOfEventsPerLine() throws Exception {
        File file = new File("build.ML-BBRafTestML.log");
        file.delete();
        FileOutputStream fos = new FileOutputStream(file.getAbsolutePath());
        int events = 100;// * 1000 * 2;
        for (int i = 0; i < events; i++) {
            String[] ll = this.lines;
            for (String line : ll) {
                line = line + " line:"  +i + " length:" + line.length() + " END\n";
                fos.write(line.getBytes());
            }
        }
        fos.close();
        MLineByteBufferRAF raf = new MLineByteBufferRAF(file.getAbsolutePath());
        raf.setBreakRule(BreakRule.Rule.Year.name());


        String rr = "";
        String prevLine = "";
        while ((rr = raf.readLine()) != null) {
            if (!rr.contains("END")) {
                System.out.println("Failed:" + rr);
                String readLine = raf.readLine();
                assertTrue(rr.contains("END"));
            }
            prevLine = rr;
            if (rr.endsWith("line:72 length:203 END")) {
                System.out.println(".....");
            }
        }

        file.delete();

    }

    @Test
    public void shouldWorkWithOSSEC() throws Exception {
        FileOutputStream fos = new FileOutputStream(TEST_FILE);
        String line = "** Alert 1305864240.21708: - windows,\n" +
                "2011 May 20 00:04:00 (CLONWWMAIL1) 192.168.3.100->WinEvtLog\n" +
                "Rule: 18105 (level 4) -> 'Windows audit failure event.'\n" +
                "Src IP: (none)\n" +
                "User: (no user - LINE_1)\n" +
                "WinEvtLog: Security: AUDIT_FAILURE(5159): Microsoft-Windows-Security-Auditing: (no user): no domain: CLONWWMAIL1.clone-systems.com: The Windows Filtering Platform has blocked a bind to a local port. Application Information:  Process ID:  4424  Application Name: \\device\\harddiskvolume1\\program files\\microsoft\\exchange server\\v14\\bin\\edgetransport.exe  Network Information:  Source Address:  0.0.0.0  Source Port:  55508  Protocol:  17  Filter Information:  Filter Run-Time ID: 0  Layer Name:  %%14608  Layer Run-Time ID: 36\n\n";

        fos.write(line.getBytes());

        line = "** Alert 1305864240.22419: - windows,\n" +
                "2011 May 20 00:04:00 (CLONWWMAIL1) 192.168.3.100->WinEvtLog\n" +
                "Rule: 18105 (level 4) -> 'Windows audit failure event.'\n" +
                "Src IP: (none)\n" +
                "User: (no user LINE_2)\n" +
                "WinEvtLog: Security: AUDIT_FAILURE(5159): Microsoft-Windows-Security-Auditing: (no user): no domain: CLONWWMAIL1.clone-systems.com: The Windows Filtering Platform has blocked a bind to a local port. Application Information:  Process ID:  4424  Application Name: \\device\\harddiskvolume1\\program files\\microsoft\\exchange server\\v14\\bin\\edgetransport.exe  Network Information:  Source Address:  0.0.0.0  Source Port:  58595  Protocol:  17  Filter Information:  Filter Run-Time ID: 0  Layer Name:  %%14608  Layer Run-Time ID: 36\n\n";
        fos.write(line.getBytes());

        line = "** Alert 1305864308.24552: mail  - windows,\n" +
                "2011 May 20 00:05:08 (CLONWWMAIL1) 192.168.3.100->WinEvtLog\n" +
                "Rule: 18153 (level 10) -> 'Multiple Windows audit failure events.'\n" +
                "Src IP: (none)\n" +
                "User: (no user) LINE_3\n" +
                "1 WinEvtLog: Security: AUDIT_FAILURE(5159): Microsoft-Windows-Security-Auditing: (no user): no domain: CLONWWMAIL1.clone-systems.com: The Windows Filtering Platform has blocked a bind to a local port. Application Information:  Process ID:  4424  Application Name: \\device\\harddiskvolume1\\program files\\microsoft\\exchange server\\v14\\bin\\edgetransport.exe  Network Information:  Source Address:  0.0.0.0  Source Port:  57129  Protocol:  17  Filter Information:  Filter Run-Time ID: 0  Layer Name:  %%14608  Layer Run-Time ID: 36\n" +
                "2 WinEvtLog: Security: AUDIT_FAILURE(5159): Microsoft-Windows-Security-Auditing: (no user): no domain: CLONWWMAIL1.clone-systems.com: The Windows Filtering Platform has blocked a bind to a local port. Application Information:  Process ID:  4424  Application Name: \\device\\harddiskvolume1\\program files\\microsoft\\exchange server\\v14\\bin\\edgetransport.exe  Network Information:  Source Address:  0.0.0.0  Source Port:  59108  Protocol:  17  Filter Information:  Filter Run-Time ID: 0  Layer Name:  %%14608  Layer Run-Time ID: 36\n" +
                "3 WinEvtLog: Security: AUDIT_FAILURE(5159): Microsoft-Windows-Security-Auditing: (no user): no domain: CLONWWMAIL1.clone-systems.com: The Windows Filtering Platform has blocked a bind to a local port. Application Information:  Process ID:  4424  Application Name: \\device\\harddiskvolume1\\program files\\microsoft\\exchange server\\v14\\bin\\edgetransport.exe  Network Information:  Source Address:  0.0.0.0  Source Port:  55301  Protocol:  17  Filter Information:  Filter Run-Time ID: 0  Layer Name:  %%14608  Layer Run-Time ID: 36\n" +
                "4 WinEvtLog: Security: AUDIT_FAILURE(5159): Microsoft-Windows-Security-Auditing: (no user): no domain: CLONWWMAIL1.clone-systems.com: The Windows Filtering Platform has blocked a bind to a local port. Application Information:  Process ID:  4424  Application Name: \\device\\harddiskvolume1\\program files\\microsoft\\exchange server\\v14\\bin\\edgetransport.exe  Network Information:  Source Address:  0.0.0.0  Source Port:  58595  Protocol:  17  Filter Information:  Filter Run-Time ID: 0  Layer Name:  %%14608  Layer Run-Time ID: 36\n" +
                "5 WinEvtLog: Security: AUDIT_FAILURE(5159): Microsoft-Windows-Security-Auditing: (no user): no domain: CLONWWMAIL1.clone-systems.com: The Windows Filtering Platform has blocked a bind to a local port. Application Information:  Process ID:  4424  Application Name: \\device\\harddiskvolume1\\program files\\microsoft\\exchange server\\v14\\bin\\edgetransport.exe  Network Information:  Source Address:  0.0.0.0  Source Port:  55508  Protocol:  17  Filter Information:  Filter Run-Time ID: 0  Layer Name:  %%14608  Layer Run-Time ID: 36\n\n";
        fos.write(line.getBytes());

        raf = new MLineByteBufferRAF(TEST_FILE);
        raf.setBreakRule("Explicit:**");

        String readLine = raf.readLine();
        assertTrue("shoulld start with stars:" + line, readLine.startsWith("**"));
        assertTrue("1 Got Line:" + line, readLine.contains("LINE_1"));
        assertFalse("First line contained second:" + line, readLine.contains("LINE_2"));
        readLine = raf.readLine();
        assertTrue("should start with stars:" + line, readLine.startsWith("**"));
        assertTrue("2 Got Line:" + line, readLine.contains("LINE_2"));

        readLine = raf.readLine();
        System.out.println("Last Line:" + readLine);
        assertNotNull("Didnt read last line", readLine);
        assertTrue("3 Got Line:" + line, readLine.contains("LINE_3"));

    }
    //	@Test
//	public void shouldRead_TIME_EXPLICIT_BREAKS() throws Exception {
//		FileOutputStream fos = new FileOutputStream(TEST_FILE);
//		String split = "\0\0\0\u000C";
//		String fileData = String.format("line 1%sline 2%sline 3%sline 4%sline 5",split,split,split,split);
//		byte[] bytes = fileData.getBytes();
//		fos.write(bytes);
//		fos.close();
//
//		raf = new MLineByteBufferRAF(TEST_FILE);
//		raf.setBreakRule("Explicit:\0\0\0\u000C");
//		String line = raf.readLine();
//		assertEquals("1 Got Line:" + line, "line 1",line);
//		line = raf.readLine();
//		assertEquals("2 Got Line:" + line, "line 2",line);
//		line = raf.readLine();
//		assertEquals("3 Got Line:" + line, "line 3",line);
//		line = raf.readLine();
//		assertEquals("4 Got Line:" + line, "line 4",line);
//		assertEquals(1, raf.linesRead());
//
//		line = raf.readLine();
//		assertNull("Should have been null but was:" + line, line);
//		assertEquals(0, raf.linesRead());
//	}
    @Test
    public void shouldReadALL_EXPLICIT_BREAKS() throws Exception {
        FileOutputStream fos = new FileOutputStream(TEST_FILE);
        fos.write("line 1\n<BR>line 2\n<BR>line 3\n<BR>line 4\n<BR>partial doesnt get read".getBytes());
        fos.close();

        raf = new MLineByteBufferRAF(TEST_FILE);
        raf.setBreakRule("Explicit:<BR>");
        String line = raf.readLine();
        assertEquals("1 Got Line:" + line, "line 1",line);
        line = raf.readLine();
        assertEquals("2 Got Line:" + line, "<BR>line 2",line);
        line = raf.readLine();
        assertEquals("3 Got Line:" + line, "<BR>line 3",line);
        line = raf.readLine();
        assertEquals("4 Got Line:" + line, "<BR>line 4",line);
        assertEquals(1, raf.linesRead());

        line = raf.readLine();
        assertNotNull(line);
    }


    @Test
    public void shouldReadALL_COMPLETE_Lines() throws Exception {
        FileOutputStream fos = new FileOutputStream(TEST_FILE);
        fos.write("line 1\n".getBytes());
        fos.write("line 2\n".getBytes());
        fos.write("line 3 complete\n".getBytes());
        fos.close();

        raf = new MLineByteBufferRAF(TEST_FILE);
        String line = raf.readLine();
        assertNotNull("1 Got Line:" + line, line);
        System.out.println(line + " pos:" + raf.getFilePointer());
        line = raf.readLine();
        assertNotNull("2 Got Line:" + line, line);
        System.out.println(line + " pos:" + raf.getFilePointer());
        line = raf.readLine();
        System.out.println(line + " pos:" + raf.getFilePointer());
        assertNotNull("3 Got Line:" + line, line);
    }

    @Test
    public void shouldHandleLineSkips() throws Exception {
        FileOutputStream fos = new FileOutputStream(TEST_FILE);
        fos.write("line 1\n".getBytes());
        fos.write(" line 1.1\n".getBytes());
        fos.write("line 2\n".getBytes());
        fos.write(" line 2.2\n".getBytes());
        fos.write("line 3\n".getBytes());
        fos.write("line 4\n".getBytes());
        fos.write("   line 4.4\n".getBytes());
        fos.write("line 5\n".getBytes());
        fos.close();

        raf = new MLineByteBufferRAF(TEST_FILE);
        String line;
        line = raf.readLine();
        assertTrue("Got Line:" + line, line.contains("1.1"));
        line = raf.readLine();
        assertTrue(line.contains("2.2"));

        line = raf.readLine();
        line = raf.readLine();
        assertTrue(line.contains("4.4"));



    }

    String[] lines  = { "2012-10-22 19:18:55,020 INFO WriteBehindThread:BinaryEntryStoreWrapper($Proxy5):dev2/web/region-zero-service MutationAwareCacheStore TASK - NormalBookXmlTransformer failure time:5ms (cacheName:dev2/web/wpf-reg-0,key:book/nb8910898.xml,op:store)",
            "2012-10-22 19:18:55,173 INFO WriteBehindThread:BinaryEntryStoreWrapper($Proxy5):dev2/web/region-zero-service InMemoryMarketTypeRepository NONE - MM - MM1 market type 5222 not-mapped (market-id: 7824786) ",
            "2012-10-22 19:18:55,174 DEBUG WriteBehindThread:BinaryEntryStoreWrapper($Proxy5):dev2/web/region-zero-service XmlMarketDataProcessor NONE - Exception in XmlMarketDataProcessor::processInsert\n" +
                    "task='NormalBookXmlTransformer,\n" +
                    "key='book/nb8911546.xml',\n" +
                    "\n" +
                    "com.sportingbet.murdoch.xml.parser.exception.BadXmlException: com.sportingbet.murdoch.xml.parser.exception.BusinessRuleViolationException: Attribute 'role' not found\n" +
                    "    at com.sportingbet.murdoch.xml.parser.SAXNormalBookXmlParser.parse(SAXNormalBookXmlParser.java:109)\n" +
                    "    at com.sportingbet.murdoch.transformer.NormalBookXmlTransformer.extractObjectsFromXml(NormalBookXmlTransformer.java:83)\n" +
                    "    at com.sportingbet.murdoch.transformer.NormalBookXmlTransformer.transformInsert(NormalBookXmlTransformer.java:52)\n" +
                    "    at com.sportingbet.murdoch.dataprocessor.XmlMarketDataProcessor.processInsert(XmlMarketDataProcessor.java:27)\n" +
                    "    at com.sportingbet.murdoch.cachestore.MutationAwareCacheStore.doStore(MutationAwareCacheStore.java:40)\n" +
                    "    at com.sportingbet.common.java.coherence.cachestore.MutatingBinaryEntryStore.store(MutatingBinaryEntryStore.java:20)\n" +
                    "    at sun.reflect.GeneratedMethodAccessor21.invoke(Unknown Source)\n" +
                    "    at sun.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)\n" +
                    "    at java.lang.reflect.Method.invoke(Unknown Source)\n" +
                    "    at org.springframework.aop.support.AopUtils.invokeJoinpointUsingReflection(AopUtils.java:309)\n" +
                    "    at org.springframework.aop.framework.ReflectiveMethodInvocation.invokeJoinpoint(ReflectiveMethodInvocation.java:183)\n" +
                    "    at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:150)\n" +
                    "    at org.springframework.aop.interceptor.ExposeInvocationInterceptor.invoke(ExposeInvocationInterceptor.java:89)\n" +
                    "    at org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:172)\n" +
                    "    at org.springframework.aop.framework.JdkDynamicAopProxy.invoke(JdkDynamicAopProxy.java:202)\n" +
                    "    at $Proxy5.store(Unknown Source)\n" +
                    "    at com.tangosol.net.cache.ReadWriteBackingMap$BinaryEntryStoreWrapper.storeInternal(ReadWriteBackingMap.java:6035)\n" +
                    "    at com.tangosol.net.cache.ReadWriteBackingMap$StoreWrapper.store(ReadWriteBackingMap.java:4814)\n" +
                    "    at com.tangosol.net.cache.ReadWriteBackingMap$WriteThread.run(ReadWriteBackingMap.java:4217)\n" +
                    "    at com.tangosol.util.Daemon$DaemonWorker.run(Daemon.java:803)\n" +
                    "    at java.lang.Thread.run(Unknown Source)\n" +
                    "Caused by: com.sportingbet.murdoch.xml.parser.exception.BusinessRuleViolationException: Attribute 'role' not found\n" +
                    "    at com.sportingbet.murdoch.xml.parser.SAXNormalBookXmlParser.startElement(SAXNormalBookXmlParser.java:135)\n" +
                    "    at com.sun.org.apache.xerces.internal.parsers.AbstractSAXParser.startElement(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.parsers.AbstractXMLDocumentParser.emptyElement(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.impl.XMLDocumentFragmentScannerImpl.scanStartElement(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.impl.XMLDocumentFragmentScannerImpl$FragmentContentDriver.next(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.impl.XMLDocumentScannerImpl.next(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.impl.XMLDocumentFragmentScannerImpl.scanDocument(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.parsers.XML11Configuration.parse(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.parsers.XML11Configuration.parse(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.parsers.XMLParser.parse(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.parsers.AbstractSAXParser.parse(Unknown Source)\n" +
                    "    at com.sun.org.apache.xerces.internal.jaxp.SAXParserImpl$JAXPSAXParser.parse(Unknown Source)\n" +
                    "    at javax.xml.parsers.SAXParser.parse(Unknown Source)\n" +
                    "    at javax.xml.parsers.SAXParser.parse(Unknown Source)\n" +
                    "    at com.sportingbet.murdoch.xml.parser.SAXNormalBookXmlParser.parse(SAXNormalBookXmlParser.java:88)\n" +
                    "    ... 20 more\n" +
                    "Caused by: com.sportingbet.murdoch.xml.parser.exception.BusinessRuleViolationException: Attribute 'role' not found\n" +
                    "    at com.sportingbet.murdoch.xml.parser.util.AttributeValueExtractor.extractString(AttributeValueExtractor.java:27)\n" +
                    "    at com.sportingbet.murdoch.xml.parser.SAXNormalBookXmlParser.buildParticipant(SAXNormalBookXmlParser.java:140)\n" +
                    "    at com.sportingbet.murdoch.xml.parser.SAXNormalBookXmlParser.startElement(SAXNormalBookXmlParser.java:124)\n" +
                    "    ... 34 more"};


    String[] explicit_AS =( "Processing CalendarsController#show (for 192.168.70.171 at 2012-11-22 12:07:13) [GET]\n" +
            "  Parameters: {\"project_id\"=>\"project-0\", \"controller\"=>\"calendars\", \"action\"=>\"show\"}\n" +
            "Rendering template within layouts/base\n" +
            "Rendering calendars/show\n" +
            "Completed in 5979ms (View: 353, DB: 2357) | 200 OK [http://192.168.70.179/projects/project-0/issues/calendar]\n" +
            "\n" +
            "\n" +
            "Processing CalendarsController#show (for 192.168.70.171 at 2012-11-22 12:07:13) [GET]\n" +
            "  Parameters: {\"project_id\"=>\"project-0\", \"controller\"=>\"calendars\", \"action\"=>\"show\"}\n" +
            "Rendering template within layouts/base\n" +
            "Rendering calendars/show\n" +
            "Completed in 5573ms (View: 385, DB: 2398) | 200 OK [http://192.168.70.179/projects/project-0/issues/calendar]\n" +
            "\n" +
            "\n" +
            "Processing CalendarsController#show (for 192.168.70.171 at 2012-11-22 12:07:25) [GET]\n" +
            "  Parameters: {\"project_id\"=>\"project-0\", \"controller\"=>\"calendars\", \"action\"=>\"show\"}\n" +
            "Rendering template within layouts/base\n" +
            "Rendering calendars/show\n" +
            "Completed in 399ms (View: 181, DB: 131) | 200 OK [http://192.168.70.179/projects/project-0/issues/calendar]\n" +
            "\n" +
            "\n" +
            "Processing CalendarsController#show (for 192.168.70.179 at 2012-11-22 12:07:42) [GET]\n" +
            "  Parameters: {\"project_id\"=>\"project-0\", \"controller\"=>\"calendars\", \"action\"=>\"show\"}\n" +
            "Rendering template within layouts/base\n" +
            "Rendering calendars/show\n" +
            "Completed in 1225ms (View: 891, DB: 201) | 200 OK [http://192.168.70.179/projects/project-0/issues/calendar]\n" +
            "\n" +
            "\n" +
            "Processing CalendarsController#show (for 192.168.70.227 at 2012-11-22 12:08:17) [GET]\n" +
            "  Parameters: {\"project_id\"=>\"project-0\", \"controller\"=>\"calendars\", \"action\"=>\"show\"}\n" +
            "Rendering template within layouts/base\n" +
            "Rendering calendars/show\n" +
            "Completed in 164ms (View: 44, DB: 13) | 200 OK [http://192.168.70.179/projects/project-0/issues/calendar]\n").split("\n");


    String[] explicit_TIME = (
            "/usr/sbin/mysqld, Version: 5.1.58-1ubuntu1-log ((Ubuntu)). started with:\n" +
                    "LINE:1 Tcp port: 3306  Unix socket: /var/run/mysqld/mysqld.sock\n" +
                    "Time                 Id Command    Argument\n" +
                    "# Time: 121123  6:25:13\n" +
                    "#LINE:2 User@Host: debian-sys-maint[debian-sys-maint] @ localhost []\n" +
                    "# Query_time: 0.357397  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0\n" +
                    "SET timestamp=1353651913;\n" +
                    "# administrator command: Refresh;\n"+
                    "Time                 Id Command    Argument\n" +
                    "# Time: 121123  6:25:13\n" +
                    "#LINE:3 User@Host: debian-sys-maint[debian-sys-maint] @ localhost []\n" +
                    "# Query_time: 0.357397  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0\n" +
                    "SET timestamp=1353651913;\n" +
                    "# administrator command: Refresh;\n" +
                    "Time                 Id Command    Argument\n" +
                    "# Time: 121123  6:25:13\n" +
                    "#LINE:4 User@Host: debian-sys-maint[debian-sys-maint] @ localhost []\n" +
                    "# Query_time: 0.357397  Lock_time: 0.000000 Rows_sent: 0  Rows_examined: 0\n" +
                    "SET timestamp=1353651913;\n" +
                    "# administrator command: Refresh;\n").split("\n");

    String hp_lines = "<2014-09-29 18:38:08,972> [ERROR] [pool-1-thread-1] (AbstractWinkClientHandler.java:51) - \n" +
            "com.hp.sw.bto.ast.security.lwsso.ws.handlers.SecurityHandlersException\n" +
            "\tat com.hp.sw.bto.ast.security.lwsso.ws.handlers.wrappers.LWSSOWinkClientHandler.handleException(LWSSOWinkClientHandler.java:42)\n" +
            "\tat com.hp.sw.bto.ast.generichandlers.wrappers.AbstractWinkClientHandler.handle(AbstractWinkClientHandler.java:39)\n" +
            "\tat org.apache.wink.client.internal.handlers.HandlerContextImpl.doChain(HandlerContextImpl.java:52)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.invoke(ResourceImpl.java:216)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.invoke(ResourceImpl.java:178)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.get(ResourceImpl.java:288)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMRestClientAPIsImpl.getAuthorizedFolderNames(CMRestClientAPIsImpl.java:814)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMClientCacheProxy.updateCache(CMClientCacheProxy.java:254)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMClientCacheProxy$CacheUpdateThread.run(CMClientCacheProxy.java:232)\n" +
            "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)\n" +
            "\tat java.util.concurrent.FutureTask$Sync.innerRunAndReset(FutureTask.java:351)\n" +
            "\tat java.util.concurrent.FutureTask.runAndReset(FutureTask.java:178)\n" +
            "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:178)\n" +
            "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)\n" +
            "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1110)\n" +
            "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:603)\n" +
            "\tat java.lang.Thread.run(Thread.java:722)\n" +
            "<2014-09-29 18:39:08,988> [ERROR] [pool-1-thread-1] (AbstractWinkClientHandler.java:51) - \n" +
            "com.hp.sw.bto.ast.security.lwsso.ws.handlers.SecurityHandlersException\n" +
            "\tat com.hp.sw.bto.ast.security.lwsso.ws.handlers.wrappers.LWSSOWinkClientHandler.handleException(LWSSOWinkClientHandler.java:42)\n" +
            "\tat com.hp.sw.bto.ast.generichandlers.wrappers.AbstractWinkClientHandler.handle(AbstractWinkClientHandler.java:39)\n" +
            "\tat org.apache.wink.client.internal.handlers.HandlerContextImpl.doChain(HandlerContextImpl.java:52)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.invoke(ResourceImpl.java:216)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.invoke(ResourceImpl.java:178)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.get(ResourceImpl.java:288)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMRestClientAPIsImpl.getAuthorizedFolderNames(CMRestClientAPIsImpl.java:814)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMClientCacheProxy.updateCache(CMClientCacheProxy.java:254)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMClientCacheProxy$CacheUpdateThread.run(CMClientCacheProxy.java:232)\n" +
            "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)\n" +
            "\tat java.util.concurrent.FutureTask$Sync.innerRunAndReset(FutureTask.java:351)\n" +
            "\tat java.util.concurrent.FutureTask.runAndReset(FutureTask.java:178)\n" +
            "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:178)\n" +
            "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)\n" +
            "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1110)\n" +
            "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:603)\n" +
            "\tat java.lang.Thread.run(Thread.java:722)\n" +
            "<2014-09-29 18:40:08,988> [ERROR] [pool-1-thread-1] (AbstractWinkClientHandler.java:51) - \n" +
            "com.hp.sw.bto.ast.security.lwsso.ws.handlers.SecurityHandlersException\n" +
            "\tat com.hp.sw.bto.ast.security.lwsso.ws.handlers.wrappers.LWSSOWinkClientHandler.handleException(LWSSOWinkClientHandler.java:42)\n" +
            "\tat com.hp.sw.bto.ast.generichandlers.wrappers.AbstractWinkClientHandler.handle(AbstractWinkClientHandler.java:39)\n" +
            "\tat org.apache.wink.client.internal.handlers.HandlerContextImpl.doChain(HandlerContextImpl.java:52)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.invoke(ResourceImpl.java:216)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.invoke(ResourceImpl.java:178)\n" +
            "\tat org.apache.wink.client.internal.ResourceImpl.get(ResourceImpl.java:288)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMRestClientAPIsImpl.getAuthorizedFolderNames(CMRestClientAPIsImpl.java:814)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMClientCacheProxy.updateCache(CMClientCacheProxy.java:254)\n" +
            "\tat com.hp.sw.bto.ast.security.cm.api.CMClientCacheProxy$CacheUpdateThread.run(CMClientCacheProxy.java:232)\n" +
            "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)\n" +
            "\tat java.util.concurrent.FutureTask$Sync.innerRunAndReset(FutureTask.java:351)\n" +
            "\tat java.util.concurrent.FutureTask.runAndReset(FutureTask.java:178)\n" +
            "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:178)\n" +
            "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)\n" +
            "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1110)\n" +
            "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:603)\n" +
            "\tat java.lang.Thread.run(Thread.java:722)a\n";

}
