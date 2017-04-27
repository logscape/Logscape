package com.logscape.disco.kv;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.logscape.disco.indexer.Pair;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static junit.framework.Assert.assertNotNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 18/04/2013
 * Time: 13:52
 * To change this template use File | Settings | File Templates.
 */
public class RulesKeyValueExtractorTest {

    private RulesKeyValueExtractor kve;

    @Before
    public void before() {
        kve = new RulesKeyValueExtractor();
    }
    @Test
    public void testShouldSpeedFactSet() throws Exception {

        if (true) return;
        String lastLine = "";
        int lineCount = 0;
        try {

//            while (true) {
                String file = "/WORK/logs/FactSet/access.log";
        //        while (true) {
                RAF raf = RafFactory.getRafSingleLine(file);
                String line = "";
                long start = System.currentTimeMillis();
                lineCount = 0;
                int count = 0;
                RulesKeyValueExtractor kve = new RulesKeyValueExtractor();

                while ((line = raf.readLine()) != null) {
                    lastLine = line;
    //                    System.out.println(line);
                    List<Pair> fields = kve.getFields(line);
                    count += fields.size();
                    lineCount++;
                }
                long end = System.currentTimeMillis();
                System.out.println("Lines:" + lineCount + " Elapsed:" + (end - start) + " Fields:" + count);
                printThroughputRate(file, (end - start));
//            }
        } catch (Throwable t) {
            System.out.println("Line: " + lineCount + "\nLastLine:" + lastLine);
            t.printStackTrace();
        }
    }


    private void printThroughputRate(String absolutePath, long elapsed) {
        long length = new File(absolutePath).length();
        double seconds = elapsed/1000.0;
        long mbytes = length/ FileUtil.MEGABYTES;
        System.out.println(String.format("Throughput:%f MB/sec", (mbytes/seconds)));
    }

    @Test
    public void testThingy() throws Exception {
        //ns1:datacenterId="NA1"
        //"OpenFileDescriptorCount": "183",
       String [] rules = new String[] {
               "[\"](A1:.)\": \"(^\"])[\"]",
       };

        RulesKeyValueExtractor.Config config = new RulesKeyValueExtractor.Config(Arrays.asList(rules), true);
        config.startPos = 0;
        kve = new RulesKeyValueExtractor(config);

//        String nLine = "2013-08-23 08:00:18 REPORT_SCHEDULE Schedule:Windows_Disk_Space_Alert Action:Triggered Events:2 ThresholdPassed[1]\n" +
//                "\n" +
//                " \t23/08/2013 08:00:11,EX311,HarddiskVolume1,71,71";
        //String nLine = "2013-09-18 10:01:25,747 INFO pool-2-thread-1 (license.TrialListener)\t Action:'Download' Email:'neil@neilavery.com' IpAddress:'217.20.25.200' Company:'mysome'";
        //String nLine = "Oct 29 15:44:43 logscape-dev vmunix: [357366.319836] type=1701 audit(1383061483.300:21308): auid=4294967295 uid=1000 gid=1000 ses=4294967295 pid=21276 comm=\"chrome\" reason=\"seccomp\" sig=0 syscall=4 compat=0 ip=0x7f0b83906215 code=0x50000";
//        String nLine = "08/11/13 09:24:43.360 INFO: [ServiceEvent] Engine:CDCS192022-18:TaskAccepted:FENG_VAR_LITE-3626900278003326020-0:'489726/0/6/0'";
//        String nLine = "Nov 14 11:09:23 battlestar kernel: [   13.148848] type = 1400 audit(1384427363.907:14): apparmor=\"STATUS\" operation=\"profile_load\" name=\"/usr/sbin/tcpdump\" pid=950 comm=\"apparmor_parser\"";
       // String nLine = "2009-04-22 18:40:24,109 WARN main (ORMapperFactory)123 Aa = bB Service";
        //String nLine = "Jul 31 00:00String: cloudAdminHost=admin.opsourcecloud.net|two";
//         String nLine = "23-Jan-2014 15:41:23\tAGT-LON-UBU3\troot\t7901\t0\t0.0\t00:00:03\t1.2\t37988\t102980\t?\tS\t2-09:07:28\truby\t/usr/bin/puppet_agent";
        //String nLine = "2014-01-30 16:29:35,238 INFO netty-reply-10-1 (space.AggSpaceImpl)       LOGGER - REGISTER SEARCH:0_0_0_0_0_0_0_1%0_62570-sysadmin-LLABS-13910993750640-alteredcarbon.local-20140130_162935 req:0_0_0_0_0_0_0_1%0_62570-sysadmin-LLABS-13910993750640-alteredcarbon.local-20140130_162935\n";

        // returning numeric key 189957: no value was found....
        //String nLine2 = "Jul 30 23:11:50 189957: [syslog@9 s_id=\"as2ag1ccnlabash01:514\"]: *Jul 19 06:26:19.290: %CDP-4-DUPLEX_MISMATCH: duplex mismatch discovered on GigabitEthernet0/21 (not half duplex), with fs2ag1ccnlabash01(AMS16460245) mgmt0 (half duplex).";
        //String nLine2 = "Jul 31 00:00:02 10.162.0.25 TP-Processor822 ERROR handlers.AbstractRestHandler error.159  - Exception occurred.  Reason Code: SERVER_DOES_NOT_EXIST.  Reason Detail: Could not find server with Id f2c50d77-e523-4b51-9c74-c4b2f438878e : Text logged: Could not find Server with Id f2c50d77-e523-4b51-9c74-c4b2f438878e";

        // Zs example
        String zLine = "06-Mar-2014 14:59:22 GMT\t{ \"objectName\": \"java.lang:type=OperatingSystem\", \"OpenFileDescriptorCount\": \"183\", \"MaxFileDescriptorCount\": \"40960\", \"FreeSwapSpaceSize\": \"19019595776\", \"ProcessCpuLoad\": \"0.0\", \"SystemCpuLoad\": \"0.0\", \"namepsace\": \"_.group1\", \"host\": \"10.28.1.170\" } ";
        String zLine1 = "25-Jul-13 17:38:00\tjava.lang:type=Memory.HeapMemoryUsage: committed:991744000\tinit:268435456\tmax:1065025536\tused:323971800\tpid:3930\tport:8989";
//        String zLine = "19-Aug-13 14:09:34 vhostTS=2013-08-19T14:09:00+01:00 entity=\"2400\" name=\"VPN-Unicredit-Smart\" type=\"VirtualMachine?\",diskusage=\"0\",cpu=\"71\",mem=\"399\",maxNetUsage=\"1\",diskRead=\"0\",diskWrite=\"0\",diskUsageMax=\"0\",netRcvRate=\"1\",netTransRate=\"0\",memConsumed=\"3144804\",netUsage=\"1\"";
        String zLine2 = "6/15/2013 10:17:12 AM Thread: Autobahn background (ID: 4996)  6022 Information - After service call: IStreamingSettingsProvider.GetStreamingSettings(). Response: \n" +
                " {\n" +
                " Type: StreamingSettings\n" +
                " DefaultAmountsSettings = '\n" +
                " {\n" +
                " Type: AmountsSettings\n" +
                " LeftMinAmount = '100'\n" +
                " RightMinAmount = '100'\n" +
                " LeftDefaultAmount = '1000000'\n" +
                " service=\"WorkstationFiles\"" +
                " RightDefaultAmount=\"1000000\"'\n" +
                " }'\n" +
                " DefaultMaxFarTenor = '120M'\n" +
                " DefaultNdfTenor = '1M'\n" +
                " MinNdfTenor = 'SN'\n" +
                " DefaultFixingReferenceCode = 'MANUAL'\n" +
                " Headers = '";
        // damians example
        String dLine = "2013-06-13 \n" +
                "06:08:02,573 \n" +
                "[Thread-0] \n" +
                "INFO \n" +
                "instrumentation \n" +
                "- \n" +
                " ODCThread[pool-2-thread-4] \n" +
                " sessionId:2e7a6a57-4dc7-449e-a222-a0b0ca62a353, \n" +
                " queryId:5d08de71-470c-432f-92d4-28bb363b7e52, \n" +
                " user:vest, \n" +
                " status:finished, \n" +
                " partitionCount:100, \n" +
                " objectCount:0, \n" +
                " metric:ExecutePage, \n" +
                " took:335, \n" +
                " query:odc.query().fromTransaction().where(transaction().getId()).equalTo($0).joinToCashflows(); \n" +
                " args:$0 \n" +
                "= \n" +
                "TransactionIdImpl \n" +
                "[sourceSystemInstance=WSS \n" +
                "GBLO \n" +
                "RBS, \n" +
                "sourceSystemTradeId=727272_PC_decr1]; \n" +
                " ";

        List<Pair> fields = kve.getFields(zLine);
        System.out.println("Fields:" + fields.size());
        for (Pair field : fields) {
            System.out.println(field.key + " => " + field.value);
        }

    }

    @Test
    public void testSingleKey_KVSlider() throws Exception {
        assertEquals("[Pair(2,9,14)]", scanLine(new KeySlider("[\t, ](A1.) = (^, )"), " a USER = NEIL "));
    }


    @Test
    public void testThingy2() throws Exception {
        String line = "2013-06-18 syslog.uri=\"udp://alteredcarbon.local:1514/syslog\"";
        List<Pair> fields = kve.getFields(line);
        System.out.println(fields);
        Assert.assertEquals(1, fields.size());
        Assert.assertEquals("udp://alteredcarbon.local:1514/syslog", fields.get(0).value);

    }


    @Test
    public void testTibcoXMLMessageAttributes() throws Exception {
        String msg = "<Trace Level=\"MIN\">\n" +
                " <Time Millis=\"1444646759145\">2015-10-12 12:45:59.145+02:00</Time>\n" +
                " <Server Format=\"IP\">EURV192D01.eu.rabodev.com</Server>\n" +
                " <LogText><![CDATA[hardware outputFile=-480440992]]></LogText>\n" +
                " <Source FileName=\"./../../../src/invscan/scanengine/wscanhw.cpp\" Method=\"wscanhw()\" Line=\"472\"/>\n" +
                " <Thread>7868</Thread>\n" +
                " <Process>10672</Process>\n" +
                "</Trace>";

        List<Pair> fields = kve.getFields(msg);
        System.out.println(fields.toString());
        Assert.assertTrue(fields.get(0).toString().contains("Level"));

    }
    @Test
    public void testEMSXML() throws Exception {
        String ems = "2013-07-10 15:00:39,763 [EMSDATA] <?xml version=\"1.0\" encoding=\"UTF-8\"?>[loyaltyrefdata][][/loyaltyrefdata] Request:<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"><SOAP-ENV:Header/><SOAP-ENV:Body><RetrieveReasonCodesResponse xmlns=\"http://soa.delta.com/loyaltyreferencedata/v1\"><ReasonCodesResponse Description=\"PREVIOUSLY REPORTED\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"A\"/><ReasonCodesResponse Description=\"NON-QUAL ACTIVITY\" MileageIndicator=\"Y\" PublishCode=\"Y\" ReasonCode=\"B\"/><ReasonCodesResponse Description=\"NEED DOCUMENTATION\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"C\"/><ReasonCodesResponse Description=\"CPN REFUNDED/XCHGD\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"D\"/><ReasonCodesResponse Description=\"POST TO ONE PROGRAM\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"F\"/><ReasonCodesResponse Description=\"PROMO CAP REACHED\" MileageIndicator=\"Y\" PublishCode=\"Y\" ReasonCode=\"H\"/><ReasonCodesResponse Description=\"PROMO CAP EXCEEDED\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"I\"/><ReasonCodesResponse Description=\"TRNSFR TO FLYINGBLUE\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"K\"/><ReasonCodesResponse Description=\"RETRO OUTSIDE 9 MO\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"L\"/><ReasonCodesResponse Description=\"DUPE ACT FROM MERGE\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"M\"/><ReasonCodesResponse Description=\"NON-QUAL ACTIVITY\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"N\"/><ReasonCodesResponse Description=\"DUPE ORIG OR DEST\" MileageIndicator=\"N\" PublishCode=\"Y\" ReasonCode=\"O\"/><ReasonCodesResponse Description=\"PENDING FROM PARTNER\" ";

        List<Pair> fields = kve.getFields(ems);
        System.out.println(fields);

    }

    //@Test DodgyTest - this test is bogus
    public void testFromSOAPMessage() throws Exception {
        //String line = "Jul 31 00:51:43 10.162.0.25 service.oec.vm.provision.submission.3 INFO  transformer.CloudRequestJaxbTransformer info.89  - Got cloud request: datacenterIdentifier=null, oecRp=f47add5a-9e76-49d2-af07-02235ff98d4a,params={},payload=<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><ns1:deployServerRequest ns1:serverId=\"f47add5a-9e76-49d2-af07-02235ff98d4a\" ns1:datacenterId=\"NA3\" xmlns:ns1=\"http://oec.messaging.opsource.net/schemas/messaging\"><ns1:organizationId>bc14fcd1-a3ae-4d51-b801-400d5c946adf</ns1:organizationId><ns1:serverName>serv6</ns1:serverName><ns1:machineName>10-226-181-11</ns1:machineName><ns1:serverImageVmwareId>vm-22621</ns1:serverImageVmwareId><ns1:cpuCount>2</ns1:cpuCount><ns1:memoryAmountMB>4096</ns1:memoryAmountMB><ns1:osStorageGb>50</ns1:osStorageGb><ns1:localStorageGb>0</ns1:localStorageGb><ns1:isStarted>false</ns1:isStarted><ns1:ipAddress>10.226.181.11</ns1:ipAddress><ns1:networkVirtualContextId>3b05b256-c922-11e2-b29c-001517c4643e</ns1:networkVirtualContextId><ns1:privateNet>10.";
//        String line = " ns1:id=\"21000\"";
        String line = " <ns1:ipAddress>10.226.181.11</ns1:ipAddress>";

        //PARAMS !!
        // ?option=com_content&sectionid=-1&task=edit&cid[]=3"


        //KeySlider s1 = new KeySlider("<(A1:.)>(^<)");
        KeySlider s1 = new KeySlider("[\t, ](A1:.)=\"(^\")");

        scanLine(new KeySlider[] { s1 }, line,false);


        String[] kv2 = s1.results.get(0).getKeyValue(line);
        assertEquals("ns1:ipAddress=10.226.181.11", kv2[0] +"="+ kv2[1]);

//        kv2 = s1.results.get(1).getKeyValue(line);
//        assertEquals("Key2:Value2", kv2[0] +":"+ kv2[1]);
//
//        kv2 = s1.results.get(2).getKeyValue(line);
//        assertEquals("aqs:chrome.0.57j0l3j60j62.641j0", kv2[0] +":"+ kv2[1]);
//
//        kv2 = s1.results.get(3).getKeyValue(line);
//        assertEquals("sourceid:chrome", kv2[0] +":"+ kv2[1]);
    }


    @Test
    public void testFromURLParams() throws Exception {
        String line = "https://www.google.co.uk/search?Key1=Value1&Key2=Value2&aqs=chrome.0.57j0l3j60j62.641j0&sourceid=chrome&ie=UTF-8";

        //PARAMS !!
        // ?option=com_content&sectionid=-1&task=edit&cid[]=3"


        KeySlider s1 = new KeySlider("[?&](A1.)=(A1.)");
        scanLine(new KeySlider[] { s1 }, line,false);


        String[] kv2 = s1.results.get(0).getKeyValue(line);
        assertEquals("Key1:Value1", kv2[0] +":"+ kv2[1]);

        kv2 = s1.results.get(1).getKeyValue(line);
        assertEquals("Key2:Value2", kv2[0] +":"+ kv2[1]);

        kv2 = s1.results.get(2).getKeyValue(line);
        assertEquals("aqs:chrome.0.57j0l3j60j62.641j0", kv2[0] +":"+ kv2[1]);

        kv2 = s1.results.get(3).getKeyValue(line);
        assertEquals("sourceid:chrome", kv2[0] +":"+ kv2[1]);
    }


    @Test
    public void testFromRealLINE_Weblogs() throws Exception {
        String line = "147.114.226.182 - - [31/Mar/2010:12:02:29 -0400] \"GET /plu/bullist.gif HTTP/1.1\" 200 108 \"http://www.liquidlabs-cloud.com/administrator/index.php?option=com_content&sectionid=-1&task=edit&cid[]=3\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2\"\n";
//            "147.114.226.182 - - [31/Mar/2010:12:02:29 -0400] \"GET /plugins/editors/tinymce/jscripts/tiny_mce/themes/advanced/images/justifyfull.gif HTTP/1.1\" 200 71 \"http://www.liquidlabs-cloud.com/administrator/index.php?option=com_content&sectionid=-1&task=edit&cid[]=3\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2\"\n" +
//            "147.114.226.182 - - [31/Mar/2010:12:02:29 -0400] \"GET /plugins/editors/tinymce/jscripts/tiny_mce/themes/advanced/images/indent.gif HTTP/1.1\" 200 112 \"http://www.liquidlabs-cloud.com/administrator/index.php?option=com_content&sectionid=-1&task=edit&cid[]=3\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2\"\n";

        //PARAMS !!
        // ?option=com_content&sectionid=-1&task=edit&cid[]=3"


        KeySlider s1 = new KeySlider("[?&](A1.)=(A1.)");
        scanLine(new KeySlider[] { s1 }, line,false);


        String[] kv2 = s1.results.get(0).getKeyValue(line);
        assertEquals("option:com_content", kv2[0] +":"+ kv2[1]);

        kv2 = s1.results.get(1).getKeyValue(line);
        assertEquals("sectionid:-1", kv2[0] +":"+ kv2[1]);


    }

    @Test
    public void testFromRealLINE_FACTSET() throws Exception {
//        String line = "21-May-13 11:11:11\toType=\"VirtualMachine\",configStatus=\"GREEN\",guest.toolsStatus=\"TOOLS_NOT_INSTALLED\",name=\"darren-linux\",runtime.maxCpuUsage=\"3058\",runtime.maxMemoryUsage=\"2048\",runtime.powerState=\"POWERED_OFF\"\n" +
//                "21-May-13 11:11:11\toType=\"VirtualMachine\",configStatus=\"GREEN\",guest.guestFamily=\"solarisGuest\",guest.hostName=\"soldev01\",guest.ipAddress=\"192.168.70.208\",guest.toolsStatus=\"TOOLS_OK\",name=\"soldev01\",runtime.maxCpuUsage=\"3058\",runtime.maxMemoryUsage=\"1024\",runtime.powerState=\"POWERED_ON\"";
//        String line = "2013-04-03 12:40:04:70 service=\"WorkstationFiles\",serverName=\"Gateway\",feHost=\"cauthstagea02\",chainId=\"515C5B643267A439\",clientIp=\"192.168.1.1\",method=\"GET\",url=\"/services/WorkstationFiles/real_time/config/ic_bw.xmlss\",httpVer=\"HTTP/1.1\",hdrHost=\"lima-gateway-staging.factset.com\",hdrConnection=\"keep-alive\",hdrAcceptCharset=\"utf-8\",hdrIfModifiedSince=\"Fri, 04 May 2012 17:55:23 GMT\",hdrXFds3pAtlasVersion=\"2192.168.1.1\",hdrXFdsOverrideName=\"qa\",hdrAuthorization=\"Basic T1RQX1NFUlZJQ0VTOmQ0M3lQdnFrcEp0RUlmUTI=\",hdrXDatadirectAuthToken=\"724b192.168.1.1\",hdrUserAgent=\"Chrome/192.168.1.1 FactSet/2192.168.1.1\",hdrAcceptEncoding=\"gzip,deflate\",hdrAcceptLanguage=\"en-US,en\",hdrXForwardedFor=\"192.168.1.1\",user=\"NKOCHAKIAN\",serial=\"QA\",authUser=\"OTP_SERVICES\",beRspTime=\"42\",beMethod=\"GET\",beUrl=\"/qa/service/WorkstationFiles/real_time/config/ic_bw.xmlss?username=NKOCHAKIAN&serial_number=QA\",beHttpVer=\"HTTP/1.1\",beHdrHost=\"services-staging.factset.com\",beHdrConnection=\"keep-alive\",beHdrAcceptCharset=\"utf-8\",beHdrIfModifiedSince=\"Fri, 04 May 2012 17:55:23 GMT\",beHdrXFds3pAtlasVersion=\"2192.168.1.1\",beHdrXFdsOverrideName=\"qa\",beHdrAuthorization=\"Basic T1RQX1NFUlZJQ0VTOmQ0M3lQdnFrcEp0RUlmUTI=\",beHdrUserAgent=\"Chrome/192.168.1.1 FactSet/2192.168.1.1\",beHdrAcceptEncoding=\"gzip,deflate\",beHdrAcceptLanguage=\"en-US,en\",beHdrXForwardedFor=\"192.168.1.1\",beHdrXFdsaRequestKey=\"515C5B643267A439\",beHdrXFdsaProxyOrigClientAddr=\"192.168.1.1\",beRspCode=\"304\",beRspContentLength=\"0\",beRspHdrDate=\"Wed, 03 Apr 2013 16:40:04 GMT\",beRspHdrServer=\"Apache/2.2.3 (Red Hat)\",beRspHdrConnection=\"Keep-Alive\",beRspHdrKeepAlive=\"timeout=15, max=98\",beRspHdrCacheControl=\"private, max-age=86400, persistent-storage\",duration=\"58\",rspCode=\"304\",rspContentLength=\"0\",rspHdrDate=\"Wed, 03 Apr 2013 16:40:04 GMT\",rspHdrCacheControl=\"private, max-age=86400, persistent-storage\",rspHdrConnection=\"keep-alive\",rspHdrKeepAlive=\"timeout=30\",rspHdrXDatadirectRequestKey=\"515C5B643267A439\",rspHdrServer=\"FactSet Lima Proxy\",connLeased=\"0\",connAvailable=\"2\",connPending=\"0\",connMax=\"1024\",reqActive=\"0\",reqPoolSize=\"10\",reqMaxPoolSize=\"1024\",resActive=\"0\",resPoolSize=\"10\",resMaxPoolSize=\"1024\"";
        //String line = "2013-04-03 12:40:03:899 service=\"WorkstationFiles\",rspHdrCacheControl=\"private, max-age=86400, persistent-storage\",serverName=\"Gateway\",feHost=\"cauthstagea02\",chainId=\"515C5B63EFBB1E46\",clientIp=\"192.168.1.1\",method=\"GET\",url=\"/services/WorkstationFiles/real_time/config/rt_fields.xml\",httpVer=\"HTTP/1.1\",hdrHost=\"lima-gateway-staging.factset.com\",hdrConnection=\"keep-alive\",hdrAcceptCharset=\"utf-8\",hdrIfModifiedSince=\"Thu, 10 Jan 2013 09:22:14 GMT\",hdrXFds3pAtlasVersion=\"2192.168.1.1\",hdrXFdsOverrideName=\"qa\",hdrAuthorization=\"Basic T1RQX1NFUlZJQ0VTOmQ0M3lQdnFrcEp0RUlmUTI=\",hdrXDatadirectAuthToken=\"724b192.168.1.1\",hdrUserAgent=\"Chrome/192.168.1.1 FactSet/2192.168.1.1\",hdrAcceptEncoding=\"gzip,deflate\",hdrAcceptLanguage=\"en-US,en\",hdrXForwardedFor=\"192.168.1.1\",user=\"NKOCHAKIAN\",serial=\"QA\",authUser=\"OTP_SERVICES\",beRspTime=\"136\",beMethod=\"GET\",beUrl=\"/qa/service/WorkstationFiles/real_time/config/rt_fields.xml?username=NKOCHAKIAN&serial_number=QA\",beHttpVer=\"HTTP/1.1\",beHdrHost=\"services-staging.factset.com\",beHdrConnection=\"keep-alive\",beHdrAcceptCharset=\"utf-8\",beHdrIfModifiedSince=\"Thu, 10 Jan 2013 09:22:14 GMT\",beHdrXFds3pAtlasVersion=\"2192.168.1.1\",beHdrXFdsOverrideName=\"qa\",beHdrAuthorization=\"Basic T1RQX1NFUlZJQ0VTOmQ0M3lQdnFrcEp0RUlmUTI=\",beHdrUserAgent=\"Chrome/192.168.1.1 FactSet/2192.168.1.1\",beHdrAcceptEncoding=\"gzip,deflate\",beHdrAcceptLanguage=\"en-US,en\",beHdrXForwardedFor=\"192.168.1.1\",beHdrXFdsaRequestKey=\"515C5B63EFBB1E46\",beHdrXFdsaProxyOrigClientAddr=\"192.168.1.1\",beRspCode=\"304\",beRspContentLength=\"0\",beRspHdrDate=\"Wed, 03 Apr 2013 16:40:03 GMT\",beRspHdrServer=\"Apache/2.2.3 (Red Hat)\",beRspHdrConnection=\"Keep-Alive\",beRspHdrKeepAlive=\"timeout=15, max=99\",beRspHdrCacheControl=\"private, max-age=86400, persistent-storage\",duration=\"150\",rspCode=\"304\",rspContentLength=\"0\",rspHdrDate=\"Wed, 03 Apr 2013 16:40:03 GMT\",rspHdrCacheControl=\"private, max-age=86400, persistent-storage\",rspHdrConnection=\"keep-alive\",rspHdrKeepAlive=\"timeout=30\",rspHdrXDatadirectRequestKey=\"515C5B63EFBB1E46\",rspHdrServer=\"FactSet Lima Proxy\",connLeased=\"0\",connAvailable=\"2\",connPending=\"0\",connMax=\"1024\",reqActive=\"0\",reqPoolSize=\"10\",reqMaxPoolSize=\"1024\",resActivne=\"0\",resPoolSize=\"10\",resMaxPoolSize=\"1024\"\n";
        //String line = "2013-04-03 12:40:04:433 service=\"WorkstationFiles\",hdrIfModifiedSince=\"Wed, 24 Oct 2012 07:18:27 GMT\",serverName=\"Gateway\",feHost=\"cauthstagea02\",chainId=\"515C5B6448297805\",clientIp=\"192.168.1.1\",method=\"GET\",url=\"/services/WorkstationFiles/real_time/config/FloatFieldMapping.txt\",httpVer=\"HTTP/1.1\",hdrHost=\"lima-gateway-staging.factset.com\",hdrConnection=\"keep-alive\",hdrAcceptCharset=\"utf-8\",hdrXFds3pAtlasVersion=\"2192.168.1.1\",hdrXFdsOverrideName=\"qa\",hdrAuthorization=\"Basic T1RQX1NFUlZJQ0VTOmQ0M3lQdnFrcEp0RUlmUTI=\",hdrXDatadirectAuthToken=\"724b192.168.1.1\",hdrUserAgent=\"Chrome/192.168.1.1 FactSet/2192.168.1.1\",hdrAcceptEncoding=\"gzip,deflate\",hdrAcceptLanguage=\"en-US,en\",hdrXForwardedFor=\"192.168.1.1\",user=\"NKOCHAKIAN\",serial=\"QA\",authUser=\"OTP_SERVICES\",beRspTime=\"46\",beMethod=\"GET\",beUrl=\"/qa/service/WorkstationFiles/real_time/config/FloatFieldMapping.txt?username=NKOCHAKIAN&serial_number=QA\",beHttpVer=\"HTTP/1.1\",beHdrHost=\"services-staging.factset.com\",beHdrConnection=\"keep-alive\",beHdrAcceptCharset=\"utf-8\",beHdrIfModifiedSince=\"Wed, 24 Oct 2012 07:18:27 GMT\",beHdrXFds3pAtlasVersion=\"2192.168.1.1\",beHdrXFdsOverrideName=\"qa\",beHdrAuthorization=\"Basic T1RQX1NFUlZJQ0VTOmQ0M3lQdnFrcEp0RUlmUTI=\",beHdrUserAgent=\"Chrome/192.168.1.1 FactSet/2192.168.1.1\",beHdrAcceptEncoding=\"gzip,deflate\",beHdrAcceptLanguage=\"en-US,en\",beHdrXForwardedFor=\"192.168.1.1\",beHdrXFdsaRequestKey=\"515C5B6448297805\",beHdrXFdsaProxyOrigClientAddr=\"192.168.1.1\",beRspCode=\"304\",beRspContentLength=\"0\",beRspHdrDate=\"Wed, 03 Apr 2013 16:40:04 GMT\",beRspHdrServer=\"Apache/2.2.3 (Red Hat)\",beRspHdrConnection=\"Keep-Alive\",beRspHdrKeepAlive=\"timeout=15, max=96\",beRspHdrCacheControl=\"private, max-age=86400, persistent-storage\",duration=\"59\",rspCode=\"304\",rspContentLength=\"0\",rspHdrDate=\"Wed, 03 Apr 2013 16:40:04 GMT\",rspHdrCacheControl=\"private, max-age=86400, persistent-storage\",rspHdrConnection=\"keep-alive\",rspHdrKeepAlive=\"timeout=30\",rspHdrXDatadirectRequestKey=\"515C5B6448297805\",rspHdrServer=\"FactSet Lima Proxy\",connLeased=\"0\",connAvailable=\"2\",connPending=\"0\",connMax=\"1024\",reqActive=\"0\",reqPoolSize=\"10\",reqMaxPoolSize=\"1024\",resActive=\"0\",resPoolSize=\"10\",resMaxPoolSize=\"1024\"\n";
        String line = "2013-04-03 12:40:04:433 service=\"WorkstationFiles\",beUrl=\"/qa/service/WorkstationFiles/real_time/config/rt_fields.xml?username=NKOCHAKIAN&serial_number=QA\",beHttpVer=\"HTTP/1.1\",beHd\n" +
                "rHost=\"services-staging.factset.com\",beHdrConnection=\"keep-alive\",beHdrAcceptCharset=\"utf-8\",beHdrIfModifiedSince=\"Thu, 10 Jan 2013 09:22:14 GMT\",beHdrXFds3pAtlasVersion=\"2192.168.1.1\"\n" +
                ",beHdrXFdsOverrideName=\"qa\",beHdrAuthorization=\"Basic T1RQX1NFUlZJQ0VTOmQ0M3lQdnFrcEp0RUlmUTI=\",beHdrUserAgent=\"Chrome/192.168.1.1 FactSet/2192.168.1.1\",beHdrAcceptEncoding=\"gzip,deflate\",beHdrAcceptLanguage=\"en-US,en\",beHdrXForwardedFor=\"192.168.1.1\",beHdrXFdsaRequestKey=\"515C5B63EFBB1E46\",beHdrXFdsaProxyOrigClientAddr=\"192.168.1.1\",beRspCode=\"304\",beRspContentLeng\n" +
                "th=\"0\",beRspHdrDate=\"Wed, 03 Apr 2013 16:40:03 GMT\",beRspHdrServer=\"Apache/2.2.3 (Red Hat)\",beRspHdrConnection=\"Keep-Alive\",beRspHdrKeepAlive=\"timeout=15, max=99\",beRspHdrCacheControl=\n" +
                "\"private, max-age=86400, persistent-storage\",duration=\"150\",rspCode=\"304\",rspContentLength=\"0\",rspHdrDate=\"Wed, 03 Apr 2013 16:40:03 GMT\",rspHdrCacheControl=\"private, max-age=86400, pe\n" +
                "rsistent-storage\",rspHdrConnection=\"keep-alive\",rspHdrKeepAlive=\"timeout=30\",rspHdrXDatadirectRequestKey=\"515C5B63EFBB1E46\",rspHdrServer=\"FactSet Lima Proxy\",connLeased=\"0\",connAvailab\n" +
                "le=\"2\",connPending=\"0\",connMax=\"1024\",reqActive=\"0\",reqPoolSize=\"10\",reqMaxPoolSize=\"1024\",resActive=\"0\",resPoolSize=\"10\",resMaxPoolSize=\"1024\"\n";

//        List<Pair> fields = kve.getFields(line);
//        System.out.println("Fields:" + fields);

//        KeySlider s1 = new KeySlider(" (A1)=\"(^\")");
        KeySlider s2 = new KeySlider("[\t, ](A1)=\"(^\")");
        KeySlider[] ss = new KeySlider[] { s2 };
        scanLine(ss, line,false);

//        String[] kv1 = s1.results.get(0).getKeyValue(line);
//        assertEquals("service:WorkstationFiles", kv1[0] +":"+ kv1[1]);

        String[] kv2 = s2.results.get(0).getKeyValue(line);
        assertEquals("service:WorkstationFiles", kv2[0] +":"+ kv2[1]);
        //assertEquals("beUrl:/qa/service/WorkstationFiles/real_time/config/rt_fields.xml?username=NKOCHAKIAN&serial_number=QA", kv2[0] +":"+ kv2[1]);
    }

    @Test
    public void shouldLearnTheConfig() throws Exception {
        String line = "2013-04-03 12:40:04:433 service=\"WorkstationFiles\",beUrl=\"/qa/service/WorkstationFiles/real_time/config/rt_fields.xml?username=NKOCHAKIAN&serial_number=QA\",beHttpVer=\"HTTP/1.1\",beHd\n" +
//                "rHost=\"services-staging.factset.com\",beHdrConnection=\"keep-alive\",beHdrAcceptCharset=\"utf-8\",beHdrIfModifiedSince=\"Thu, 10 Jan 2013 09:22:14 GMT\",beHdrXFds3pAtlasVersion=\"2192.168.1.1\"\n" +
//                ",beHdrXFdsOverrideName=\"qa\",beHdrAuthorization=\"Basic T1RQX1NFUlZJQ0VTOmQ0M3lQdnFrcEp0RUlmUTI=\",beHdrUserAgent=\"Chrome/192.168.1.1 FactSet/2192.168.1.1\",beHdrAcceptEncoding=\"gzip,deflate\",beHdrAcceptLanguage=\"en-US,en\",beHdrXForwardedFor=\"192.168.1.1\",beHdrXFdsaRequestKey=\"515C5B63EFBB1E46\",beHdrXFdsaProxyOrigClientAddr=\"192.168.1.1\",beRspCode=\"304\",beRspContentLeng\n" +
//                "th=\"0\",beRspHdrDate=\"Wed, 03 Apr 2013 16:40:03 GMT\",beRspHdrServer=\"Apache/2.2.3 (Red Hat)\",beRspHdrConnection=\"Keep-Alive\",beRspHdrKeepAlive=\"timeout=15, max=99\",beRspHdrCacheControl=\n" +
//                "\"private, max-age=86400, persistent-storage\",duration=\"150\",rspCode=\"304\",rspContentLength=\"0\",rspHdrDate=\"Wed, 03 Apr 2013 16:40:03 GMT\",rspHdrCacheControl=\"private, max-age=86400, pe\n" +
//                "rsistent-storage\",rspHdrConnection=\"keep-alive\",rspHdrKeepAlive=\"timeout=30\",rspHdrXDatadirectRequestKey=\"515C5B63EFBB1E46\",rspHdrServer=\"FactSet Lima Proxy\",connLeased=\"0\",connAvailab\n" +
                "le=\"2\",connPending=\"0\",resMaxPoolSize=\"1024\"\n";

        List<Pair> fields1 = kve.getFields(line);
        int pos = 0;
        for (Pair fieldI : fields1) {
            System.out.printf(pos++ + ") " + fieldI + "\n");
        }
        RulesKeyValueExtractor.Config config = kve.getConfig();
        assertTrue(config.getRules().size() > 0);

        config.learn = false;
        kve = new RulesKeyValueExtractor(config);
        List<Pair> fields2 = kve.getFields(line);

        pos = 0;
        for (Pair fieldI : fields2) {
            System.out.printf(pos++ + ") " + fieldI + "\n");
        }


        Assert.assertEquals(fields1.size(),fields2.size());
}
    @Test
     public void shouldLearnTheConfigAgain() throws Exception {
        String line = "2013-08-23 08:00:18 REPORT_SCHEDULE Schedule:Windows_Disk_Space_Alert Action:Triggered Events:2 ThresholdPassed[1]\n";

        List<Pair> fields1 = kve.getFields(line);
        RulesKeyValueExtractor.Config config1 = kve.getConfig();
        assertEquals(1, config1.rules.size());
        assertEquals(35, config1.startPos);
        assertEquals(35, config1.rules.get(0).from);

//        Assert.assertEquals(fields1.size(),fields2.size());

    }




    @Test
    public void testFromRealLINE_WithQuotes() throws Exception {
        String line = "2013-04-18 00:00:00,184 INFO manager-34-25 (tailer.TailerEmbeddedAggSpace)\tStarting SEARCH[all_X_System_Utilizati CPU=\"100\" on_X_LLABS-4a7c4b56-13e173f2eea-73d4-20130418-000000]";
        KeySlider keySlider = new KeySlider(" (A1)=\"(1)");
        scanLine(keySlider, line);
        assertEquals("[Pair(113,118,122)]",keySlider.results().toString());

        String[] keyValue = keySlider.results().get(0).getKeyValue(line);
        assertEquals("CPU:100", keyValue[0] + ":" + keyValue[1]);
    }


    @Test
    public void testFromRealLINE() throws Exception {
        String line = "2013-04-18 00:00:00,184 INFO manager-34-25 (tailer.TailerEmbeddedAggSpace)\tStarting SEARCH[all_X_System_Utilizati CPU:100 on_X_LLABS-4a7c4b56-13e173f2eea-73d4-20130418-000000]";
        KeySlider keySlider = new KeySlider(" (A1):(1)");
        scanLine(keySlider, line);
        assertEquals("[Pair(113,117,121)]",keySlider.results().toString());

        String[] keyValue = keySlider.results().get(0).getKeyValue(line);
        assertEquals("CPU:100", keyValue[0] + ":" + keyValue[1]);
    }


    @Test
    public void testGrabMultipleKeys() throws Exception {
        KeySlider keySlider = new KeySlider(" (A1):(1)");
        scanLine(keySlider, " CPU:100 stuff CPU2:100 ");
        assertEquals("[Pair(0,4,8), Pair(14,19,23)]",keySlider.results().toString());
    }

    @Test
    public void testOffsetMoreKey_KVSlider() throws Exception {
        KeySlider keySlider = new KeySlider(" (A1):(1)");
        scanLine(keySlider, "   CPU:100 stuff");
        assertEquals("[Pair(2,6,10)]",keySlider.results().toString());
    }

    @Test
    public void testOffsetKey_KVSlider() throws Exception {
        KeySlider keySlider = new KeySlider(" (A1):(A1)");
        scanLine(keySlider, "  CPU:100 stuff");
        assertEquals("[Pair(1,5,9)]",keySlider.results().toString());
    }


    @Test
    public void testSingleKey_WithVQuotes() throws Exception {
        String line = " host.cpu=\"100\"";
        KeySlider slider = new KeySlider("[, ](A1.)=\"(^\")");
        scanLine(slider, line);
        System.out.println("GOT:" + slider.results().get(0).toString());
        String[] keyValue = slider.results().get(0).getKeyValue(line);
        Assert.assertEquals("host.cpu:100", keyValue[0] + ":" + keyValue[1]);
    }


    /**
     * "key": "val"
     * "key": 999
     * "key": val,
     * @throws Exception
     */
    @Test
    public void testSingleKey_WithJSonr() throws Exception {
        assertEquals("[Pair(1,7,11)]", scanLine(new KeySlider("\"(A1)\": (1)"), " \"CPU\": 100, "));
    }

    @Test
    public void testSingleKey_KVSliderEQ() throws Exception {
        assertEquals("[Pair(0,4,8)]", scanLine(new KeySlider(" (A1):(1)"), " CPU:100 stuff"));
    }
    @Test
    public void testSingleKey_KVSliderOptionalPre() throws Exception {
        assertEquals("[Pair(0,4,8)]", scanLine(new KeySlider("[, ](A1):(1)"), " CPU:100 stuff"));
    }


    @Test
    public void testGetSetupStuff() throws Exception {
        String testLine = " \"stuff\": \"value\"";
        KVRule.Rule preToken = kve.getPreText(" (A1):(A1)");
        assertTrue(preToken.toString().contains("SingleChar"));
        String postToken = kve.getPostText(" (A1)\":(A1)\"");
        assertEquals("\":", postToken);

        KVRule.Rule rule = kve.getKeyRule(" (A1) (1) ");
        assertNotNull(rule);

        KVRule.Rule vrule = kve.getValueRule(" (A1) (1) ");
        assertNotNull(vrule);

    }

    @Test
    public void shouldMatchFieldWithSpaces() {
        final RulesKeyValueExtractor extractor = new RulesKeyValueExtractor("[\t, [](A1.)='(^')[']");
        final List<Pair> fields = extractor.getFields(" FIELD1='abc' field2='def' field3='112 345' ");
        //2015-08-23 20:43:24,473 INFO pool-2-thread-2 (license.TrialListener)	 Action:'Download' Email:'jhunsaker@yourserviceconsulting.com' IpAddress:'173.219.61.203' Company:'AYS'
        assertThat(fields.size(), is(3));

    }

    @Test
    public void shouldFindkeyPrecededByTab() {
        String foo = "2013-09-24 13:32:26,226 INFO long-running-11-22 (event.LoggingEventMonitor)\tevent:kvIndexRemove dbName:lut logId:242";
        final RulesKeyValueExtractor extractor = new RulesKeyValueExtractor();
        final List<Pair> fields = extractor.getFields(foo);
        assertThat(fields.size(), is(3));
    }

    @Test
    public void shouldDoSomething() {
        KVRule.Rule preMatcher = kve.getPreText("[, ](A1):(A1)");
        assertTrue(preMatcher.isValid(','));
        assertTrue(preMatcher.isValid(' '));
    }

    private String scanLine(KeySlider keySlider, String line) {
        return scanLine(new KeySlider[] { keySlider}, line, true);
    }
    private String scanLine(KeySlider[] slider, String line, boolean stringifyResults) {
        char[] chars = line.toCharArray();
        for (int i = 0; i < chars.length ; i++) {
            for (int j = 0; j < slider.length; j++) {
                slider[j].next(chars[i], i, chars.length);
            }
        }
        if (stringifyResults) return  slider[0].results().toString();
        else return "";
    }

}
