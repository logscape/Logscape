package com.logscape.disco.grokit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static junit.framework.Assert.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 12/02/2014
 * Time: 12:30
 * To change this template use File | Settings | File Templates.
 */
public class GrokItTest {
    private GrokIt grokIt;
    private String filename = "nothing";
    private long timeMs = System.currentTimeMillis();

    @Before
    public void setUp() throws Exception {
        grokIt = new GrokIt(GrokIt.defaultConfig);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testCollectDMultiGroup() throws Exception {
        String defaultConfig =   "cpu-::cpu(ex352) is (500)";
        grokIt = new GrokIt(defaultConfig);
        Map<String,String> results = grokIt.processLine(filename, "cpuex352 is 500");
        //assertTrue("No Results", results.size() > 0);
        assertEquals("500", results.get("cpu-ex352"));
    }

    @Test
    public void testCollectdExtraction() throws Exception {
        String defaultConfig = "disk_used-percent_::.*\\.disk\\.ext\\S+\\.(\\S+)\\.used_percent\\ (\\d+\\.?\\d*).*";
        grokIt = new GrokIt(defaultConfig);
        Map<String,String> results = grokIt.processLine(filename, "CLD.deathstar.disk.ext4.-.used_percent 92.01360432500952 1466698660");
        assertEquals("92.01360432500952", results.get("disk_used-percent_-"));
    }

    @Test
    public void testCollectShouldIgnoreExtraGroup() throws Exception {
        String defaultConfig =   "cpu-::cpu(ex352) is (500) (cycles)";
        grokIt = new GrokIt(defaultConfig);
        Map<String,String> results = grokIt.processLine(filename, "cpuex352 is 500 cycles");
        //assertTrue("No Results", results.size() > 0);
        assertEquals("500", results.get("cpu-ex352"));
    }

    @Test
    public void testThreadRule() throws Exception {
        filename = "filename";

        grokIt.addRule("_threadName::(\\d+@\\w+\\-\\d+)");
        String line = "2014-11-05 08:14:06,888 [10162952@qtp0-27387] - Request [GetViewTree, 492270348, Session{sessionId='164', username='admin', customerId=1, id name: Default Client, clientAddr='local', lastUsedInMillis=141352151\n" +
                "6073}, logged in user: Integration], Input:\n" +
                " INFO   - TaskGenerator: no results for probe VCLD13GDAFHAP05, waiting 30 seconds [1544627621@qtp0-16772  bob@japan.com ";

        Map<String,String> results = grokIt.processLine(filename, line);
        assertTrue(results.toString().contains("threadName"));
//        assertEquals("james.mcT@gmail.com", results.get("_email"));

    }


    @Test
    public void testEmailPattern() throws Exception {
        filename = "filename";
        long start = System.currentTimeMillis();

        int total = 10000;
        for (int i = 0; i < total; i++) {
            Map<String,String> results = grokIt.processLine(filename, "Hello james.mcT@gmail.com and stuff");
        }
        long end = System.currentTimeMillis();
        long elaped = end - start;

        System.out.println("Elapsed:" + elaped + " per item(ms):" + ((double) elaped / (double) total));
//        assertTrue("No Results", results.size() > 0);
//        assertEquals("james.mcT@gmail.com", results.get("_email"));

    }
    @Test
    public void testIPAddressPattern() throws Exception {
        Map<String,String> results = grokIt.processLine(filename, "message from 10.28.1.150:here");
        assertTrue("No Results", results.size() > 0);
        assertEquals("10.28.1.150", results.get("_ipAddress"));

    }
    @Test
    public void testException() throws Exception {

        Map<String,String> results = grokIt.processLine(filename, "2014-02-10 17:15:56,197 INFO agent-sched-13-1 (VAuditLogger)\t module:Agent UpdateWork:battlestar-11003-0:replicator-1.0:DownloadService status: ERROR  msg:Mon Feb 10 17:15:56 GMT 2014 java.lang.InterruptedException: sleep interrupted\n" +
                "sleep interrupted\n" +
                " java.lang.InterruptedException: sleep interrupted\n" +
                "\tat java.lang.Thread.sleep(Native Method)\n" +
                "\tat com.liquidlabs.vso.agent.ScriptExecutor$ScriptRunnable.run(ScriptExecutor.java:224)\n" +
                "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:471)\n" +
                "\tat java.util.concurrent.FutureTask.run(FutureTask.java:262)\n" +
                "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThre");
        assertTrue("No Results", results.size() > 0);
        assertEquals("java.lang.InterruptedException", results.get("_exception"));
    }

    @Test
    public void testURL() throws Exception {
        Map<String,String> results = grokIt.processLine(filename, "2014-02-12 03:37:20,664 INFO netty-reply-24-22 (VAuditLogger)\t module:LookupSpace Register:ServiceInfo name:ResourceSpace_ALLOC zone:lab uri[stcp://10.28.1.160:11003/?svc=SHARED&host=LAB-UK-XS-UB1&_startTime=11-Feb-14_17-08-34&udp=0] iface[null] added[] rep[stcp://10.28.1.160:11022?svc=ResourceSpace_ALLOC] agent:lab.Management");
        assertTrue("No Results", results.size() > 0);
        assertEquals("stcp://10.28.1.160:11003/", results.get("_url"));
    }

    @Test
    public void testURLHost() throws Exception {
        Map<String,String> results = grokIt.processLine(filename, "2014-02-12 03:37:20,664 INFO netty-reply-24-22 (VAuditLogger)\t module:LookupSpace Register:ServiceInfo name:ResourceSpace_ALLOC zone:lab uri[stcp://10.28.1.160:11003/?svc=SHARED&host=LAB-UK-XS-UB1&_startTime=11-Feb-14_17-08-34&udp=0] iface[null] added[] rep[http://10.28.1.160:11022?svc=ResourceSpace_ALLOC] agent:lab.Management");
        assertTrue("No Results", results.size() > 0);
        assertEquals("10.28.1.160:11003/", results.get("_urlHost"));
    }

    @Test
    public void testLevel() throws Exception {
        Map<String,String> results = grokIt.processLine(filename, "2014-02-12 03:37:20,664 INFO netty-reply-24-22 (VAuditLogger)\t module:LookupSpace Register:ServiceInfo name:ResourceSpace_ALLOC zone:lab uri[stcp://10.28.1.160:11003/?svc=SHARED&host=LAB-UK-XS-UB1&_startTime=11-Feb-14_17-08-34&udp=0] iface[null] added[] rep[stcp://10.28.1.160:11022?svc=ResourceSpace_ALLOC] agent:lab.Management");
        assertTrue("No Results", results.size() > 0);
        assertEquals("INFO", results.get("_level"));
    }

    @Test
    public void testPath() throws Exception {
//        String badLine = "<font color='#0000FF' size='12'>java.class.path - .::/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib/flex-messaging-common.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib/flex-messaging-core.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib/flex-messaging-opt.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib/flex-messaging-proxy.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib/flex-messaging-remoting.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib/flex-rds-server.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib/groovy-all-1.8.7.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/lib/jdk7-introspector.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/boot:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/boot/lib/common.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/boot/lib/transport.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/boot/lib/vs-orm.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/boot/lib/vso.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/boot/lib/vspace.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/boot/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/lib-1.0:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/lib-1.0/lib/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/lib-1.0/thirdparty/snappy-java-1.1.0.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/lib-1.0/thirdparty/thirdparty-all.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/lib-1.0/thirdparty/xpp3_min-1.1.4c.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/lib-1.0/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/replicator-1.0:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/replicator-1.0/lib/replicator.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/replicator-1.0/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-admin-1.0:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-admin-1.0/config/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-admin-1.0/lib/admin-all.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-admin-1.0/lib/vs-admin.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-admin-1.0/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-log-1.0:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-log-1.0/lib/vs-log.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-log-1.0/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-log-server-1.0:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-log-server-1.0/lib/vs-log-server.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-log-server-1.0/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-syslog-server-1.0:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-syslog-server-1.0/lib/syslog4j-0.9.46.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-syslog-server-1.0/lib/vs-syslog-server.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-syslog-server-1.0/*:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-util-1.0:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-util-1.0/lib/vs-util.jar:/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/system-bundles/vs-util-1.0/*</font>";
        String goodLine = "2014-02-12 03:37:20,664/etc/home INFO netty-reply-24-22 (VAuditLogger)\t module:LookupSpace Register:ServiceInfo name:ResourceSpace_ALLOC zone:lab uri[stcp://10.28.1.160:11003/?svc=SHARED&host=LAB-UK-XS-UB1&_startTime=11-Feb-14_17-08-34&udp=0] iface[null] added[] rep[stcp://10.28.1.160:11022?svc=ResourceSpace_ALLOC] agent:lab.Management";
        String line = goodLine;
        Map<String,String> results = grokIt.processLine(filename, line);
        assertTrue("No Results", results.size() > 0);
        assertNotNull("/etc/home", results.get("_gpath"));
    }

}
