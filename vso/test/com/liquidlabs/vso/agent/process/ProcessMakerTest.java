package com.liquidlabs.vso.agent.process;

import junit.framework.TestCase;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessMakerTest extends TestCase {

    private ProcessMaker processMaker;
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        new File("build/work").mkdirs();
        processMaker = new ProcessMaker("./", "build/work", false ,null, true);
    }


    public void testStrippingOfUnwantedJars() throws Exception {
        String path = ".::/WORK/Logscape_2.5.0/master/build/logscape/lib:/WORK/Logscape_2.5.0/master/build/logscape/lib/groovy-all-1.8.7.jar:/WORK/Logscape_2.5.0/master/build/logscape/lib/jdk7-introspector.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/boot:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/boot/lib/common.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/boot/lib/transport.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/boot/lib/vs-orm.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/boot/lib/vso.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/boot/lib/vspace.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/boot/*:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/lib/groovy-all-1.8.7.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/lib/jdk7-introspector.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-analyzers-common-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-codecs-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-core-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-grouping-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-highlighter-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-join-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-memory-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-misc-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-queries-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-queryparser-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-sandbox-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-spatial-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/lucene-suggest-4.7.2.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/snappy-java-1.1.0.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/thirdparty-all.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/thirdparty/xpp3_min-1.1.4c.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/lib-1.0/*:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/replicator-1.0:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/replicator-1.0/lib/replicator.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/replicator-1.0/*:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-admin-1.0:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-admin-1.0/config/*:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-admin-1.0/lib/admin-all.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-admin-1.0/lib/vs-admin.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-admin-1.0/*:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-log-1.0:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-log-1.0/lib/vs-log.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-log-1.0/*:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-log-server-1.0:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-log-server-1.0/lib/vs-log-server.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-log-server-1.0/*:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-syslog-server-1.0:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-syslog-server-1.0/lib/syslog4j-0.9.46.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-syslog-server-1.0/lib/vs-syslog-server.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-syslog-server-1.0/*:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-util-1.0:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-util-1.0/lib/vs-util.jar:/WORK/Logscape_2.5.0/master/build/logscape/system-bundles/vs-util-1.0/*";
        path = path.replace(":", File.pathSeparator);

        String cp = processMaker.getFilteredClassPath(path,File.pathSeparator);
        System.out.println(cp.replace(":","\n"));
        assertFalse(cp.contains("luce"));
    }

    public void testClassPathExtraction() throws Exception {
        String cp = processMaker.getJava6Classpath(new String[] { "a:b:c", "d:e:f"});
        assertEquals(String.format("a%sb%sc%sd%se%sf%s",File.pathSeparator,File.pathSeparator,File.pathSeparator,File.pathSeparator, File.pathSeparator, File.pathSeparator), cp);;

    }

    public void testShouldBeAbleToSortoutSLAContainerStuffAndCreateMeOne() throws Exception {
        Map<String, Object> variables = new HashMap<String, Object>();
        ProcessMaker processMaker2 = new ProcessMaker(".", "build/work", false, variables, true );
        processMaker2.runSLAContainer(new String[] { "-cp:lib/*.jar:.:../lib/*.jar", "-Dlog4j.debug=false", "-consumer:com.liquidlabs.flow.sla.FlowConsumer", "-serviceToRun:pooooo", "-sla:sla.xml", "-template:WorkerTemplate" });

    }

    public void testShouldLookupSTARInPathFileItem() throws Exception {
        String path = "../lib/*/*.jar";
        List<String> lookupFileItems = processMaker.lookupFileItems(path);
        System.out.println(lookupFileItems);
        assertTrue(lookupFileItems.size() > 0);
    }

    public void testShouldLookupDotDotFileItem() throws Exception {
        String path = "../lib/lib/*.jar";
        List<String> lookupFileItems = processMaker.lookupFileItems(path);
        System.out.println(lookupFileItems);
        assertTrue(lookupFileItems.size() > 0);
    }


    public void testShouldExtractJavaClassPathProperly() throws Exception {
        String[] sampleCP = new String[] { "../lib/lib/*.jar", "*.jar" };
        String processClassPath = processMaker.processClassPath(sampleCP);
        System.out.println(processClassPath);
        assertTrue(processClassPath.length() > 0);
        assertTrue(processClassPath.contains("lib"));
    }
}
