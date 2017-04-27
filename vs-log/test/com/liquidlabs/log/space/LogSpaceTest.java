package com.liquidlabs.log.space;

import com.liquidlabs.admin.User;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.links.Link;
import com.liquidlabs.log.roll.ContentBasedSorter;
import com.liquidlabs.log.roll.NullFileSorter;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.thoughtworks.xstream.XStream;
import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class LogSpaceTest extends MockObjectTestCase {

    private LogSpaceImpl logSpace;
    private ORMapperFactory mapperFactory;
    private Mock scheduler;
    private Mock lookupSpace;
    private String hosts;
    private Mock resourceSpace;
    private String hostname;
    private int maxAgeDays;
    private boolean isDW;
    FieldSet fieldSet = FieldSets.get();
    private String breakRule;
    private boolean overwrite;
    private boolean systemFieldsEnabled;
    User user = new User("user", "", "pwd", 123l, "group", 999l, "whenever", "", "", "usergroup", null, "", "", User.ROLE.Read_Write_User);

    @Override
    protected void setUp() throws Exception {
        VSOProperties.setResourceType("Management");
        VSpaceProperties.setBaseSpaceDir("build/space");
        new File(VSpaceProperties.baseSpaceDir(), "touch.file").delete();
        lookupSpace = mock(LookupSpace.class);
        scheduler = mock(ScheduledExecutorService.class);
        scheduler.stubs();
        resourceSpace = mock(ResourceSpace.class);
        resourceSpace.stubs();
        mapperFactory = new ORMapperFactory();
        SpaceServiceImpl spaceServiceImpl1 = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), mapperFactory, LogSpace.NAME, (ScheduledExecutorService) scheduler.proxy(), false, false, false);
        SpaceServiceImpl spaceServiceImpl2 = new SpaceServiceImpl((LookupSpace) lookupSpace.proxy(), mapperFactory, LogSpace.NAME, (ScheduledExecutorService) scheduler.proxy(), false, false, true);
        logSpace = new LogSpaceImpl(spaceServiceImpl1, spaceServiceImpl2, null, null, null, null, null, (ResourceSpace) resourceSpace.proxy(), (LookupSpace) lookupSpace.proxy());
        logSpace.llc = true;
    }

    public void testShouldListMyDataSource() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.addWatch("usergroup.Test","-","-","-","-","-",1,"-",true,"-","-",false,false);
        List<WatchDirectory> w1 = logSpace.watchDirectories(user, "", false);
        List<WatchDirectory> w2 = logSpace.watchDirectories(null, "", false);

        User user = new User("user", "", "pwd", 123l, "group", 999l, "whenever", "", "", "guest", null, "", "", User.ROLE.Read_Write_User);
        List<WatchDirectory> w3 = logSpace.watchDirectories(user, "", false);
        assertTrue(w1.contains("usergroup.Test"));
        assertFalse(w3.contains("usergroup.Test"));


    }
    public void testShouldSaveAsUserAccount() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.saveSearch(new Search("user-added", "Damian", Arrays.asList( ".*AllWindows.*"), ".*.log", Arrays.asList( 5),5,""), user);
        List<String> searches = logSpace.listSearches(user);
        assertTrue(searches.contains("usergroup.user-added"));


    }

    public void testShouldExportUsingTypeFilterDataSource() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.addWatch("Dam", ".","xxx","","","",1, "", false,"","",true, systemFieldsEnabled);

        String orig = logSpace.exportConfig("Source:dam");
        assertTrue("Export Failed:" + orig, orig.indexOf("Dam") > 0);
    }

    public void testShouldExportUsingTypeFilter() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.saveSearch(new Search("Damos Report", "Damian", Arrays.asList( ".*AllWindows.*"), ".*.log", Arrays.asList( 5),5,""), user);
        String orig = logSpace.exportConfig("Search:Dam");
        assertTrue("Export Failed:" + orig, orig.indexOf("Dam") > 0);
    }

    public void testShouldImportExportWorkspace() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.saveWorkspace("My Workspace", "CONTENT: \" here \"", user);
        String xml = logSpace.exportConfig("my");
        logSpace.deleteWorkspace("My Workspace", user);
        assertNull(logSpace.getWorkspace("My Workspace", user));
        logSpace.importConfig(xml, true, true);
        assertNotNull(logSpace.getWorkspace("My Workspace", user));


    }


    // DodgyTest? Maybe put the file in a location whtat can be accessed by everyone??
    public void xxxtestShouldUpgradeFields() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        String config = FileUtil.readAsString("/Volumes/Media/LOGSCAPE/TRUNK/logscapeApps/repo/LogScapeMonitorApp-1.0/logscape-mon-system.config");
        logSpace.importConfig(config,false,true);
        FieldSet fs = logSpace.getFieldSet("logscape");
        fs.upgrade();
        assertTrue(fs.fields().size() > 0);
    }


    public void testShouldDoWorkspaceStuff() throws Exception {

        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.saveWorkspace("ws1","stuff", user);
        Workspace ws1 = logSpace.getWorkspace("ws1", user);
        assertNotNull(ws1);
        List<String> list = logSpace.listWorkSpaces(user);
        assertTrue(list.toString().equals("[ws1]"));
        logSpace.deleteWorkspace("ws1", user);
        assertNull("Workspace still exists",logSpace.getWorkspace("ws1", user));
        assertTrue(logSpace.listWorkSpaces(user).size() == 0);



    }

//    @Test
//	public void testShouldLoadSearchWithFunnyExpression() throws Exception {
//    	lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
//    	logSpace.start();
//    	logSpace.saveSearch(new Search("TEST","O", Arrays.asList("type='log4j' all  | pctUtil.avg(server) pctUtil.gt({0}) chartMax(100) bucketWidth(5m) chart(clustered) "), "*", Arrays.asList(1),100,""));
//
//    	JReport printReport = logSpace.getPrintReport("Sample Report");
//    	printReport.searchName = "TEST";
//    	logSpace.savePrintReport(printReport);
//
//    	JReport printReport2 = logSpace.getPrintReport("Sample Report");
//    	assertNotNull("Didnt find default PrintReport", printReport2);
//    	assertTrue(printReport2.visibleFields.length() > 0);
//
//	}

//    @Test
//	public void testShouldHaveJReportWithFields() throws Exception {
//    	lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
//    	logSpace.start();
//    	JReport printReport = logSpace.getPrintReport("Sample Report");
//    	assertNotNull("Didnt find default PrintReport", printReport);
//    	assertTrue(printReport.visibleFields.length() > 0);
//
//    	// now add a field - store and get it out again
//    	printReport.visibleFields = "level";
//    	logSpace.savePrintReport(printReport);
//    	JReport printReport2 = logSpace.getPrintReport("Sample Report");
//    	assertNotNull("Didnt find default PrintReport", printReport2);
//    	assertTrue(printReport2.visibleFields.length() > 0);
//
//	}

    @Test
    public void testShouldListDataSourcesByFilter() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();

        logSpace.addWatch("coh5", "/opt/coh5", "*.txt", "", "", "host1,host2", 7, "", false, "Default", null, false, systemFieldsEnabled);
        logSpace.addWatch("coh55", "/opt/coh55", "*.txt", "", "", "host1,host2", 7, "", false, "Default", null, false, systemFieldsEnabled);
        logSpace.addWatch("coh6", "/opt/coh6/stuff/etc", "*.txt", "", "", "host1,host2", 7, "", false, "Default", null, false, systemFieldsEnabled);
        List<WatchDirectory> result1 = logSpace.watchDirectoriesWithFilter("tag:coh5");
        assertEquals(1, result1.size());
        assertEquals("coh5", result1.get(0).tags);

        List<WatchDirectory> result2 = logSpace.watchDirectoriesWithFilter("tag:coh6");
        assertEquals(1, result2.size());
        assertEquals("coh6", result2.get(0).tags);

        List<WatchDirectory> result3 = logSpace.watchDirectoriesWithFilter("*coh6*");
        assertEquals(1, result3.size());
        assertEquals("coh6", result3.get(0).tags);
    }

    public void testShouldImportShouldSupportOverwrite() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        // first
        logSpace.saveSearch(new Search("Neils Report", "Neil", Arrays.asList( "ORIG"), "FILES.log", Arrays.asList( 5),5,""), user);
        String orig = logSpace.exportConfig("");

        // replace with something to overwrite
        logSpace.saveSearch(new Search("Neils Report", "Neil", Arrays.asList( "REPLACED"), "OVERWRITEME.log", Arrays.asList( 5),5,""), user);

        // overwrite it
        logSpace.importConfig(orig, true, true);

        // now check that it is reverted
        Search search = logSpace.getSearch("Neils Report", user);
        assertTrue(search.patternFilter.get(0).contains("ORIG"));
    }

    public void testShouldExpandResourceGroup() throws Exception {
        resourceSpace.expects(once()).method("expandGroupIntoHostnames").withAnyArguments().will(returnValue(new HashSet<String>(Arrays.asList("myHost"))));
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        String list = "group:MyGroup";
        Set<String> hosts = logSpace.expandHostsString(list);
        assertEquals("[myHost]", hosts.toString());
    }
    public void testShouldExpandResourceGroupWithHostName() throws Exception {
        resourceSpace.expects(once()).method("expandGroupIntoHostnames").withAnyArguments().will(returnValue(new HashSet<String>(Arrays.asList("myHost"))));
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));

        logSpace.start();
        String list = "host1, group:MyGroup,host2";
        Set<String> hosts = logSpace.expandHostsString(list);
        assertEquals(new HashSet<String>(Arrays.asList("myHost","host2", "host1")), hosts);
    }

    public void testShouldReturnEmpty() {
        resourceSpace.expects(once()).method("expandGroupIntoHostnames").withAnyArguments().will(returnValue(Collections.emptySet()));
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));

        logSpace.start();

        Set<String> hosts = logSpace.expandHostsString("group:abcde");
        assertTrue(hosts.isEmpty());
    }
    public void testShouldStart() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
    }

    public void testShouldStop() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        lookupSpace.expects(once()).method("unregisterService").withAnyArguments().will(returnValue(true));
        lookupSpace.expects(once()).method("unregisterService").withAnyArguments().will(returnValue(true));
        logSpace.start();
        logSpace.stop();
    }

    public void testShouldHandleFieldSets() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.saveFieldSet(FieldSets.getLog4JFieldSet());
        FieldSet fieldSet1 = logSpace.getFieldSet(FieldSets.getLog4JFieldSet().getId());


        FieldSet basicFieldSet = logSpace.getFieldSet("basic");
        assertEquals("basic", basicFieldSet.id);
    }

    public void testShouldHandleLinkItemsProperly() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();

        List<Link> links = logSpace.getLinks();
        int firstSize = links.size();
        logSpace.createLink("Exception Report", ".*Exception.*", 1, -1);
        links = logSpace.getLinks();
        assertEquals(firstSize + 1, links.size());

        assertNotNull(logSpace.getLinkForSearch("Exception Report"));
    }

    public void testShouldDeleteLinkWhenSearchIsDeleted() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();

        String searchName = "TestDeleteSearch";
        logSpace.createLink(searchName, ".*Exception.*", 1, -1);
        logSpace.saveSearch(new Search("TestDeleteSearch", "Damian", Arrays.asList( ".*AllWindows.*"), ".*.log", Arrays.asList( 5),5,""), user);
        assertNotNull(logSpace.getLinkForSearch(searchName));
        logSpace.deleteSearch(searchName, user);
        assertNull(logSpace.getLinkForSearch(searchName));
    }

    public void testShouldDeleteOrphanedLinks() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();

        String searchName = "OrphanedTestDeleteSearch";
        logSpace.createLink(searchName, ".*Exception.*", 1, -1);
        int deleteOrphanedLinks = logSpace.deleteOrphanedLinks();
        assertTrue(deleteOrphanedLinks == 1);
    }

    public void testShouldStoreAndRetrieveRemoteWatchDirectory() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.addWatch("", ".", ".*\\.log", "timeformat", ContentBasedSorter.class.getName(), hosts, maxAgeDays, "", isDW, breakRule, null, false, systemFieldsEnabled);
        logSpace.addWatch("", "D:\\HPC\\DataSynapse\\Engine\\work\\", ".*\\.log", null, NullFileSorter.class.getName(), hosts, maxAgeDays, "", isDW, breakRule, null, false, systemFieldsEnabled);

        ProxyFactoryImpl proxyFactory = new ProxyFactoryImpl(8090, Executors.newCachedThreadPool(), "logSpaceTest");
        proxyFactory.start();
        LogSpace remoteLogSpace = proxyFactory.getRemoteService(LogSpace.NAME, LogSpace.class, mapperFactory.getProxyFactory().getAddress().toString());

        LogConfiguration configuration = remoteLogSpace.getConfiguration(hostname);

        List<WatchDirectory> watchDirectories = configuration.watching();

        proxyFactory.stop();

        boolean pass = false;
        for (WatchDirectory watchDirectory : watchDirectories) {
            if (watchDirectory.getFileSorter().getClass().getName().equals(NullFileSorter.class.getName())) {
                pass = true;
            }
        }
        assertTrue("Failed to get Null sorter in:" + watchDirectories, pass);
    }

    //    public void testShouldStoreAndAllowDeleteOfDodgyWatch() throws Exception {
//    	lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
//    	logSpace.start();
//    	logSpace.addWatch("", "D:\\HPC\\Program Files\\Engine\\work\\", ".*.log", null, false, null, NullFileSorter.class.getName(), hosts, maxAgeDays, isDW, breakRule, null);
//    	logSpace.addWatch("", "D:\\HPC\\Program Files\\Engine\\work\\", "*.log", null, true, null, NullFileSorter.class.getName(), hosts, maxAgeDays, isDW, breakRule, null);
//    	List<WatchDirectory> watchDirectories = logSpace.getConfiguration(hostname).watching();
//    	assertEquals(2, watchDirectories.size());
//    	logSpace.removeWatch(hosts);
//    	List<WatchDirectory> watchDirectories2 = logSpace.getConfiguration(hostname).watching();
//    	assertEquals(1, watchDirectories2.size());
//    }
    public void testShouldStoreAndRetrieveWatchDirectory() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.addWatch("", ".", ".*\\.log", "timeformat", ContentBasedSorter.class.getName(), hosts, maxAgeDays, "", false, breakRule, null, false, systemFieldsEnabled);
        logSpace.addWatch("", "D:\\HPC\\DataSynapse\\Engine\\work\\", ".*\\.log", null, NullFileSorter.class.getName(), hosts, maxAgeDays, "", false, breakRule, null, false, systemFieldsEnabled);
        List<WatchDirectory> watchDirectories = logSpace.getConfiguration(hostname).watching();
        boolean pass = false;
        for (WatchDirectory watchDirectory : watchDirectories) {
            if (watchDirectory.getFileSorter().getClass().getName().equals(NullFileSorter.class.getName())) {
                pass = true;
            }
        }
        assertTrue("Failed to get Null sorter in:" + watchDirectories, pass);
    }

    public void testShouldStoreListRetrieveAndDeleteSearchSet() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        int orig = logSpace.listDashboardNames(new HashSet<String>(Arrays.asList("owner"))).size();
        logSpace.storeDashboard(new SearchSet("me", new ArrayList<String>(),"owner",2, 1));
        assertEquals(orig + 1, logSpace.listDashboardNames(new HashSet<String>(Arrays.asList("owner"))).size());
        assertNotNull(logSpace.getDashboard("me"));
        logSpace.deleteDashboard("me");
        assertEquals(orig , logSpace.listDashboardNames(new HashSet<String>(Arrays.asList("owner"))).size());
    }

    public void testShouldExport() {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        String xmlData = logSpace.exportConfig("");
        assertNotNull(xmlData);

        String xmlData2 = logSpace.exportConfig("ds");
        // should not have 2 Hourly Exceptions
        assertFalse(xmlData2.contains("Hourly"));
    }

    // turning off cause the UTF on svn checkout breaks the build
    public void XXXtestShouldLoadInThisStringOfOld() throws Exception {
        String search ="<map> <entry>\n" +
                "    <string>com.liquidlabs.log.report.Report-DEMO - CH left Cluster 4</string>\n" +
                "    <string>com.liquidlabs.log.report.Report�0�.*coherence.*.log�DEMO - CH left Cluster 4�admin�java.lang.Integer%C2%B51%5Ejava.lang.Integer%C2%B54%5E�java.lang.String%C2%B5_URL_ENCODED_.*TcpRing%253A%2528.*%2529%253B%2528.*%2529%253B_PL%26US_removing_PL%26US_%2528.*%2529%253A%2528.*%2529_PL%26US_%257C_PL%26US_count%2528LeftReasons%252C1%2529%5E�2880�false�DEMO - CH left Cluster 4�O_STR�</string>\n" +
                "  </entry></map>";

        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.importConfig(search, true, overwrite);
        Search search2 = logSpace.getSearch("DEMO - CH left Cluster 4", user);
        List<String> searchNames = logSpace.listSearches(null);
        for (String string : searchNames) {
            System.out.println(string);
        }

        assertNotNull("didnt find it:" + searchNames, search2);

    }

    public void testShouldExportUsingFilter() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        logSpace.saveSearch(new Search("Damos Report", "Damian", Arrays.asList( ".*AllWindows.*"), ".*.log", Arrays.asList( 5),5,""), user);
        String orig = logSpace.exportConfig("Dam");
        assertTrue("Export Failed:" + orig, orig.indexOf("Dam") > 0);
    }

    public void testShouldAlsoMergeReports() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        String orig = logSpace.exportConfig("");
        int origReportSize = logSpace.listSearches(null).size();
        clearOutSpace();
        logSpace.saveSearch(new Search("Damos Report", "Damian", Arrays.asList( ".*AllWindows.*"), ".*.log", Arrays.asList( 5),5,""), user);
        logSpace.importConfig(orig, true, true);
        assertEquals(origReportSize + 1, logSpace.listSearches(null).size());
        assertNotNull(logSpace.getSearch("Damos Report", user));
    }
    public void testShouldDeleteConfigData() throws Exception {
        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        String orig = logSpace.exportConfig("");
        int origReportSize = logSpace.listSearches(null).size();
        clearOutSpace();
        logSpace.saveSearch(new Search("Damos Report", "Damian", Arrays.asList( ".*AllWindows.*"), ".*.log", Arrays.asList( 5),5,""), user);
        logSpace.importConfig(orig, true, overwrite);
        String exportConfig = logSpace.exportConfig("Damos");
        assertNotNull(exportConfig);
        assertTrue(exportConfig.contains("Damos"));
        logSpace.removeConfig(exportConfig);

        int origReportSizeAgains = logSpace.listSearches(null).size();
        assertEquals(origReportSize, origReportSizeAgains);
    }

    private void clearOutSpace() {
        try {
            logSpace.importConfig(emptyXStreamedmap(), false, overwrite);
            logSpace.saveFieldSet(FieldSets.getBasicFieldSet());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testShouldImportClean() {
        Set<String> userIds = new HashSet<String>(Arrays.asList("admin"));

        lookupSpace.expects(atLeastOnce()).method("registerService").withAnyArguments().will(returnValue("stuff"));
        logSpace.start();
        clearOutSpace();
        assertEquals(0, logSpace.getSchedules("").size());
    }

    private String emptyXStreamedmap() {
        return new XStream().toXML(new HashMap<String, String>());
    }

    static class MyLogReplayListener implements LogReplayHandler {
        public String getId() {
            return "myId";
        }

        public void handle(ReplayEvent event) {
        }

        public void handle(Bucket event) {
        }

        public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
            return 1;
        }

        public int handle(List<ReplayEvent> events) {
            return 100;
        }

        public int status(String provider, String subscriber, String msg) {
            return 1;
        }

        public void handleSummary(Bucket bucketToSend) {
        }
    }

    public static class MyLogEventListener implements LogEventListener {
        public void handle(LogEvent event) {
        }

        public String getId() {
            return "myId";
        }
    }
}
