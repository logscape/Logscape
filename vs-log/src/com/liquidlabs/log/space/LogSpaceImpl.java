package com.liquidlabs.log.space;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.User;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.log.CancellerListener;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.TailerListener;
import com.liquidlabs.log.alert.AlertScheduler;
import com.liquidlabs.log.alert.AlertSchedulerJmxAdmin;
import com.liquidlabs.log.alert.Schedule;
import com.liquidlabs.log.explore.Explore;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.jreport.JReport;
import com.liquidlabs.log.jreport.JReportRunner;
import com.liquidlabs.log.jreport.ReportRunner;
import com.liquidlabs.log.links.Link;
import com.liquidlabs.log.roll.RolledFileSorter;
import com.liquidlabs.log.space.agg.AggEngineState;
import com.liquidlabs.log.space.agg.AggSpaceManager;
import com.liquidlabs.log.space.agg.AggSpaceManagerImpl;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.space.VSpaceProperties;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.Notifier;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.VSOProperties.ports;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.resource.ResourceGroup;
import com.liquidlabs.vso.resource.ResourceSpace;
import com.liquidlabs.vso.resource.ResourceSpaceImpl;
import com.liquidlabs.vso.work.WorkAllocatorImpl;
import com.thoughtworks.xstream.XStream;
import javolution.util.FastMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogSpaceImpl implements LogSpace {

    private static String CONFIG_START = "<!-- LogSpace Config Start -->";
    private static String CONFIG_END = "<!-- LogSpace Config End -->";

    private static final String XML_2_0 = "<!--xml-2.0-->";
    private static final Logger LOGGER = Logger.getLogger(LogSpaceImpl.class);
    public static final String CDATA = "<![CDATA[\n";
    public static final String CD_END = "]]>";

    final SpaceService logDataService;

    private final String[] liveIncludeFilters;
    private final String[] liveExcludeFilters;

    AlertScheduler logScheduler;
    private ScheduledExecutorService scheduler;

    private final AdminSpace adminSpace;

    private final AggSpace aggSpace;

    private final SpaceService logEventService;

    private final AggSpaceManager aggSpaceManager;

    private final ResourceSpace resourceSpace;

    FieldSet fieldSet = FieldSets.get();
    private final LookupSpace lookupSpace;
    State state = State.STOPPED;
    boolean llc = false;

    public LogSpaceImpl(SpaceService dataSpaceService, SpaceService logEventSpace, AdminSpace adminSpace, AggSpace aggSpace, AggSpaceManager aggSpaceManager, String[] liveIncludeFilters, String[] liveExcludeFilters, ResourceSpace resourceSpace, LookupSpace lookupSpace) {
        this.logDataService = dataSpaceService;
        this.logEventService = logEventSpace;
        this.adminSpace = adminSpace;
        this.aggSpaceManager = aggSpaceManager;
        this.aggSpace = aggSpace;
        this.liveIncludeFilters = liveIncludeFilters;
        this.liveExcludeFilters = liveExcludeFilters;
        this.resourceSpace = resourceSpace;
        this.lookupSpace = lookupSpace;
        this.scheduler = ExecutorService.newScheduledThreadPool(3, "logSpace");
    }

    public void start() {
        LOGGER.info("Starting LogSpace");
        logDataService.start(this, "vs-log-1.0");
        logEventService.start(this, "vs-log-1.0");
        boolean firstTime = firstTime();

        try {
            boolean managerOnly = VSOProperties.isManagerOnly() || VSOProperties.isFailoverNode();
            if (managerOnly && firstTime) {
                LogFilters existingFilters = logDataService.findById(LogFilters.class, LogFilters.key);
                if (existingFilters == null) {
                    LogFilters config = new LogFilters(liveIncludeFilters, liveExcludeFilters);
                    setLiveLogFilters(config);
                }

            } else {
                if (!managerOnly) LOGGER.info(" **************** LOGSPACE is NOT Management - it will not add DEFAULT DataTypes and DataSources");
            }
            if (managerOnly) {
                Workspace home = logDataService.findById(Workspace.class, "Home");
                if (home == null) {
                    LOGGER.info("Loading Default downloads/*.config files");
                    String[] configFiles = new File("downloads").list(new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                            return name.endsWith(".config");
                        }
                    });
                    if (configFiles != null) {
                        for (String configFile : configFiles) {
                            String content = FileUtil.readAsString("downloads/" + configFile);
                            if (content != null) {
                                LOGGER.info("Importing Config " + configFile);
                                try {
                                    importConfig(content,false, true);
                                } catch (Throwable t) {
                                    t.printStackTrace();;
                                    LOGGER.error("Failed to loadConfig:" + configFile, t);
                                }
                            }
                        }
                    }
                }

            }

            logScheduler = new AlertScheduler(this, aggSpace, adminSpace, logDataService, lookupSpace, logDataService.getServiceInfo(), resourceSpace);
            logScheduler.registerEventListener(logDataService);
            logScheduler.start();

            admin = new AlertSchedulerJmxAdmin(logScheduler);


            if (managerOnly && firstTime) {

                upgradeOldDataSources();
                upgradeNotSoOldDataSources();

                deleteOrphanedLinks();
                addDefaultFieldSets();
            }
            upgradeFieldSets();

            ResourceGroupListenerImpl listener = new ResourceGroupListenerImpl(this, scheduler);
            if (resourceSpace != null) {
                resourceSpace.resourceGroupListener(listener, listener.getId());
            } else {
                LOGGER.warn("ResouceSpace is null");
            }

            this.llc = adminSpace.getLLC(true) != -1;

            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    LogSpaceImpl.this.llc = adminSpace.getLLC(true) != -1;
                }
            }, 1, 1, TimeUnit.MINUTES);
        } catch (Throwable t) {
            LOGGER.fatal("Start LogSpace Failed", t);
        } finally {
            state = State.RUNNING;
        }

    }

    private void upgradeFieldSets() {
        LOGGER.info("Checking to UpgradingFieldSets size:" + getFieldSets().size());
        for (FieldSet lsLog : getFieldSets()) {
            if (lsLog.upgrade()){
                saveFieldSet(lsLog);
            }
        }
    }

    private boolean firstTime() {
        try {
            return new File(VSpaceProperties.baseSpaceDir(), "touch.file").createNewFile();
        } catch (IOException e) {
            return true;
        }
    }

    private void upgradeOldDataSources() {
        List<WatchDirectory> watchDirectories = watchDirectories(null, "", false);
        for (WatchDirectory watch : watchDirectories) {
            String oldKey = watch.dirName + watch.filePattern;
            String newKey = WatchDirectory.getPreviousKeyPattern(watch.dirName, watch.filePattern, watch.hosts);
            if (!oldKey.equals(newKey)) {
                logDataService.remove(WatchDirectory.class, oldKey);
                LOGGER.info("Upgrading WATCH:" + watch);
                saveWatch(watch);
            }
        }
    }

    private void upgradeNotSoOldDataSources() {
        List<WatchDirectory> watchDirectories = watchDirectories(null, "", false);
        for (WatchDirectory watch : watchDirectories) {
            String currentKey = watch.wdId;
            String oldKeyStyle = WatchDirectory.getPreviousKeyPattern(watch.dirName, watch.filePattern, watch.hosts);

            if (!currentKey.equals(oldKeyStyle)) {
                logDataService.remove(WatchDirectory.class, currentKey);
                LOGGER.info("Upgrading WATCH:" + watch);
                watch.wdId = UUID.randomUUID().toString();
                saveWatch(watch);
            }
        }
    }

    public String executeSchedule(String name) {
        try {
            return logScheduler.manuallyRun(name);
        } catch (Throwable t) {
            return "Error:" + t.toString();
        }
    }
    public void setLogAlertEnableFlag(String name, String enableOrDisable) {
        Schedule schedule = getSchedule(name);
        if (schedule != null) {
            String existingTrigger = schedule.trigger;
            if (existingTrigger == null) existingTrigger = "";
            if (existingTrigger.startsWith("-") && enableOrDisable.equals("enable")) {
                existingTrigger = existingTrigger.substring(1);
            } else if (!existingTrigger.startsWith("-") && enableOrDisable.equals("disable")) {
                existingTrigger = "-" + existingTrigger;
            }
            schedule.trigger = existingTrigger;
            try {
                saveSchedule(schedule);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String printReportToPDF(String searchName, long fromTimeMs, long toTimeMs, String userId, String fileIncludes, String fileExcludes, int mode) {
        String description = "Generated by: "+ userId;
        LOGGER.info("Creating PDF:" + searchName + " => " + description);
        User user = adminSpace.getUser(userId);

        JReport printReport = new JReport(searchName,description, searchName,user.username(), user.department, user.getReportLogo(), true, "", mode, true);
        ReportRunner reportRunner =  getReportRunner(printReport,userId);
        return reportRunner.run(new DateTime(fromTimeMs), new DateTime(toTimeMs), user.department);
    }

    public void savePrintReport(JReport report) {
        LOGGER.info("Saving JReport:" + report.name);
        logDataService.store(report, -1);
    }

    public JReport getPrintReport(String name) {
        JReport foundReport = logDataService.findById(JReport.class, name);
        List<String> searchNames = listSearches(null);
        if (foundReport != null && searchNames.contains(foundReport.searchName)) {
            try {
                Search search = getSearch(foundReport.searchName, null);
                LogRequest logRequest = new LogRequestBuilder().getLogRequest("", search, "0,0,0,0,0,0");
            } catch (Throwable t) {
                t.printStackTrace();
                LOGGER.warn("Failed to load Search:" + foundReport.searchName, t);

            }
        }
        return foundReport;
    }

    public String[] listPrintReports(String department) {
        if (department.equals("all")) return logDataService.findIds(JReport.class, "");
        return logDataService.findIds(JReport.class, String.format("department equals '%s' OR department equals 'all'", department));
    }

    public void removePrintReport(String name) {
        LOGGER.info("Removing JReport:" + name);
        logDataService.remove(JReport.class, name);
    }
    public String emailPrintReport(String emailTo, String reportName){
//    	File pdfFile = new File(LogProperties.getReportDir() + reportName);
        File pdfFile = new File(LogProperties.getWebAppDir(LogProperties.getWebSslPort()) + "/" + reportName);
        if (!pdfFile.exists()) {
            String errorMsg = "Failed to locate Report:" + pdfFile.getAbsolutePath();
            return errorMsg;
        }
        return adminSpace.sendEmail(LogProperties.getEmailFrom(), Arrays.asList(emailTo.split(",")), "Logscape Report:" + reportName, "See PDF for:" + reportName, pdfFile.getAbsolutePath());
    }

    public String executePrintReport(String reportName, String fileIncludes, String fileExcludes, String execUser, String execDept, DateTime fromTime, DateTime toTime) {
        JReport reportToRun = logDataService.findById(JReport.class, reportName);
        if (reportToRun == null) return "FAIL:Failed to load Report:" + reportName;
        ReportRunner reportRunner = getReportRunner(reportToRun, execUser);
        long start = DateTimeUtils.currentTimeMillis();
        reportToRun.department = execDept;
        reportToRun.owner = execUser;

        User user = adminSpace.getUser(execUser);
        reportToRun.logo = user.getReportLogo();

        String msg = reportRunner.run(fromTime, toTime, user.department);
        long end = DateTimeUtils.currentTimeMillis();
        return String.format("Report finished, elapsed:%dms msg:%s", (end - start), msg);
    }

    private ReportRunner getReportRunner(JReport found, String userId) {
        return new JReportRunner(this, adminSpace,30,"",found.searchName,userId);
    }


    private void addDefaultFieldSets() {
//        saveFieldSetIfPossible(TestFieldSet.getCiscoASALog());
//        saveFieldSetIfPossible(TestFieldSet.getAccessCombined());
        saveFieldSetIfPossible(FieldSets.getSysLog());
//        saveFieldSetIfPossible(TestFieldSet.get2008EVTFieldSet());
        //saveFieldSetIfPossible(TestFieldSet.getNTEventLog());
        saveFieldSetIfPossible(FieldSets.getLog4JFieldSet());
        saveFieldSetIfPossible(FieldSets.getAgentStatsFieldSet());
        // always rewrite basic
        saveFieldSet(FieldSets.getBasicFieldSet());
    }

    private void saveFieldSetIfPossible(FieldSet fieldSet) {
        if (logDataService.findById(FieldSet.class, fieldSet.getId()) == null) {
            LOGGER.info("Saving FieldSet:" + fieldSet.getId());
            saveFieldSet(fieldSet);
        }
    }

    public void saveFieldSet(FieldSet fieldSet) {
        if (!fieldSet.validate()) throw new RuntimeException("Invalid DataType, validation Failed on:" + fieldSet);

        LOGGER.info("LS_EVENT:Save FieldSet:" + fieldSet.id);
        fieldSet.lastModified = DateTimeUtils.currentTimeMillis();
        logDataService.store(fieldSet, -1);
    }

    public void removeFieldSet(String fieldSetId) {
        LOGGER.info("LS_EVENT:Remove FieldSet:" + fieldSetId);
        if (fieldSetId.equals("basic")) {
            LOGGER.warn("Attempted to remove 'basic' fieldSet - this operation cannot be performed");
            return;
        }

        FieldSet remove = logDataService.remove(FieldSet.class, fieldSetId);
        if (remove == null) LOGGER.warn("Failed to remove fieldset:" + fieldSetId);
    }

    int deleteOrphanedLinks() {
        List<Link> links = getLinks();
        int count = 0;
        for (Link link : links) {
            Search search = getSearch(link.searchName, null);
            if (search == null) {
                deleteLinksForSearch(link.searchName);
                count++;
            }
        }
        return count;
    }

    public String exportConfig(String filterList) {
        LOGGER.info("Export Config, filter:" + filterList);
        Map<String, Object> map = logDataService.exportObjects("");
        List<String> keys = new ArrayList<String>(map.keySet());
        Map<String, Object> result = new TreeMap<String, Object>();
        String[] filters = filterList.split(",");
        for (String key : keys) {
            for (String filter : filters) {

                if (filter.contains(":")) {
                    String[] typeKey = filter.split(":");
                    String alternativeName =  getAlternativeType(key);
                    handleUserTypeFilter(map, result, key, typeKey, alternativeName);
                    if (key.contains("WatchDir") && StringUtil.containsIgnoreCase(typeKey[0],"Source")) {
                        WatchDirectory wd = (WatchDirectory) map.get(key);
                        if(typeKey.length == 2){
                            if (StringUtil.containsIgnoreCase(wd.getTags(),typeKey[1])) {
                                result.put(key, wd);
                            }
                        }
                    }

                } else {
                    Object value = map.get(key);
                    String cleanKey = cleanKey(key, WatchDirectory.class);
                    cleanKey = cleanKey(cleanKey, SearchSet.class);
                    cleanKey = cleanKey(cleanKey, Workspace.class);
                    cleanKey = cleanKey(cleanKey, Search.class);
                    cleanKey = cleanKey(cleanKey, Schedule.class);
                    cleanKey = cleanKey(cleanKey, JReport.class);
                    cleanKey = cleanKey(cleanKey, FieldSet.class);

                    if (StringUtil.containsIgnoreCase(cleanKey, filter) || key.contains("Watch") && StringUtil.containsIgnoreCase(value.toString(), filter)) {
                        result.put(key, value);
                    }
                }
            }
        }
        return CONFIG_START + "\n" + new XStream().toXML(result) + "\n" + CONFIG_END;
    }

    private String getAlternativeType(String key) {
        if ( key.contains("Workspace")) return "Workspace";
        if ( key.contains("Search")) return "Search";
        if ( key.contains("WatchDir")) return "DataSource";
        if ( key.contains("FieldSet")) return "DataType";
        if ( key.contains("Schedule")) return "Alert";
        return "";
    }

    private void handleUserTypeFilter(Map<String, Object> map, Map<String, Object> result, String key, String[] typeKey, String alternativeTypeName) {
        if (typeKey.length == 2 && (StringUtil.containsIgnoreCase(key,typeKey[0].trim()) || StringUtil.containsIgnoreCase(alternativeTypeName,typeKey[0].trim()))) {
            if (typeKey[1].equals("*") || StringUtil.containsIgnoreCase(key,typeKey[1].trim())) {
                result.put(key, map.get(key));
            }
        }
    }

    private String cleanKey(String key, Class<?> class1) {
        return key.replace(class1.getCanonicalName().replaceAll(".class", ""),"");
    }

    public void importConfig(String xmlConfig, boolean merge, boolean overwrite) throws Exception {
        LOGGER.info(String.format("LS_EVENT:ImportConfig  Merge:%b Overwrite:%b", merge, overwrite));

        // Collect pre-import data
        List<FieldSet> fieldSetsBefore = this.fieldSets();
        List<WatchDirectory> sourcesBefore = this.watchDirectories(null, null, false);

        List<String> existingSchedules = this.getScheduleNames("");

        xmlConfig = upgradeXmlConfig(xmlConfig);

        // Import

        if (xmlConfig.contains(CONFIG_START)) {
            String config = xmlConfig.substring(xmlConfig.indexOf(CONFIG_START), xmlConfig.indexOf(CONFIG_END));
            Map<String, Object> fromXML = (Map<String, Object>) new XStream().fromXML(xmlConfig);
            logDataService.importData2(fromXML, merge, overwrite);
        } else if (xmlConfig.startsWith(XML_2_0)) {
            xmlConfig = xmlConfig.substring(XML_2_0.length());
            Map<String, Object> fromXML = (Map<String, Object>) new XStream().fromXML(xmlConfig);
            logDataService.importData2(fromXML, merge, overwrite);
        } else {
            xmlConfig = fixOldXMLformat(xmlConfig);
            importConfigOld(xmlConfig, merge, overwrite);
        }

        Thread.sleep(1000);

        // Fire updates for any deltas that need to trigger event listeners
        List<WatchDirectory> sourcesAfter = this.watchDirectories(null, null, false);
        updateDataSourceDeltas(sourcesBefore, sourcesAfter);
        Thread.sleep(1000);

        // crap - but sometimes DSs can get missed
        updateDataSourceDeltas(sourcesBefore, sourcesAfter);

        Thread.sleep(1000);
        updateTypeChanges(fieldSetsBefore, this.fieldSets());
        if (getFieldSet(FieldSets.getBasicFieldSet().getId()) == null) saveFieldSet(FieldSets.getBasicFieldSet());

        updateScheduleDeltas(existingSchedules, !xmlConfig.contains("<enabled>"));
    }

    private String upgradeXmlConfig(String xmlConfig) {
        // try and upgrade old classes to their replacements
        xmlConfig = xmlConfig.replaceAll("com.liquidlabs.log.report.Report", "com.liquidlabs.log.space.Search");
        return xmlConfig;
    }

    public void importConfigOld(String xmlConfig, boolean merge, boolean overwrite) throws Exception {
        try {

            Map<String, String> map = (Map<String, String>) new XStream().fromXML(xmlConfig);
            LOGGER.info("LS_EVENT:ImportingConfig Size:" + map.size());
            logDataService.importData(map, merge, overwrite);

        } catch (Throwable t) {
            LOGGER.error("Failed to import properly", t);
            throw new RuntimeException("Import Failed", t);
        }
    }

    private String fixOldXMLformat(String xmlConfig) {
        xmlConfig = Config.revertEscapes(xmlConfig);
        xmlConfig = xmlConfig.trim();
        if (xmlConfig.startsWith("<entry>")) xmlConfig = "<map>" + xmlConfig + "</map>";
        return xmlConfig;
    }
    private void updateScheduleDeltas(List<String> existingSchedules, boolean forceEnabled) {
        List<String> currentSchedules = this.getScheduleNames("");
        for (String scheduleid : currentSchedules) {
            try {
                Schedule schedule = getSchedule(scheduleid);
                if (forceEnabled) schedule.setEnabled(true);
                LOGGER.info("ApplySchedule:" + scheduleid);
                saveSchedule(schedule);
            } catch (Exception e) {
                LOGGER.warn("Failed to apply schedule:" + scheduleid);
            }
        }
    }

    public void removeConfig(String xmlConfig) {
        xmlConfig = upgradeXmlConfig(xmlConfig);

        LOGGER.info("LS_Event:RemovingConfig");
        xmlConfig = xmlConfig.trim();
        Set<String> keysToRemove = new HashSet<String>();
        if (xmlConfig.startsWith(XML_2_0)) {
            // NEW xml config
            xmlConfig = xmlConfig.substring(XML_2_0.length());
            Map<String, Object> items = (Map<String, Object>) new XStream().fromXML(xmlConfig);
            keysToRemove.addAll(items.keySet());

        } else {
            // OLD xml config
            if (xmlConfig.startsWith("<entry>")) xmlConfig = "<map>" + xmlConfig + "</map>";
            Map<String, String> map = (Map<String, String>) new XStream().fromXML(xmlConfig);
            keysToRemove.addAll(map.keySet());
        }

        int count = 0;
        int fsCount = 0;
        int warn = 0;
        for (String key : keysToRemove) {
            try {
                Object item = null;
                String id = key.substring(key.indexOf("-")+1, key.length());

                // DataTypes
                if (key.contains(FieldSet.class.getName())) {
                    item = logDataService.remove(FieldSet.class, id);
                    fsCount++;
                }
                // Searches
                if (key.contains(com.liquidlabs.log.space.Search.class.getName())) {
                    item = logDataService.remove(Search.class, id);
                }
                // Dashboards
                if (key.contains(SearchSet.class.getName())) {
                    item = logDataService.remove(SearchSet.class, id);
                }
                // Workspace
                if (key.contains(Workspace.class.getName())) {
                    item = logDataService.remove(Workspace.class, id);
                }

                // Print Report
                if (key.contains(JReport.class.getName())) {
                    item = logDataService.remove(JReport.class, id);
                }
                // Alerts
                if (key.contains(Schedule.class.getName())) {
                    item = logDataService.remove(Schedule.class, id);
                }
                if (item != null) {
                    count++;
                    LOGGER.info("Uninstalled/Removed:" + key);
                }
            } catch (Throwable t) {
                LOGGER.warn("Failed to remove:" + key, t);
                warn++;
            }
        }
        LOGGER.info(String.format("LS_Event:RemoveConfig ItemCount:%d FieldSet:%d Warns:%d", count, fsCount, warn));
    }

    private void updateDataSourceDeltas(List<WatchDirectory> watchDirsBefore, List<WatchDirectory> watchDirsAfter) {
        ArrayList<WatchDirectory> watchDirsBeforeCopy = new ArrayList<WatchDirectory>(watchDirsBefore);
//		CANNOT WORK - the data is already deleted from the space
        // those items left are deleted during import
        watchDirsBeforeCopy.removeAll(watchDirsAfter);

        watchDirsAfter.removeAll(watchDirsBefore);
        List<WatchDirectory> watchDirectories = watchDirsAfter;
        for (WatchDirectory watchDirectory : watchDirectories) {
            LOGGER.info("UpdateDelta:" + watchDirectory);
            saveWatch(watchDirectory);
        }
    }

    private void updateTypeChanges(List<FieldSet> fieldSetsBefore, List<FieldSet> fieldSetsAfter) {
        List<FieldSet> fieldSetsBeforeCopy = new ArrayList<FieldSet>(fieldSetsBefore);
        List<FieldSet> fieldSetsAfterCopy = new ArrayList<FieldSet>(fieldSetsAfter);
        // remove the before items to see what has been added
        fieldSetsAfterCopy.removeAll(fieldSetsBefore);

        for (FieldSet fieldSet : fieldSetsAfterCopy) {
            if (fieldSet.upgrade()) saveFieldSet(fieldSet);
        }
    }



    public void stop() {
        LOGGER.info("LS_EVENT:Stop LogSpace");
        logScheduler.stop();
        logDataService.stop();
        logEventService.stop();
    }

    /**
     * Used to submit a Search OR Replay Request to Tailers
     */
    public void executeRequest(final LogRequest request) {
        LOGGER.info(String.format("LOGGER Executing SEARCH[%s] Request Q[%s] SearchHeads[%d]", request.subscriber(), request.queries(), searchers.size()));
        logEventService.store(request, request.getTimeToLiveMins() * 60);
        // now execute the request here so the bloom filter gets passed
        for (final Map.Entry<String, LogRequestHandler> handlers : searchers.entrySet()) {
            scheduler.submit(new Runnable() {
                @Override
                public void run() {
                    // try /  and remove when we hit errors?
                    LogRequestHandler logRequestHandler = handlers.getValue();
                    LOGGER.info("SEARCH:" +  logRequestHandler);
                    logRequestHandler.search(request);
                }
            });

        }
    }

    public List<LogReplayRequestState> loadLogRequests() {
        List<LogRequest> requests = logEventService.findObjects(LogRequest.class, "", false, -1);
        ArrayList<LogReplayRequestState> result = new ArrayList<LogReplayRequestState>();
        for (LogRequest logReplayRequest : requests) {
            User user = logReplayRequest.getUser();
            String username = user != null ? user.username() : "system";
            result.add(new LogReplayRequestState(username, logReplayRequest.subscriber()));
        }
        return result;
    }

    public void cancel(String subscriberId) {
        if (subscriberId == null || subscriberId.trim().length() == 0) return;
        aggSpace.cancel(subscriberId);
        LogRequest remove = logEventService.remove(LogRequest.class, subscriberId);
        if (remove != null) LOGGER.info("LOGGER Cleanup/Cancel:" + subscriberId + " removed:" + remove);
    }

    public String registerTailerListener(String listenerId, TailerListener tailerListener, String hostname, int timeout, String resourceType) throws Exception {
        LOGGER.info("Register TailerListener:" + listenerId);

        String result = registerCancelListener(listenerId, tailerListener, timeout);
        registerConfigListener(listenerId, tailerListener);
        registerWatchListener(listenerId, tailerListener, hostname);
        if (!resourceType.contains("Forwarder")) registerRequestHandler(listenerId, tailerListener, hostname);
        registerFieldSetListener(listenerId, tailerListener);
        if (resourceType.contains("Index") || resourceType.contains("Manage") || resourceType.contains("Fail")) registerExplore(listenerId, tailerListener);
        return result;
    }

    Map<String, Explore> explore = new java.util.concurrent.ConcurrentHashMap<>();
    private void registerExplore(String listenerId, TailerListener tailerListener) {
        explore.put(listenerId, tailerListener);
    }

    public void registerFieldSetListener(String listenerId, final FieldSetListener eventListener) throws Exception {
        LOGGER.info("Register FieldSetListener" + eventListener);
        logDataService.registerListener(FieldSet.class, "", new Notifier<FieldSet>() {
            public void notify(Type event, FieldSet result) {
                if (event == Type.TAKE) eventListener.remove(result);
                if (event == Type.WRITE) eventListener.add(result);
                if (event == Type.UPDATE) eventListener.update(result);
            }
        }, listenerId, -1, new Event.Type[]{Type.WRITE, Type.UPDATE, Type.TAKE});

    }

    public void registerConfigListener(String listenerId, final LogConfigListener eventListener) throws Exception {
        LOGGER.info("Register ConfigListener" + eventListener);
        logDataService.registerListener(LogFilters.class, "id equals " + LogFilters.key, new Notifier<LogFilters>() {
            public void notify(Type event, LogFilters result) {
                eventListener.setFilters(result);
            }
        }, listenerId, -1, new Event.Type[]{Type.WRITE, Type.UPDATE});

    }

    public void registerWatchListener(String listenerId, final LogConfigListener eventListener, final String hostname) {
        logDataService.registerListener(WatchDirectory.class, null, new Notifier<WatchDirectory>() {
            public void notify(Type event, WatchDirectory result) {
                try {
                    setExpandedHostsOnWatch(result);
                    if (event == Type.TAKE) {
                        LOGGER.info("Remove Watch:" + result);
                        eventListener.removeWatch(result);
                    }
                    if (event == Type.WRITE) {
                        LOGGER.info("Add Watch:" + result + " Listener:" + eventListener);
                        eventListener.addWatch(result);
                    }
                    if (event == Type.UPDATE) {
                        LOGGER.info("Update Watch:" + result + " Listener:" + eventListener);
                        eventListener.updateWatch(result);
                    }
                } catch (Throwable t) {
                    LOGGER.warn(t);
                }
            }
        }, listenerId, -1, new Event.Type[]{Type.WRITE, Type.TAKE, Type.UPDATE});
    }

    public String registerCancelListener(String listenerId, final CancellerListener listener, int leaseTimeOut) {
        String registerListener = logEventService.registerListener(LogRequest.class, "", new Notifier<LogRequest>() {
            public void notify(Type event, LogRequest result) {
                listener.cancel(result.subscriber());
            }
        }, listenerId, leaseTimeOut, new Event.Type[]{Type.TAKE});
        LOGGER.debug("Registered CancelListener:" + listener + " lease:" + registerListener);
        return registerListener;
    }

    protected boolean isHostMatching(Set<String> hosts, String hostname) {
        if (hostname == null || hostname.trim().length() == 0) return true;
        if (hosts.size() == 0) return true;
        for (String hostnamePart : hosts) {
            hostnamePart = hostnamePart.toUpperCase();
            if (hostname.toUpperCase().contains(hostnamePart)) return true;
        }
        return false;
    }

    Map<String, LogRequestHandler> searchers = new FastMap<String, LogRequestHandler>().shared();

    // replay/search worker
    public void registerRequestHandler(final String listenerId, final LogRequestHandler replayer, final String hostname) throws Exception {
        LOGGER.info(String.format("LOGGER Registering Searcher :%s", listenerId));
        searchers.put(listenerId, replayer);

        logEventService.registerListener(LogRequest.class, null, new Notifier<LogRequest>() {
            public void notify(final Type event, final LogRequest request) {

                if (event == Type.WRITE || event == Type.UPDATE) {
                    if (request.isVerbose()) {
                        String msg = "LOGGER Searching[%s][%s]";
                        LOGGER.info(String.format(msg, request.subscriber(), replayer));
                        if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
                    }
//                    Now handled off of the searchers hashmap
//                    replayer.search(request);
                }
                if (event == Type.TAKE) {
                    replayer.cancel(request);

                }
            }
        }, listenerId, -1, new Event.Type[]{Type.WRITE, Type.UPDATE, Type.TAKE});
    }

    public void unregisterWatchListener(String listenerId) {
        logDataService.unregisterListener(listenerId);
    }

    public void unregisterIndexListener(String listenerId) {
        logDataService.unregisterListener(listenerId);
    }

    public void unregisterTailerListener(String listenerId) {
        logEventService.unregisterListener(listenerId);
    }

    public void unregisterLogReplayer(String listenerId) {
        logEventService.unregisterListener(listenerId);
    }

    public void unregisterConfigListener(String listenerId) {
        logEventService.unregisterListener(listenerId);
    }

    public void setLiveLogFilters(LogFilters config) {
        LOGGER.info("LOGGER Updated Live Filters:" + config.toString());
        logDataService.store(config, -1);
    }

    public String addWatch(String tags, String dir, String pattern, String timeFormat, String rollClass, String hosts, int maxAgeDays, String archivingRules, boolean discoveryEnabled, String breakRule, String watchId, boolean grokItEnabled, boolean systemFieldsEnabled) {
        rollClass = rollClass.trim();
        WatchDirectory watchDirectory = new WatchDirectory(tags, dir, pattern, timeFormat, hosts, maxAgeDays, archivingRules, discoveryEnabled, breakRule, watchId, grokItEnabled, systemFieldsEnabled);
        if (!watchDirectory.isValid()) throw new RuntimeException("Invalid DataSource:" + watchDirectory);
        if (rollClass != null && rollClass.length() > 0) {
            try {
                Class<?> forName = Class.forName(rollClass);
                RolledFileSorter fileSorter = (RolledFileSorter) forName.newInstance();
                fileSorter.setFormat(timeFormat);
                watchDirectory.setFileSorter(fileSorter);
            } catch (Throwable t) {
                LOGGER.warn("Failed to construct RolledFileSorter class:" + rollClass, t);
            }
        }
        saveWatch(watchDirectory);
        return watchDirectory.id();
    }

    public void saveWatch(WatchDirectory watchDirectory) {
        if (!watchDirectory.isValid()) throw new RuntimeException("Invalid DataSource:" + watchDirectory);
        LOGGER.info("LS_EVENT:ADD_DataSource " + watchDirectory.id() + " tag:"+ watchDirectory.getTags());
        watchDirectory.validate();
        logDataService.store(watchDirectory, -1);
    }

    WatchDirectory findWatchIdForDirAndPattern(String id) {
        return logDataService.findById(WatchDirectory.class, id);
    }

    public void removeWatch(String id) {
        WatchDirectory removed = logDataService.remove(WatchDirectory.class, id);
        if (removed != null) LOGGER.info("Removed:" + removed);
    }
    public void reindexWatch(String id) {
        LOGGER.info("ReIndexing:" + id);
        WatchDirectory removed = logDataService.remove(WatchDirectory.class, id);
        if (removed == null) return;

        LOGGER.info("ReIndex Progress - Removed:" + removed);
        try {
            Thread.sleep(20 * 1000);
        } catch (InterruptedException e) {
        }
        LOGGER.info("ReIndex Progress - ReAdding:" + removed);
        logDataService.store(removed, -1);
        LOGGER.info("ReIndex Progress - Complete:" + removed);
    }



    public List<String> fieldSetList() {
        String[] findIds = logDataService.findIds(FieldSet.class, "");
        return com.liquidlabs.common.collection.Arrays.asList(findIds);
    }
    public List<FieldSet> fieldSets() {
        List<FieldSet> findObjects = logDataService.findObjects(FieldSet.class, null, true, Integer.MAX_VALUE);
        Collections.sort(findObjects, new Comparator<FieldSet>() {
            public int compare(FieldSet o1, FieldSet o2) {
                return o1.id.compareTo(o2.id);
            }
        });
        return findObjects;
    }

    public FieldSet getFieldSet(String id) {
        return logDataService.findById(FieldSet.class, id);
    }

    public List<WatchDirectory> watchDirectories(User user, String hostname, boolean expand) {
        List<WatchDirectory> results = listWatches(user, hostname, expand);

        // race state on client startup
        int count = 0;
        if (results.size() == 0 && count++ < 4) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return listWatches(user, hostname, expand);
        }
        return results;
    }

    /**
     * Includes list can be path or tag
     */
    public List<WatchDirectory> watchDirectoriesWithFilter(String fileIncludesList) {
        List<WatchDirectory> results = new ArrayList<WatchDirectory>();
        List<WatchDirectory> initialList = listWatches(null, "", true);
        String[] split = fileIncludesList.split(",");
        for (WatchDirectory watchDirectory : initialList) {
            for (String fileItem : split) {
                try {
                    if (fileItem.contains("tag:")) fileItem = fileItem.replaceAll("tag:", "").trim();
                    if (fileItem.contains("*")){
                        String fileExpr = SimpleQueryConvertor.convertSimpleToRegExp(fileItem);
                        if (watchDirectory.dirName.matches(fileExpr) && !results.contains(watchDirectory)) results.add(watchDirectory);
                    }
                    else {
                        String[] watchTags = watchDirectory.tags.split(",");
                        for (String watchTag : watchTags) {
                            if (watchTag.equals(fileItem) && !results.contains(watchDirectory)) results.add(watchDirectory);
                        }
                    }
                } catch (Throwable t) {
                    LOGGER.warn("fileItem error:" + t.toString());
                }
            }
        }
        return results;
    }

    @Override
    public WatchDirectory getWatchDir(String id) {
        return logDataService.findById(WatchDirectory.class, id);
    }
    @Override
    public Map<String, Double> getWatchVolumes(String optionalIdSubstringMatch) {

        HashMap<String, Double> map = new HashMap<>();
        map.put("logscape-logs",100.1);
        for (final Map.Entry<String, LogRequestHandler> handlers : searchers.entrySet()) {
            try {
                LogRequestHandler value = handlers.getValue();
                Map<String, Double> volumes = value.volumes();
                map.putAll(volumes);
            } catch (Throwable t) {
                LOGGER.warn("volumes call failed:",t);
            }
        }
        for (Map.Entry<String, Double> stringDoubleEntry : map.entrySet()) {
            double value = stringDoubleEntry.getValue() / FileUtil.GB;
            stringDoubleEntry.setValue(Math.floor(value * 1000) / 1000);
        }

        return map;
    }


    private List<WatchDirectory> listWatches(User user, String hostname, boolean expandHosts) {
        List<WatchDirectory> watches = null;
        if (user == null) {
            watches = logDataService.findObjects(WatchDirectory.class, null, true, Integer.MAX_VALUE);
        } else {
            Set<String> ids = getWatchIds(WatchDirectory.class, user);
            watches = new ArrayList<WatchDirectory>();
            for (String id : ids) {
                watches.add(logDataService.findById(WatchDirectory.class, id));
            }

        }
        if (expandHosts) {
            for (WatchDirectory watchDirectory : watches) {
                setExpandedHostsOnWatch(watchDirectory);
            }
        }
        Collections.sort(watches, new Comparator<WatchDirectory>() {
            public int compare(WatchDirectory o1, WatchDirectory o2) {
                return o1.getTags().toLowerCase().compareTo(o2.getTags().toLowerCase());
            }
        });
        LOGGER.info("Returning DataSources:" + watches.size());
        return watches;
    }

    private void setExpandedHostsOnWatch(WatchDirectory watchDirectory) {
        if(watchDirectory.getHosts() != null && !watchDirectory.getHosts().trim().isEmpty()) {
            Set<String> hosts = expandHostsString(watchDirectory.getHosts());
            if (hosts.isEmpty()) {
                watchDirectory.setHosts("");
            } else {
                String hostsString = hosts.toString();
                watchDirectory.setHosts(hostsString.substring(1, hostsString.length() -1).trim());
            }
        }
    }

    public LogFilters readLiveFilters() {
        List<LogFilters> findObjects = logDataService.findObjects(LogFilters.class, "id equals " + LogFilters.key, false, 1);
        if (!findObjects.isEmpty()) {
            return findObjects.get(0);
        }
        return new LogFilters();
    }

    public int size() {
        return logEventService.findObjects(LogEvent.class, null, false, Integer.MAX_VALUE).size();
    }



    public List<String> getScheduleNames(String department) {
        List<String> result = null;
        if (department.length() == 0 || department.equals("all") ) {
            result = Arrays.asList(logDataService.findIds(Schedule.class, ""));
        } else {
            result = Arrays.asList(logDataService.findIds(Schedule.class, "deptScope equalsAny all," + department));
        }
        Collections.sort(result);
        return result;
    }

    public List<Schedule> getSchedules(String department) {
        List<Schedule> schedules = null;
        if (department.length() == 0 || department.equals("all") ) {
            schedules = logDataService.findObjects(Schedule.class, "", false, -1);
        } else {
            schedules = logDataService.findObjects(Schedule.class, "deptScope equalsAny all," + department, false, -1);
        }
        for (Schedule schedule : schedules) {
            schedule.updateExecution(logDataService);
        }
        return schedules;
    }

    public void deleteSchedule(String name) throws Exception {
        LOGGER.info("DeletingSchedule:" + name);
        logDataService.remove(Schedule.class, name);
    }

    public void saveSchedule(Schedule schedule) throws Exception {
        LOGGER.info("SaveSchedule:" + schedule.name);
        logScheduler.lastError();
        logDataService.store(schedule, -1);
        Thread.sleep(100);
        Throwable throwable = logScheduler.lastError();
        if (throwable != null) throw new RuntimeException(throwable);

    }
    public void saveSchedule(Schedule schedule, boolean overwrite) throws Exception {
        LOGGER.info("SaveSchedule:" + schedule.name);
        boolean exists = logDataService.findById(Schedule.class, schedule.name) != null;
        if (overwrite || !exists) logDataService.store(schedule, -1);
    }

    public Schedule getSchedule(String name) {
        Schedule schedule = logDataService.findById(Schedule.class, name);
        if (schedule == null) {
            LOGGER.error("Failed to getSchedule:" + name);
            return null;
        }
        schedule.updateExecution(logDataService);
        return schedule;
    }

    public void cancelLease(String leaseKey) {
        LOGGER.debug("Cancel Lease:" + leaseKey);
        Throwable t1 = cancelLease(logDataService, leaseKey);
        Throwable t2 = cancelLease(logEventService, leaseKey);
        if (t1 != null && t2 != null) {
            LOGGER.warn("t1:" + t1.getMessage());
            LOGGER.warn("t2:" + t2.getMessage());
            throw new RuntimeException(t1);
        }
    }

    private Throwable cancelLease(SpaceService space, String leaseKey) {
        try {
            space.cancelLease(leaseKey);
        } catch (Throwable t) {
            return t;
        }
        return null;
    }

    /**
     * Allow the same call for both services
     */
    public void renewLease(String leaseKey, int expires) {
        Throwable t1 = renewLease(logDataService, leaseKey, expires);
        Throwable t2 = renewLease(logEventService, leaseKey, expires);
        if (t1 != null && t2 != null) {
            LOGGER.warn("t1:" + t1.getMessage());
            LOGGER.warn("t2:" + t2.getMessage());
            throw new RuntimeException(t1.getMessage());
        }
    }


    private Throwable renewLease(SpaceService space, String leaseKey, int expires) {
        try {
            space.renewLease(leaseKey, expires);
        } catch (Throwable t) {
            return t;
        }
        return null;
    }

    public LogConfiguration getConfiguration(String hostname) {
        LOGGER.info(" >> LoadConfig host:" + hostname);
        try {
            if (this.state != State.RUNNING) LOGGER.info("Waiting for Start");
            while (this.state != State.RUNNING)  Thread.sleep(100 + (long)(Math.random() * 500));
        } catch (InterruptedException e) { return null; }

        long start = System.currentTimeMillis();
        LogConfiguration logConfiguration = new LogConfiguration(readLiveFilters(), watchDirectories(null, hostname, true), getFieldSets());
        long end = System.currentTimeMillis();
        LOGGER.info(" << LoadConfig host:" + hostname + " ElapsedMS:" +  (end - start) + "ms" + " host:" + hostname);
        return logConfiguration;
    }

    private List<FieldSet> getFieldSets() {
        return logDataService.findObjects(FieldSet.class, "", false, -1);
    }

    public List<AggEngineState> addLogAggEngine(String criteria, String group) {
        return aggSpaceManager.addAggEngine(criteria, group);
    }

    public List<AggEngineState> bounceLogAggEngine(String host) {
        return aggSpaceManager.bounceAggEngine(host);
    }

    public List<AggEngineState> deleteAggEngine(String hostname) {
        return aggSpaceManager.deleteAggEngine(hostname);
    }

    public List<AggEngineState> loadLogAggEngines() {
        return aggSpaceManager.loadAggEngines();
    }

    private static int MY_PORT = VSOProperties.getPort(VSOProperties.ports.LOGSPACE);
    private JmxHtmlServerImpl jmxServer;

    public static void main(String[] args) {
        try {

            JmxHtmlServerImpl jmxServer = new JmxHtmlServerImpl(VSOProperties.getJMXPort(ports.LOGSPACE), true);
            jmxServer.start();

            LOGGER.info("Starting JMX:" + jmxServer.getURL());

            String lookupSpaceURI = VSOProperties.getLookupAddress();

            LOGGER.info("Starting - LookupURI:" + lookupSpaceURI);
            final ORMapperFactory mapperFactory = new ORMapperFactory(MY_PORT, LogSpace.NAME, 20 * 1024, VSOProperties.getREPLICATIONPort(ports.LOGSPACE));

            LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupSpaceURI, mapperFactory.getProxyFactory(), "AggSpaceBoot");
            AdminSpace adminSpace = SpaceServiceImpl.getRemoteService("LogSpaceBoot", AdminSpace.class, lookupSpace, mapperFactory.getProxyFactory(), AdminSpace.NAME, true, false);
            AggSpace aggSpace = AggSpaceImpl.getRemoteService("LogSpaceBoot", lookupSpace, mapperFactory.getProxyFactory());

            LOGGER.info("Starting LOGSPACE 3");

            VSpaceProperties.setSnapshotInterval(15 * 60);
            SpaceServiceImpl logSpaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, LogSpace.NAME, mapperFactory.getScheduler(), true, true, false);
            SpaceServiceImpl eventSpaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, LogSpace.NAME_REPLAY, mapperFactory.getScheduler(), true, false, true);
            AggSpaceManager aggSpaceManager = new AggSpaceManagerImpl(WorkAllocatorImpl.getRemoteService("LogSpaceBoot", lookupSpace, mapperFactory.getProxyFactory()));

            ResourceSpace resourceSpace = ResourceSpaceImpl.getRemoteService("LogSpaceBoot", lookupSpace, mapperFactory.getProxyFactory());

            LOGGER.info("Starting LOGSPACE 4 (Creating Instance)");

            final LogSpaceImpl logSpace = new LogSpaceImpl(logSpaceService, eventSpaceService, adminSpace, aggSpace, aggSpaceManager, new String[]{"Fatal","ERROR", "WARN", "Exception"}, new String[]{"LOGGER"}, resourceSpace, lookupSpace);

            LOGGER.info("Starting LOGSPACE 5 (Starting Instance) - next action looking for success");

            logSpace.start();

            LOGGER.info("Starting LOGSPACE 6 Logspace successfully started");
            new ResourceProfile().scheduleOsStatsLogging(mapperFactory.getScheduler(), LogSpace.class, LOGGER);

            Runtime.getRuntime().addShutdownHook(new Thread(){
                public void run() {
                    LOGGER.warn("Stopping:" + logSpace.toString());
                    logSpace.stop();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    mapperFactory.stop();
                }
            });


        } catch (Exception e) {
            LOGGER.error("Failed to start logSpace:" + e.getMessage(), e);
        }
    }

    static String getArg(String key, String[] args, String defaultResult) {
        for (String arg : args) {
            if (arg.startsWith(key)) return arg.replace(key, "");
        }
        if (defaultResult != null) return defaultResult;
        throw new RuntimeException("Argument:" + key + " was not found in:" + Arrays.toString(args));
    }

    private AlertSchedulerJmxAdmin admin;

    public static LogSpace getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {
        return getRemoteService(whoAmI, lookupSpace, proxyFactory, true);
    }

    public static LogSpace getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory, boolean wait) {
        return SpaceServiceImpl.getRemoteService(whoAmI, LogSpace.class, lookupSpace, proxyFactory, LogSpace.NAME, wait, false);
    }

    /**
     * Searches
     */
    public List<String> listSearches(User user) {
        return getIds(Search.class, user);
    }


    public Search getSearch(String name, User user) {
        Set<String> ids = new HashSet<String>(getIds(Search.class, user));
        if (ids.contains(name)) return logDataService.findById(Search.class, name);

        if (llc && user != null) name = user.getGroupId(name);
        return logDataService.findById(Search.class, name);
    }

    public String saveSearch(Search report, User user) throws Exception {
        if (llc && user != null) report.name = user.getGroupId(report.name);
        LOGGER.info("LS_EVENT:SaveSearch:" + report.name);

        logDataService.store(report, -1);
        return report.name;
    }

    public void deleteSearch(String name, User user) throws Exception {
        if (llc && user != null) name = user.getGroupId(name);
        Search removed = logDataService.remove(Search.class, name);
        if (removed == null) LOGGER.info("Failed to remove/find search:" + name);
        else LOGGER.info("Removed Search:" + name);
        logDataService.remove(Link.class, name);

    }

    /**
     * Workspaces
     */
    public List<String>  listWorkSpaces(User user) {
        return getIds(Workspace.class, user);
    }
    public Workspace getWorkspace(String name, User user) {
        name = name.trim();

        Workspace result = null;

        // The allowed set of user Entity Ids
        Set<String> workspaceIds = new HashSet<String>(getIds(Workspace.class, user));

        // existing and no-trickery
        if (workspaceIds.contains(name)) {
            result = logDataService.findById(Workspace.class, name);
        } else {
            // not found -= check for a user. group alias
            // scope it to this users version of the Workspace
            if (llc && user != null) {
                if (name.startsWith("user.")) {
                    // replace the user. with this users group
                    name = user.getGroupId(name).replace("user.","");
                }
                name = user.getGroupId(name);
            }
            result =  logDataService.findById(Workspace.class, name);
        }


        // fallback to the standard home
        if (result == null && name.contains("Home")) {
            result = logDataService.findById(Workspace.class, "Home");
        }

        if (result != null) {
            String json = result.content;
            if (json.startsWith(CDATA)) {
                return new Workspace(result.name, result.content.substring(CDATA.length(), result.content.length() - CD_END.length()));
            };
            return new Workspace(result.name, result.content);
        }
        return null;
    }

    public String saveWorkspace(String name, String workspace, User user) {
        LOGGER.info("Save Workspace:" + name);
        name = name.trim();
        if (llc && user != null) name = user.getGroupId(name);
        Workspace workspace1 = new Workspace(name, workspace);
        logDataService.store(workspace1, -1);
        return workspace1.name;

    }
    public String deleteWorkspace(String name, User user) {
        LOGGER.info("Delete Workspace:" + name);
        if (user != null) name = user.getGroupId(name);
        logDataService.remove(Workspace.class, name);
        logDataService.remove(SearchSet.class, name);
        return "";
    }

    Set<String> getWatchIds(Class clazz, User user) {

        // 1. Group scoped
        Set<String> results = new HashSet<>(Arrays.asList(logDataService.findIds(clazz, getWatchGroupFilter(user))));

        // 2. Global = Non-scoped searches dont have a dot
        List<WatchDirectory> watches = logDataService.findObjects(WatchDirectory.class, "", false, Integer.MAX_VALUE);
        for (WatchDirectory watch : watches) {
            if (!watch.getTags().contains(".")) results.add(watch.id());
        }
        return results;
    }
    /**
     * Generic Accessors
     */
    private String getWatchGroupFilter(User user) {
        return llc && user != null ? user.getGroupFilter("tags") : "";
    }


    List<String> getIds(Class clazz, User user) {

        // 1. Group scoped
        List<String> results = new ArrayList<String>(Arrays.asList(logDataService.findIds(clazz, getGroupFilter(user))));
        Collections.sort(results);

        // 2. Global = Non-scoped searches dont have a dot
        List<String> results2 = new ArrayList<String>(Arrays.asList(logDataService.findIds(clazz, "")));
        Collections.sort(results2);
        for (String id : results2) {
            if (!id.contains(".") && !results.contains(id)) results.add(id);
        }
        return results;
    }

    /**
     * Generic Accessors
     */
    private String getGroupFilter(User user) {
        return llc && user != null ? user.getGroupFilter("name") : "";
    }


    /**
     * Dashboards (OLD)
     */
    public List<String> listDashboardNames(Set<String> userIds) {
        StringBuilder userIdsList = new StringBuilder();
        for (String userId : userIds) {
            userIdsList.append(userId).append(",");
        }
        String query = "";
        if (userIdsList.length() > 0) {
            query =  "owner equalsAny " + userIdsList.toString();
        }

        String[] searchNames = logDataService.findIds(SearchSet.class, query);
        LOGGER.info("Get SearchSets/Dashboards count:" + searchNames.length + " Users:" + userIdsList);
        List<String> results = com.liquidlabs.common.collection.Arrays.asList(searchNames);
        Collections.sort(results);
        return results;
    }

    public SearchSet getDashboard(String name) {
        return logDataService.findById(SearchSet.class, name);
    }

    public void storeDashboard(SearchSet dashboard) {
        LOGGER.info("LS_EVENT:SaveDashboard Name:" + dashboard.getName());
        logDataService.store(dashboard, -1);
    }

    public void deleteDashboard(String name) {
        logDataService.remove(SearchSet.class, name);
    }
    public void addSearchToDashboard(String searchName, String dashboardName) {
        SearchSet dashboard = getDashboard(dashboardName);
        dashboard.addSearch(searchName);
        storeDashboard(dashboard);
    }




    public List<SearchSet> listAllDashboards() {
        List<SearchSet> findObjects = logDataService.findObjects(SearchSet.class, "", false, -1);
        Collections.sort(findObjects, new Comparator<SearchSet>() {
            public int compare(SearchSet o1, SearchSet o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return findObjects;
    }

    public List<Link> getLinks() {
        return logDataService.findObjects(Link.class, "", false, -1);
    }

    public void deleteLinksForSearch(String reportName) {
        logDataService.remove(Link.class, "searchName equals " + reportName, -1);
    }

    public Link getLinkForSearch(String searchName) {
        List<Link> findObjects = logDataService.findObjects(Link.class, "searchName equals '" + searchName + "'", false, 1);
        if (findObjects.size() > 0) return findObjects.get(0);
        return null;
    }

    public void createLink(String reportName, String expr, int linkGroup,
                           int paramGroup) {
        logDataService.store(new Link(reportName, expr, reportName, linkGroup, paramGroup), -1);
    }

    public void writeLogStat(LogStats stats) {
        logDataService.store(stats, -1);
    }

    public List<LogStats> getLogStats() {
        return logDataService.findObjects(LogStats.class, "", false, -1);
    }

    synchronized public Set<String> expandHostsString(String hostsString) {
        HashSet<String> results = new HashSet<String>();
        if (hostsString == null || hostsString.trim().length() == 0 || hostsString.equals("*")) return results;
        String[] splitHosts = hostsString.split(",");
        for (String part : splitHosts) {
            part = part.trim();
            if (part.startsWith("group:")) {
                final String groupName = part.replace("group:", "");
                Set<String> hosts = resourceSpace.expandGroupIntoHostnames(groupName);
                if (hosts != null) results.addAll(hosts);
            } else {
                results.add(part);
            }
        }
        return results;
    }

    public List<Search> searches() {
        return logDataService.findObjects(Search.class, null, true, Integer.MAX_VALUE);
    }

    public void resourceGroupUpdated(Type event, ResourceGroup result) {
        LOGGER.info("LS_EVENT: ResourceGroupUpdated id:" + result.getName());
        List<WatchDirectory> watches = logDataService.findObjects(WatchDirectory.class, "hosts contains group:" + result.getName(), true, Integer.MAX_VALUE);
        for (WatchDirectory watchDirectory : watches) {
            LOGGER.info("LS_Event:ResourceGroupUpdate - forcing re-eval:" + watchDirectory.dirName);
            logDataService.store(watchDirectory, -1);
        }
        LOGGER.info("LS_EVENT:ResourceGroupUpdated  count:" + watches.size());

    }

    public Set<String> exploreHosts(User user) {
        Set<String> hosts = new HashSet<String>();
        for (Explore explore1 : this.explore.values()) {
            hosts.addAll(explore1.hosts(user));
        }
        return hosts;
    }
    public Map<String, List<String>> exploreDirs(User user, String host) {
        Map<String, List<String>> results = new HashMap<>();
        for (Explore explore1 : this.explore.values()) {
            results.put(explore1.url(), explore1.dirs(user, host));
        }
        return results;
    }
}
