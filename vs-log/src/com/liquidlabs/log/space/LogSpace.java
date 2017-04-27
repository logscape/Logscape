package com.liquidlabs.log.space;

import com.liquidlabs.admin.User;
import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.log.CancellerListener;
import com.liquidlabs.log.TailerListener;
import com.liquidlabs.log.alert.Schedule;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.jreport.JReport;
import com.liquidlabs.log.links.Link;
import com.liquidlabs.log.space.agg.AggEngineState;
import com.liquidlabs.transport.proxy.clientHandlers.Broadcast;
import com.liquidlabs.transport.proxy.clientHandlers.Cacheable;
import com.liquidlabs.transport.proxy.clientHandlers.FailFastOnce;
import com.liquidlabs.transport.proxy.ReplayOnAddressChange;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.resource.ResourceGroup;
import org.joda.time.DateTime;
import java.util.Map;

import java.util.List;
import java.util.Set;

public interface LogSpace extends LifeCycle{
    String NAME = LogSpace.class.getSimpleName();
    String NAME_REPLAY = LogSpace.class.getSimpleName() + "_REPLAY";

    @ReplayOnAddressChange
    String registerTailerListener(String listenerId, TailerListener tailerListener, String hostname, int timeout, String resourceType) throws Exception;

    @ReplayOnAddressChange
    void registerConfigListener(String listenerId, LogConfigListener eventListener) throws Exception;

    @ReplayOnAddressChange
    void registerRequestHandler(String listenerId, LogRequestHandler replayer, String hostname) throws Exception;

    @ReplayOnAddressChange
    void registerWatchListener(String listenerId, LogConfigListener listener, String hostname);

    @ReplayOnAddressChange
    String registerCancelListener(String listenerId, CancellerListener listener, int timeout);

    @FailFastOnce
    void unregisterTailerListener(String listenerId);

    void unregisterLogReplayer(String listenerId);
    void unregisterWatchListener(String listenerId);
    void unregisterIndexListener(String indexListenerId);
    void unregisterConfigListener(String listenerId);

    void setLiveLogFilters(LogFilters config) throws Exception;

    LogFilters readLiveFilters();

    Search getSearch(String name, User user);

    @Cacheable(ttl=5)
    List<String> listSearches(User user);

    String saveSearch(Search report, User user) throws Exception;

    void deleteSearch(String name, User user) throws Exception;

    List<String> getScheduleNames(String department);

    List<Schedule> getSchedules(String department);

    void deleteSchedule(String name) throws Exception;

    void saveSchedule(Schedule schedule) throws Exception;

    Schedule getSchedule(String name);

    @Cacheable(ttl=1)
    LogConfiguration getConfiguration(String hostname);

    void saveWatch(WatchDirectory watch);

    String addWatch(String tags, String dir, String pattern, String timeFormat, String rollClass, String hosts, int maxAgeDays, String archivingRules, boolean discoveryEnabled, String breakRule, String watchId, boolean grokItEnabled, boolean systemFieldsEnabled);

    void removeWatch(String id);
    void reindexWatch(String id);

    //	@Cacheable(ttl=1)
    List<WatchDirectory> watchDirectories(User user, String hostname, boolean expandHosts);

    @Cacheable(ttl=10)
    List<WatchDirectory> watchDirectoriesWithFilter(String fileIncludes);

    WatchDirectory getWatchDir(String id);

    Map<String, Double> getWatchVolumes(String optionalSubstringMatch);


    void renewLease(String leaseKey, int expires) throws Exception;

    void cancelLease(String leaseKey);

    void cancel(String subscriberId);

    @Broadcast
    void executeRequest(LogRequest request) throws Exception;

    List<AggEngineState> addLogAggEngine(String criteria, String group);

    List<AggEngineState> bounceLogAggEngine(String host);

    List<AggEngineState> loadLogAggEngines();

    List<AggEngineState> deleteAggEngine(String hostname);

    List<LogReplayRequestState> loadLogRequests();


    String saveWorkspace(String name, String workspace, User user);
    Workspace getWorkspace(String name, User user);
    String deleteWorkspace(String name, User user);
    List<String>  listWorkSpaces(User user);

    void storeDashboard(SearchSet searchSet);
    void deleteDashboard(String name);
    SearchSet getDashboard(String name);

    List<String> listDashboardNames(Set<String> uids);
    List<SearchSet> listAllDashboards();

    @Cacheable
    List<Link> getLinks();
    Link getLinkForSearch(String searchName);
    void createLink(String searchName, String expr, int linkGroup, int paramGroup);
    void deleteLinksForSearch(String searchName);

    void writeLogStat(LogStats stats);

    @Cacheable
    List<LogStats> getLogStats();

    String exportConfig(String filter);
    void importConfig(String xmlConfig, boolean merge, boolean overwrite) throws Exception;

    @Cacheable
    List<FieldSet> fieldSets();
    @Cacheable(ttl=10)
    FieldSet getFieldSet(String id);
    void saveFieldSet(FieldSet log4jFieldSet);

    void removeFieldSet(String fieldSetId);

    void savePrintReport(JReport report);
    JReport getPrintReport(String name);
    String[] listPrintReports(String department);
    void removePrintReport(String name);
    String emailPrintReport(String emailTo, String reportName);

    @FailFastOnce
    String executePrintReport(String report, String fileIncludes, String fileExcludes, String execUser, String execDept, DateTime fromTime, DateTime toTime);

    @FailFastOnce(ttl=180)
    String printReportToPDF(String searchName, long fromTimeMs, long toTimeMs, String userId, String fileIncludes, String fileExcludes, int mode);

    void removeConfig(String xmlConfig);

    void addSearchToDashboard(String searchName, String dashboardName);

    List<String> fieldSetList();

    void resourceGroupUpdated(Type event, ResourceGroup result);

    String executeSchedule(String name2);

    void setLogAlertEnableFlag(String name2, String enableOrDisable);

    Set<String> exploreHosts(User user);

    Map<String, List<String>> exploreDirs(User user, String host);

}
