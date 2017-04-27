/**
 *
 */
package com.liquidlabs.log;

import com.liquidlabs.admin.User;
import com.liquidlabs.log.alert.Schedule;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.jreport.JReport;
import com.liquidlabs.log.links.Link;
import com.liquidlabs.log.search.HistoEvent;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.*;
import com.liquidlabs.log.space.agg.AggEngineState;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.resource.ResourceGroup;
import org.joda.time.DateTime;

import java.util.*;

public class NullLogSpace implements LogSpace {

    private LogConfiguration logConfiguration = new LogConfiguration();

    public LogFilters readLiveFilters() {
        return new LogFilters(new String [] {".*ERROR.*"}, null);
    }
    public List<String> fieldSetList() {
        return null;
    }

    public void registerConfigListener(String listenerId, LogConfigListener eventListener)
            throws Exception {
    }

    public void registerEventListener(LogEventListener eventListener,
                                      String listenerId, String filter, int leasePeriod) throws Exception {
    }

    public void registerRequestHandler(String listenerId, LogRequestHandler replayer, String hostname) throws Exception {
    }
    public String printReportToPDF(String searchName, long fromTimeMs,
                                   long toTimeMs, String userId, String fileIncludes, String fileExcludes, int mode) {
        return null;
    }

    public void replay(LogRequest replayRequest,
                       LogReplayHandler replayHandler) throws Exception {
    }

    public void search(LogRequest replayRequest,
                       LogReplayHandler replayHandler) throws Exception {
    }

    public void unregisterConfigListener(String listenerId) {
    }

    public void unregisterEventListener(String listenerId) {
    }

    public String registerTailerListener(String listenerId, TailerListener tailerListener, String hostname, int timeout, String resourceType) throws Exception {
        return null;
    }

    public void unregisterTailerListener(String listenerId) {
    }

    public int write(LogEvent logEvent) {
        return 0;
    }

    public void setLiveLogFilters(LogFilters config) {
    }

    public int write(ReplayEvent replayEvent) {
        return 0;
    }

    public void write(HistoEvent event) {
    }

    public void start() {
    }

    public void stop() {
    }

    public void deleteSearch(String name, User user) {
    }

    public Search getSearch(String name, User user) {
        return null;
    }

    public List<String> listSearches(User user) {
        return null;
    }

    public String saveSearch(Search report, User user) {
        return "";
    }

    public List<String> getScheduleNames(String department) {
        return null;
    }
    public List<Schedule> getSchedules(String department) {
        return null;
    }

    public void saveSchedule(Schedule schedule) {
    }

    public void deleteSchedule(String name) {
    }

    public Schedule getSchedule(String name) {
        return null;
    }

    public LogConfiguration getConfiguration(String hostname) {
        return logConfiguration;
    }

    @Override
    public void saveWatch(WatchDirectory watch) {

    }

    public List<WatchDirectory> watchDirectories(User user, String hostname, boolean expandHosts) {
        return null;
    }
    public List<WatchDirectory> watchDirectoriesWithFilter(String fileIncludes) {
        return null;
    }

    @Override
    public WatchDirectory getWatchDir(String id) {
        return null;
    }

    @Override
    public Map<String, Double> getWatchVolumes(String optionalSubstringMatch) {
        return null;
    }

    public void removeWatch(String id) {
    }

    @Override
    public void reindexWatch(String id) {
    }

    public String addWatch(String tags, String dir, String pattern, String timeFormat, String rollClass, String hosts, int maxAgeDays, String archivingRules, boolean discoveryEnabled, String breakRule, String watchId, boolean grokItEnabled, boolean systemFieldsEnabled) {
        return "";
    }

    public void registerWatchListener(String listenerId, LogConfigListener listener, String hostname) {
    }

    public void setLogConfig(LogConfiguration logConfiguration) {
        this.logConfiguration = logConfiguration;

    }

    public void cancelLease(String leaseKey) {
    }



    public String registerCancelListener(String listenerId, CancellerListener listener, int i) {
        return listener.getId();
    }

    public void renewLease(String leaseKey, int expires) {
    }

    public void executeRequest(LogRequest request) {
        System.out.println("Null Shit Dude!");
    }

    public List<AggEngineState> addLogAggEngine(String criteria, String group) {
        return null;
    }

    public List<AggEngineState> bounceLogAggEngine(String host) {
        return null;
    }

    public List<LogReplayRequestState> cancelRelayRequest(String subscriberId) {
        return null;
    }

    public List<AggEngineState> deleteAggEngine(String hostname) {
        return null;
    }

    public List<AggEngineState> loadLogAggEngines() {
        return null;
    }

    public List<LogReplayRequestState> loadLogRequests() {
        return null;
    }

    public void unregisterIndexListener(String indexListenerId) {
    }

    public void unregisterLogReplayer(String listenerId) {
    }

    public void unregisterWatchListener(String listenerId) {
    }

    public void cancel(String subscriberId) {
    }

    public void storeDashboard(SearchSet searchSet) {
    }

    public void deleteDashboard(String name) {
    }

    public SearchSet getDashboard(String name) {
        return null;
    }

    public List<String> listDashboardNames(Set<String> uids) {
        return null;
    }
    public void addSearchToDashboard(String searchName, String dashboardName) {
    }

    public void deleteLinksForSearch(String reportName) {
    }

    public Link getLinkForSearch(String reportName) {
        return null;
    }

    public void createLink(String reportName, String expr, int linkGroup,
                           int paramGroup) {
    }

    public List<Link> getLinks() {
        return null;
    }

    public List<LogStats> getLogStats() {
        return null;
    }

    public String exportConfig(String filter) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void importConfig(String xmlConfig, boolean merge, boolean overwrite) throws Exception {
    }
    public void removeConfig(String xmlConfig) {
    }

    public void writeLogStat(LogStats stats) {
    }

    public List<SearchSet> listAllDashboards() {
        return null;
    }

    public List<FieldSet> fieldSets() {
        return Arrays.asList(FieldSets.getBasicFieldSet());
    }
    public FieldSet getFieldSet(String id) {
        return FieldSets.getBasicFieldSet();
    }

    public void saveFieldSet(FieldSet log4jFieldSet) {
    }

    public void removeFieldSet(String fieldSetId) {
    }

    public JReport getPrintReport(String name) {
        return null;
    }
    public ArrayList<String> getFieldList(String searchName) {
        return null;
    }

    public String[] listPrintReports(String department) {
        return null;
    }

    public void savePrintReport(JReport report) {
    }

    public void removePrintReport(String name) {
    }
    public String emailPrintReport(String emailTo, String reportName) {
        return "";
    }
    public String executePrintReport(String report, String fileIncludes, String fileExcludes, String execUser, String execDept, DateTime fromTime, DateTime toTime) {
        return null;
    }

    public String exportFieldSets(String filter) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String exportSchedules(String filter) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void importSchedules(String exported) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String exportSearches(String filter) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String exportSearchSets(String filter) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void importSearchSets(String searchSets) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void importFieldSets(String exportedFieldSets) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void importSearches(String exportedSearches) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String exportWatchDirectories(String filter) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void importWatchDirectories(String exportedWatches) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
    public String executeSchedule(String name) {
        return null;
    }
    @Override
    public void resourceGroupUpdated(Type event, ResourceGroup result) {
        // TODO Auto-generated method stub

    }
    public void setLogAlertEnableFlag(String name2, String enableOrDisable) {
    }

    @Override
    public Set<String> exploreHosts(User user) {
        return null;
    }

    @Override
    public Map<String, List<String>> exploreDirs(User user, String host) {
        return null;
    }

    public String saveWorkspace(String name, String workspace, User user) {
        return "";
    }
    public String deleteWorkspace(String name, User user){ return "";}
    public Workspace getWorkspace(String name, User user) {
        return null;

    }
    public List<String>  listWorkSpaces(User user) {
        return new ArrayList<String>();
    }
}