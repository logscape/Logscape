package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.util.OSUtils;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.MBeanGetter;
import com.logscape.disco.DiscoProperties;
import org.apache.commons.lang3.ArrayUtils;
import org.joda.time.DateTime;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;

import static com.liquidlabs.common.collection.PriorityQueue.PRIORITY_QUEUE_MAX_AGE_DAYS;


public class LogProperties {

    static int coreCount = MBeanGetter.getCoreCount();
    private static int defaultBucketMultiplier;
    private static int summaryTopLimit;
    public static boolean isForwarder = VSOProperties.getResourceType().contains("Forwarder");
    public static boolean isIndexStore = VSOProperties.getResourceType().contains("Index");


    // derive all other bucket times from this... we use minutes from 2010 so we can store IntValues
    public static long TIME_BASE = 0;
    static {
        TIME_BASE = DateUtil.convertToMin(new DateTime(2010, 1, 1, 0, 0, 0).getMillis());
    }

    private static String DBRootForSummaryIndex;

    public static int fromMsToMin(long timeMs) {
        int result = (int)(DateUtil.convertToMin(timeMs) - DiscoProperties.TIME_BASE);
        if (result < 0) result = 0;
        return result;
    }
    public static long fromMinToMs(int timeMin) {
        return ((long) (timeMin + DiscoProperties.TIME_BASE) * DateUtil.MINUTE);
    }
    public static final Integer LIVE_ROLL_HOURS = Integer.getInteger("live.roll.days", 4) * 24;

    // Stored as SEc - 1 Year == 31Million, Int Max == 21B = 67Years
    public static int fromMsToSec(long timeMs) {
        int result = (int)(DateUtil.convertToSec(timeMs) - DiscoProperties.TIME_BASE);
        if (result < 0) result = 0;
        return result;
    }
    public static long fromSecToMs(int timeSec) {
        return ((long) (timeSec + DiscoProperties.TIME_BASE) * DateUtil.SECOND);
    }

    static boolean isDashboardEventsCommit = System.getProperty("dash.db.events.commit","false").equals("true");
    public static boolean isDoDbCommit() {
        return isDashboardEventsCommit;

    }

    public static int getMaxReadWaitSecs() {
        return Integer.getInteger("agent.list.max.secs", 10);
    }

    public static String getDBRoot() {
        return "work/DB";
    }
    public static String getEventsDB() {
        return getDBRoot() + "/" + "events";
    }
    public static String getDashboardEventsDB() {
        return getDBRoot() + "/" + "dash-events";
    }
    public static String getKVIndexDB() {
        return getDBRoot() + "/" + "kv-index";
    }
    public static int getWebSslPort() {
        return Integer.getInteger("web.ssl.port", 8080);
    }

    public static String[] US_FORMATS = new String[] {
            "MM/dd/yy HH:mm:ss.SSS",
            "MM-dd-yyyy HH:mm:ss,SSS zzz",
            "MM-dd-yyyy HH:mm:ss,SSS",
            "MM-dd-yyyy HH:mm:ss zzz",
            "MM/dd/yyyy HH:mm:ss.SSS",
            "MM/dd/yyyy HH:mm:ss zzz",
            "MM/dd/yyyy HH:mm:ss zzz",
            "MM-dd-yyyy HH:mm:ss",
            "MM/dd/yyyy HH:mm:ss",
            "MM/dd/yyyy HH:mm",
            "MM/dd/yy HH:mm:ss",
            "MMM-dd-yyyy HH:mm:ss",
            "MM/dd/yy HH:mm:ss",
            "MM/dd/yy-HH:mm",
    };
    public static String[] UK_FORMATS = new String[] {
            "dd/MM/yy HH:mm:ss.SSS",
            "dd-MM-yyyy HH:mm:ss,SSS zzz",
            "dd-MM-yyyy HH:mm:ss,SSS",
            "dd-MMM-yyyy HH:mm:ss.SSS",

            "dd-MM-yyyy HH:mm:ss zzz",
            "dd/MM/yyyy HH:mm:ss.SSS",
            "dd/MM/yyyy HH:mm:ss zzz",
            "dd/MM/yyyy HH:mm:ss zzz",
            "dd-MM-yyyy HH:mm:ss",
            "dd-MMM-yyyy HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss",

            "dd-MMM-yyyy HH:mm",
            "dd/MM/yy HH:mm:ss",
            "dd/MM/yy-HH:mm",
            "dd/MM/yyyy HH:mm",
            "dd-MM-yy HH:mm" ,
    };
    public static String[] LOCALE_FORMATS = System.getProperty("user.timezone").contains("Europe") ? UK_FORMATS : US_FORMATS;
    public static String[] NON_LOCALE_FORMATS = System.getProperty("user.timezone").contains("Europe") ? US_FORMATS : UK_FORMATS;

    // see http://joda-time.sourceforge.net/apidocs/org/joda/time/format/ISODateTimeFormat.html
    public static String[] ALL_FORMATS = new String[] {
            "dd-MMM-yyyy HH:mm:ss zzz",
            "dd-MMM-yyyy HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss,SSS",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy/MM/dd HH:mm:ss.SSS",
            "yyyy/MM/dd HH:mm:ss,SSS",
            "yy-MM-dd HH:mm:ss,SSS",
            "yy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss",
            "dd-MMM-yy HH:mm",


            "yyyy-MM-dd HH:mm:ss zzz",
            "yyyy-MMM-dd HH:mm:ss zzz",
            "yyyy/MM/dd HH:mm:ss zzz",

            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MMM-dd HH:mm:ss",
            "yyyy/MM/dd HH:mm:ss",


            //17-06-2013 11:41:34,921 BST

            "MMM dd yyyy HH:mm:ss",

            //17-Oct-2012 00:20
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd\tHH:mm:ss",
            "yyyy-MM-dd HH:mm:ss",
            "dd-MMM-yyyy HH:mm",
            "EEE MMM yyyy dd HH:mm:ss",
            "yyyyMMdd HH:mm:ss",

            // unix 'date' output (script)
            "EEE dd MMM yyyy HH:mm:ss",

            //Wed 14/09/2009 11:33:53
            "EEE dd/MM/yyyy HH:mm:ss",

            "EEE MMM dd yyyy HH:mm:ss",

            // java date
            //Wed Aug 11 14:30:43 BST 2010
            "EEE MMM dd HH:mm:ss zzz yyyy",

            // Weblogic Server	####<Nov 13, 2008 12:00:38 PM IST
            "char:< MMM dd, yyyy hh:mm:ss aa zzz",
            "char:< dd.MM.yyyy hh.mm",

            // firewall
            // 2;5Oct2005;00:24:24
            "char:;  dMMMyyyy;HH:mm:ss",
            "char:|2 dd MMM yyyy hh:mm:ss,SSS",

            //98  Fri Jun 12 13:00:04 BST 2009
            "1t EEE MMM dd HH:mm:ss zzz yyyy",
            "1s EEE MMM dd HH:mm:ss zzz yyyy",

            //Wed Apr 13 14:37:11 BST 201
            "EEE MMM dd HH:mm:ss zzz yyyy",

            // platform
            // Tue Sep 20 15:15:26 2009
            "EEE MMM dd HH:mm:ss yyyy",
            // Jul 02 14:11:41 2009
            "MMM dd HH:mm:ss yyyy",

            // NT Event Log - regionally specific setup
            //
            //"21s MM/dd/yyyy HH:mm:ss",
            // sys log format - "Aug 16 17:10:36 "
            "MMM  d HH:mm:ss",
            "MMM dd HH:mm:ss",

            "HH:mm:ss,SSS",

            // WMI Event Logs
            "yyyyMMddHHmmss",

            // Apache weblogs 'access_log'
            // 52.2.169.107 - - [10/Jul/2013:12:48:54 -0400] "GET /p
            // 65.33.94.190 - - [05/Apr/2003:17:26:27 -0500] \"GET /scripts/root.exe?/c+dir HTTP/1.0\" 404 276
            "char:[ dd/MMM/yyyy:HH:mm:ss zzzzz",

            // Apache - [Wed Jan 29 16:01:38 2014] [error] [client 10.38.98.249] PHP Parse error: syntax error, unexpected '[' in /data/drupal/sites/all/themes/aegonmf/templates/field--field_product_table.tpl.php on line 55
            "char:[ EEE MMM dd HH:mm:ss yyyy",

            // more weblogs
            "char:[ EEE, dd MMM yyyy HH:mm:ss zzz",

            // websphere
            "char:[ MM/dd/yy HH:mm:ss:SSS zzz",
            "char:[ MM/dd/yy HH:mm:ss",

            // HP Stuff
            "char:< yyyy-MM-dd HH:mm:ss,SSS",
            "char:< yyyy-MM-dd HH:mm:ss",


            "4s MMM dd HH:mm:ss",

            "3s UNIX_LONG",

            // GC Logs
            "14s MMM dd hh:mm:ss yyyy",

            // xpolog
            // [2008-11-13 14:04:16,049]
            "char:[ yyyy-MM-dd HH:mm:ss,SSS",
            "char:[ yyyy-MM-dd HH:mm",

            "HH:mm:ss",
            "14csv UNIX_LONG",

            //11:39:56 GMT Daylight Time
            "hh:mm:ss zzzz",

            // Some json examples
            "{ \"key\": \"Timestamp\\\":\", \"format\": \"UNIX_LONG\" }",
            "{ \"key\": \"Timestamp\\\":\\\"\", \"format\": \"dd-MMM-yyyy HH:mm:ss\" }",
            "{ \"locale\": \"NL\", \"format\": \"dd-MMM-yyyy HH:mm:ss\" }",

            // MS ETL XML & CSV TimeStamp formats
            "{ \"key\": \"SystemTime=\\\"\", \"format\": \"yyyy-MM-dd'T'HH:mm:ss\" }",
            "delim:#24 '\"'yyyy-MM-dd'T'HH:mm:ss'.'"

    };

    public static String[] FORMATS = (String[])  ArrayUtils.addAll(ArrayUtils.addAll(LOCALE_FORMATS, ALL_FORMATS), NON_LOCALE_FORMATS);

    private static int logHttpPort;

    public static String getPrintQ(int port) {
        return System.getProperty("print.q", "work/jetty-0.0.0.0-" + port +"-play.war-_play-any-/webapp/reports/");
    }

    public static String getFunctionSplit() {
        return "!";
    }
    public static String getWebAppDir(int port) {
        return "work/jetty-0.0.0.0-" + port + "-root.war-_-any-/webapp";
    }

    public static String getLogServerDIR() {
        return "LogServer" + svrName;
    }
    public static String getLogServerRoot() {
        return System.getProperty("log.server.root","../../work/" + getLogServerDIR());
    }
    private static String hostname = NetworkUtils.getHostname().toUpperCase();
    public static String getHostFromPath(String filePath) {
        int serverFrom = filePath.indexOf(getLogServerDIR());
        if (serverFrom != -1) {
            serverFrom += getLogServerDIR().length() + 1;
            int endIndex = filePath.indexOf(File.separator, serverFrom + 1);
            if (endIndex == -1) return hostname;
            return filePath.substring(serverFrom, endIndex);
        } else {
            return hostname;
        }
    }

    private static boolean flipping = Boolean.parseBoolean(System.getProperty("arg.flip", "true"));
    public static boolean isFlipAgs() {
        return flipping;
    }
    static int liveThresholdHours =Integer.getInteger("log.live.event.age.threshold.hrs",4);
    public static int getLiveEventAgeThresholdHours() {
        return liveThresholdHours;
    }
    public static void setLiveEventAgeThresholdHours(int value) {
        System.setProperty("log.live.event.age.threshold.hrs", Integer.toString(value));
    }

    // larger values provides better performance on import - it prevents line fragmentation
    // in the dw line store
    static int flushCount =Integer.getInteger("log.tailer.flush.count", 10 * 1000);
    public static int getTailerFlushCount() {
        return flushCount;
    }

    public static int getRafBufferSize() {
        return Integer.getInteger("log.raf.buffer.size", 8 * 1024);
    }

    public static int getDefaultLeasePeriod() {
        return Integer.getInteger("log.lease.period", 5 * 60);
    }
    public static int getDBMemCachePercent() {
        return Integer.getInteger("log.mem.cache.pc", 80);
    }

    public static int getDefaultLeaseRenewPeriod() {
        return Integer.getInteger("log.lease.renew.period", 2 * 60);
    }

    public static int getDefaultReplayLimit() {
        return Integer.getInteger("log.default.replay.limit", 10 * 1024);
    }
    static int flushInterval = Integer.getInteger("search.flush.int.ms", 1000);
    public static int searchFlushIntervalMs() {
        return flushInterval;
    }

    public static long aggSpaceHistoFlushMillis() {
        return Long.getLong("search.flush.ms", 1000);
    }

    public static long aggSpaceEventFlushMilliseconds() {
        return Long.getLong("replay.flush.ms", 1000);
    }

    static public Integer getAggLeaseSubscriberTimeout(){
        return Integer.getInteger("log.aggLease.sub.time", 5 * 60);
    }

    private static int histoUpateIntThr = Integer.getInteger("log.agg.update.interval.threshold.secs", 3) * 1000;
    public static int getHistoUpdateIntervalThreshold() {
        return histoUpateIntThr;
    }
    public static int getHistoEventsFlush() {
        return Integer.getInteger("log.agg.flush.events", 1000);
    }

    public static int getReplayChunkSize() {
        return Integer.getInteger("log.replay.chunk", 500);
    }

    static int replayThr = Integer.getInteger("log.replay.agg.drop.threshold", 100 * 1024);
    final public static int getAggReplayDropThreshold() {
        return replayThr;
    }

    public static int getReplayScrollLimit() {
        return Integer.getInteger("log.replay.scroll.limit", 5);
    }

    public static boolean isDeleteExistingDB() {
        return Boolean.getBoolean("log.delete.db");
    }

    public static void setLogStoreMode(boolean b) {
        System.setProperty("log.logStore", Boolean.toString(b));
    }

    public static short getRequestTTLMins() {
        return Integer.getInteger("log.request.ttl", 3).shortValue();
    }
    public static short getRequestTTLMinsLIVE() {
        return Integer.getInteger("log.request.ttl.live", Short.MAX_VALUE).shortValue();
    }

    public static Integer getMaxEventsRequested() {
        return Integer.getInteger("log.max.search.results", 100 * 1000);
    }
    public static Integer getMaxEventsStoreOnServer() {
        return Integer.getInteger("log.max.search.results", 100 * 1000);
    }
    private static int maxEventsPerHost = Integer.getInteger("log.max.search.per.host", 50 * 1000);
    public static Integer getMaxEventsPerHostSource() {
        return maxEventsPerHost;
    }



    /**
     * Careful with this value  - if too high you will run out of heap and it will spend the whole time GCing -
     * (6 threads is safe for 3GB/10,000 files, 54 folders and 756Mheap with room to spare - using 1 about 1/second/search/gb)
     * @return
     */


    // too many of these will trigger OOM on the import process

    public static int getTailerThreads() {
        int tailers = Math.min(coreCount+1, 6);
        return Integer.getInteger("log.tailer.threads", tailers);
    }

    public static int oldSchedulerThreads() {
        int tailers = Math.min(coreCount+1, 6);
        return Integer.getInteger("log.tailer.old.threads", tailers);
    }

    public static long getScanYieldInterval() {
        return Integer.getInteger("log.yield.interval", 10);
    }

    public static long getAddWatchFileDelay() {
        return Integer.getInteger("log.watch.add.delay", 13);
    }
    public static void setAddWatchFileDelay(int delay) {
        System.setProperty("log.watch.add.delay", Integer.toString(delay));
    }
    public static int getMaxTailDequeueHours() {
        return Integer.getInteger("log.watch.stop.period.mins", 26);
    }
    public static String getSystem32() {
        return System.getProperty("win.sys32", "C:\\WINDOWS\\SYSTEM32");
    }
    public static int getMaxSeriesTotal() {
        return Integer.getInteger("log.max.series.total", 256);
    }

    public static int getMaxCountAgg() {
        return Integer.getInteger("log.max.count.agg", 10 * 1024);
    }
    public static int getCountLimit() {
        return Integer.getInteger("log.count.limit",  10 * 1024);
    }

    public static int getDefautTopLimit() {
        return Integer.getInteger("log.top.limit", 50);
    }
    public static int getFieldSetMatchLines() {
        return Integer.getInteger("fs.match.lines", 100);
    }
    public static int getInitialScanSize() {
        return Integer.getInteger("log.initial.scan.lines", 100);
    }

    public static int getFileScanMinCheck() {
        return Integer.getInteger("log.scan.interval.min.secs", 1);
    }
    public static int getFileScanMaxCheck() {
        return Integer.getInteger("log.scan.interval.max.secs", 30);
    }
    public static int getFileScanInterval0() {
        return Integer.getInteger("log.scan.interval.fall.mins", 60);
    }
    public static int getFileScanInterval1() {
        return Integer.getInteger("log.scan.interval.fall2.mins", 25 * 60 * 60);
    }
    public static int getMaxFileList() {
        return Integer.getInteger("log.max.file.list", 5 * 1000);
    }
    public static int getAggSchedulerSize() {
        return Integer.getInteger("agg.scheduler.size", 30);
    }
    public static int getMaxTriggerReportLines() {
        return Integer.getInteger("report.trigger.max.lines", 100);
    }
    public static int getSearchThreads() {
        // if explicitly specified - then use it
        int size = Integer.getInteger("log.searchThreads",Integer.getInteger("log.search.threads",0));
        return numberOfThreads(size);
    }
    public static int getMTSearchThreads() {
        int size = getSearchThreads();
        int explicit = Integer.getInteger("mt.log.searchThreads",-1);
        if (explicit != -1) return explicit;
        if (size > 8) size -= 2;
        if (size > 16) size -= 2;
        return size;
    }

    private static int numberOfThreads(int size) {
        if (size  > 0) return size;
        size = coreCount;
        return size <= 3 ? 3 : size;
    }


    public static int getMaxDayHostList() {
        return Integer.getInteger("explorer.max.days.host", 120);
    }
    public static int getTimeExtractLimit() {
        return Integer.getInteger("log.time.extract.scan.limit", 128);
    }
    public static int getSlowSearchPoolSize() {
        return Integer.getInteger("log.seach.slow.pool.size", 10);
    }
    public static int slowSearchThresholdSeconds() {
        return Integer.getInteger("log.seach.slow.threshold.sec", 30);
    }
    public static void setSlowSearchThresholdSeconds(int secs) {
        System.setProperty("log.seach.slow.threshold.sec", Integer.toString(secs));
    }
    public static int detectTypeLines() {
        return Integer.getInteger("log.detect.type.lines", 15);
    }
    public static int getTimeDetectWarnMod() {
        return Integer.getInteger("log.detect.time.mod", 5000);
    }

    public static int getReportBrowseMaxPageSortSize() {
        return Integer.getInteger("report.browse.page.sort.size", 5 * 1000);
    }
    public static int getReportBrowsePageSize() {
        return Integer.getInteger("report.browse.page.size", 100);
    }
    public static int highPriorityTailerQueueSize() {
        return Integer.getInteger("log.tailer.high.queue.size", 20000);
    }
    public static int oldQueueSize =  Integer.getInteger("log.tailer.low.queue.size", ((int) OSUtils.getHeapGB()) * 20000);
    public static int lowPriorityTailerQueueSize() {
        return oldQueueSize ;
    }
    public static int getExplorerMaxHeader() {
        return Integer.getInteger("explorer.max.header", 1500);
    }
    public static int getExplorerMaxFooter() {
        return Integer.getInteger("explorer.max.footer", 1024 * 2);
    }
    public static String getEmailFrom() {
        String domain = "logscape@host.com";
        try {
            domain = "logscape@" + InetAddress.getLocalHost().getHostName() + ".com";
        } catch (Throwable t) {
        }
        return System.getProperty("email.from", domain);
    }
    public static int getDefaultBuckets() {
        return Integer.getInteger("log.bucket.defaults", 120);
    }
    public static int getMaxAutoBMs() {
        return Integer.getInteger("bm.max.auto", 100);
    }

    public static int getSearchCompleteDelaySecs() {
        return Integer.getInteger("search.complete.delay.secs", 2);
    }
    public static int getBackgroundTTLThreshold() {
        return Integer.getInteger("search.bg.threshold.mins", 10);
    }
    public static long getBucketEventDetailThresholdSecs() {
        return Integer.getInteger("search.time.detail.thold.secs", 60 * 5);
    }
    public static int getReplayAlertPauseSecs() {
        return Integer.getInteger("replay.alert.pause.secs", 5);
    }
    public static void setTestDebugMode() {
        System.setProperty("test.debug.mode", "true");
    }
    private static boolean isTestMode =Boolean.getBoolean("test.debug.mode");
    public static boolean isTestDebugMode() {
        return isTestMode;
    }
    public static void testLog(Class<? extends AgentLogServiceImpl> class1, String format) {
        System.out.println(String.format("%s %s %s", new DateTime(), class1.getSimpleName(), format));
    }
    public static int getMaxBucketThreshold() {
        return Integer.getInteger("max.bucket.th", 2000);
    }
    public static void logTest(Class class1, String msg) {
        System.out.println(String.format("%s %s %s %s", new DateTime().toString(), Thread.currentThread().getName(), class1.getSimpleName(), msg));
    }

    public static int getMaxElapsed() {
        return Integer.getInteger("elapsed.limit",5000);
    }
    public static int checkFieldSetAssignmentIntervalMinutes() {
        return Integer.getInteger("tailer.type.check.interval",5);
    }
    public static long getLogStatsUpdateIntervalMins() {
        return Integer.getInteger("log.stats.update.ival.mins", 5);
    }

    public static long getScanMaxFileYieldCount() {
        return Integer.getInteger("log.yield.max.files", 5000);
    }
    public static long getExpiryCleanInterval() {
        return Integer.getInteger("log.expire.clean.hours", 4);
    }
    public static int getMaxTriggerHeldItems() {
        return Integer.getInteger("log.trigger.max.items", 100);
    }
    public static int queueWaitMs =Integer.getInteger("log.tail.queue.wait.ms", 1000);
    public static int getStartTailingQueueWaitMs() {
        return queueWaitMs;
    }
    public static long getSyncDelaySeconds() {
        return Integer.getInteger("log.sync.data.delay.secs", 15);
    }
    public static int getSlowPoolQueueSize() {
        return Integer.getInteger("log.big.queue.size", 10000);
    }
    public static int getSearchPoolQueueSize() {
        return Integer.getInteger("log.search.queue.size", 100000);
    }
    public static int getReplayThrottleOff() {
        return Integer.getInteger("log.replay.throttle.off", 10 * 1000);
    }

    /**
     * Threshold for OLD v NEW data
     */
    static int maxDays =  Integer.getInteger("log.max.slow.days",  Integer.getInteger("max.hot.days",5));
    public static int getSlowDurationDays() {
        return maxDays;
    }
    public static void setHotDays(int days) {
        maxDays = days;
    }

    static {
        System.setProperty(PRIORITY_QUEUE_MAX_AGE_DAYS, String.valueOf(getSlowDurationDays()));
    }

    public static boolean isAnOldFile(long fileLastModified) {
        return fileLastModified < new DateTime().minusDays(LogProperties.getSlowDurationDays()).getMillis();
    }




    public static int getMinAlertTrigger() {
        return Integer.getInteger("alert.min.trigger",100);
    }
    public static long getLivePauseBeforeComplete() {
        return Integer.getInteger("live.pause.complete.delay.ms",2500);
    }
    public static long getLivePushInterval() {
        return Integer.getInteger("live.push.interval.sec",15);
    }
    static String svrName = System.getProperty("log.fwd.server.dir","_SERVER_");
    public static String getServiceDIRName() {
        return svrName;
    }

    public static String getLogHttpServerURI() {
        return "http://" + NetworkUtils.getIPAddress()  + ":" + LogProperties.getLogHttpPort() + "/";
    }
    public static int getLogHttpPort() {
        return VSOProperties.getPort(VSOProperties.ports.HTTPLOGSERVER);
    }

    public static int getMaxFirstLineSize() {
        return Integer.getInteger("log.firstline.truncate.size", 2 * 1024);
    }

    public static boolean isRollingCompressed() {
        return Boolean.getBoolean("log.roll.compressed.enabled");
    }

    static int maxFields = Integer.getInteger("search.max.fields",500);
    public static int getMaxFields() {
        return maxFields;
    }

    private static int maxModifiedMinutes = Integer.getInteger("max.modified.mins",15);
    public static int getMaxModifiedMinutes() {
        return maxModifiedMinutes;
    }

    public static boolean isVisitorDoingFileCount() {
        return System.getProperty("visitor.file.count.enabled","true").equals("true");
    }

    public static int getTailersMax() {
        return (int) (OSUtils.getHeapGB() * LogProperties.getTailersPerGB());
    }

    public static int getTailersPerGB() {
        return Integer.getInteger("tailers.max.conncurrent.per.gb", 4000);
    }

    public static long getIndexStatsLogInterval() {
        return Integer.getInteger("index.stats.interval.min", 10);
    }

    public static int visitorFileCountMaxDays() {
        return Integer.getInteger("visitor.file.count.max.days", 4);
    }

    public static int getDefaultBucketMultiplier() {
        return Integer.getInteger("search.buckets.multiplier", 3);
    }

    static int topLimit = Integer.getInteger("search.summary.top", 6);
    public static int getSummaryTopLimit() {
        return topLimit;
    }

    public static String getDBRootForFacets() {
        return getDBRoot() + "/" + "facets";
    }

    public static String getDBRootForSummaryIndex() {
        return getDBRoot() + "/" + "sidx";
    }
}
