package com.liquidlabs.log;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.admin.AdminSpaceImpl;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.explore.ExploreImpl;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSetAssember;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.reader.LogReaderFactory;
import com.liquidlabs.log.search.*;
import com.liquidlabs.log.search.tailer.TailerEmbeddedAggSpace;
import com.liquidlabs.log.space.*;
import com.liquidlabs.log.streaming.StreamingRequestHandlerImpl;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.lookup.LookupSpace;
import javolution.util.FastMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AgentLogServiceImpl implements AgentLogService, LifeCycle, SearchLockManager {

    static final Logger LOGGER = Logger.getLogger(AgentLogServiceImpl.class);
    public static final String TAG = "LOGGER";
    final ScheduledExecutorService youngScheduler;
    private final LogSpace logSpace;
    private final LoggingEventMonitor eventMonitor = new LoggingEventMonitor();
    private LogFilters filters;

    FastMap<String,Tailer> tailers = new FastMap<String,Tailer>().shared();

    final Indexer indexer;
    private final String hostname;

    private java.util.concurrent.ExecutorService searchExecutorPool;

    private final String resourceId;
    private final ProxyFactory proxyFactory;
    TailerEmbeddedAggSpace tailerAggSpace;


    State state = State.STOPPED;

    int tailerLimit = LogProperties.getTailersMax ();

    AtomicInteger tailsToSubmit = new AtomicInteger(0);
    AtomicInteger tailsStarted = new AtomicInteger(0);
    private ScheduledExecutorService leaseScheduler;

    private final LogReaderFactory logReaderFactory;
    private Thread.UncaughtExceptionHandler exceptionHandler;
    LogStatsRecorderUpdater statsRecorder;
    private WatchManager watchManager;
    private DataSourceArchiver dataSourceArchiver;
    private StreamingRequestHandlerImpl liveRequestHandler;
    private WatchVisitor watchVisitor;

    private TailerListenerImpl tailerListenerImpl;
    private final AggSpace aggSpace;
    private ScheduledThreadPoolExecutor oldScheduler;
    private AgentLogServiceJMX jmx;


    @Override
    public java.util.Map<String, Integer> getPoolSizes() {
        HashMap<String, Integer> results = new HashMap<String, Integer>();
        results.put("old-q-tailer-import-size", ((ScheduledThreadPoolExecutor) oldScheduler).getQueue().size());
        results.put("young-q-tailer-scheduler-size", ((ScheduledThreadPoolExecutor)youngScheduler).getQueue().size());
        results.put("tailerCount", tailers.size());
        return results;
    }

    public AgentLogServiceImpl(LogSpace logSpace, AggSpace aggSpace, ScheduledExecutorService scheduler, ProxyFactory proxyFactory, Indexer indexer, String resourceId, LogReaderFactory readerFactory, Thread.UncaughtExceptionHandler exceptionHandler, LookupSpace lookupSpace) {
        this.logSpace = logSpace;
        this.aggSpace = aggSpace;
        this.youngScheduler = scheduler;
        ((ScheduledThreadPoolExecutor) this.youngScheduler).setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        ((ScheduledThreadPoolExecutor) this.youngScheduler).setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        this.oldScheduler = ExecutorService.newScheduledThreadPool(LogProperties.oldSchedulerThreads(), new NamingThreadFactory("tailer-old", true, Thread.NORM_PRIORITY - 2));

        ((ScheduledThreadPoolExecutor) this.oldScheduler).setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        ((ScheduledThreadPoolExecutor) this.oldScheduler).setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        this.proxyFactory = proxyFactory;
        this.indexer = indexer;
        this.resourceId = resourceId;
        this.logReaderFactory = readerFactory;
        this.exceptionHandler = exceptionHandler;
        this.hostname = NetworkUtils.getHostname();

        LOGGER.info("Starting with LogSpace:" + logSpace);
        LOGGER.info("Starting with AggSpace:" + aggSpace);
        if (LogProperties.isForwarder) {
            tailerLimit = Integer.getInteger("tailer.fwdr.limit", 10 * 1000);
        }
        LOGGER.info("TailerLimit:" + tailerLimit);

        leaseScheduler = ExecutorService.newScheduledThreadPool(2, "tailer-lease", Thread.MAX_PRIORITY, exceptionHandler);
        int poolSize = LogProperties.getSearchThreads();
        eventMonitor.raise(new Event("Config").with("searchPoolSize", poolSize));
        searchExecutorPool = ExecutorService.newFixedPriorityThreadPool(poolSize, LogProperties.getSearchPoolQueueSize(), "tailer-search");

        leaseScheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                watchManager.expireFileData();
            }
        }, 2, LogProperties.getExpiryCleanInterval(), TimeUnit.HOURS);



        SearchRunnerImpl searchRunner = new SearchRunnerImpl(indexer, tailerAggSpace, eventMonitor, (ThreadPoolExecutor) searchExecutorPool);

        tailerAggSpace = new TailerEmbeddedAggSpace(aggSpace, searchRunner, proxyFactory.getScheduler(), hostname, eventMonitor);
        searchRunner.setAggSpace(tailerAggSpace);

        AdminSpace adminSpace = AdminSpaceImpl.getRemoteService("Agent", lookupSpace, proxyFactory);
        long llc = adminSpace.getLLC(true);

        this.watchManager = new WatchManager(new WatchManager.Callback() {
            public void deleteLogFile(List<LogFile> logFiles, boolean forceRemove) {
                AgentLogServiceImpl.this.deleteLogFile(logFiles, forceRemove);
            }
            public void statsRecorderScheduleUpdate(int delay) {
                //AgentLogServiceImpl.this.statsRecorder.scheduleUpdate(delay);
            }
            public void statsRecorderScheduleUpdateForNextMinutes(int mins) {
                //AgentLogServiceImpl.this.statsRecorder.scheduleUpdateForNextMinutes(mins);
            }
            public List<Tailer> tailers() {
                return new ArrayList<Tailer>(AgentLogServiceImpl.this.tailers.values());
            }
        }, indexer, llc == -1 ? 2: 10 * 1000);

        this.watchVisitor = new WatchVisitor(
                new WatchVisitor.Callback() {
                    public void startTailing(WatchDirectory watch, String file) {
                        AgentLogServiceImpl.this.startTailing(watch, file);
                    }
                    public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                        return AgentLogServiceImpl.this.isRollCandidateForTailer(fullFilePath, tags);
                    }
                    public boolean isTailingFile(String fullFilePath, File file) {
                        return AgentLogServiceImpl.this.isTailingFile(fullFilePath, file);
                    }

                }, watchManager.watchDirSet());
        this.watchManager.setVisitor(this.watchVisitor);

        this.dataSourceArchiver = new DataSourceArchiver(watchManager.watchDirSet(), watchVisitor.getFwdWatchDirs(), indexer);


        statsRecorder = new LogStatsRecorderUpdater(leaseScheduler, logSpace, resourceId, hostname, indexer, this, this.watchManager.watchDirSet());

        leaseScheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                AgentLogServiceImpl.this.indexer.cleanupMissingIndexedFiles();
            }

        }, 24, 24, TimeUnit.HOURS);

        liveRequestHandler = new StreamingRequestHandlerImpl(tailers, tailerAggSpace, LogProperties.getLogHttpServerURI(), indexer, scheduler);
        tailerAggSpace.setLiveRequestHandler(liveRequestHandler);
        jmx = new AgentLogServiceJMX(this, this.watchVisitor, lookupSpace, proxyFactory, this.dataSourceArchiver);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for (Tailer tailer : tailers.values()) {
                    if (tailer.getLine() == 1) {
                        eventMonitor.raise(new Event("Importing").with("Tag", tailer.getWatch().getTags()).with("File", tailer.filename()).with("Progress", tailer.getWriter().currentLine()));
                    }
                }
            }
        }, 10, Integer.getInteger("log.import.report.interval.min",10) , TimeUnit.MINUTES);
    }


    /**
     * Made this dispatched so that it can be killed if we run into crappy issues
     * @param logFiles
     * @param isExpired
     */
    void deleteLogFile(final List<LogFile> logFiles, final boolean isExpired) {
        try {
            LOGGER.info("Deleting:" + logFiles.size());
            for (LogFile logFile : logFiles) {
                LOGGER.info("Delete:" + logFile);
                Tailer tailer = tailers.remove(logFile.getFileName());
                if (tailer != null) {
                    tailer.interrupt();
                }
            }

            if (logFiles.size() < 5) {
                for (LogFile logFile : logFiles) {
                    LOGGER.info("DELETED:" + logFile);
                }
            }
            indexer.removeFromIndex(logFiles);

        } catch (Throwable t) {
            LOGGER.warn("Cleanup failed",t);
        }
    }

    public void start() {
        eventMonitor.raise(new Event("AGENT_START").with("resourceId", resourceId));
        LOGGER.info(String.format("%s Starting FilteredLogService", TAG));

        state = State.STARTED;
        LogRequestHandler searcherReplayerHandler = new LogRequestHandlerImpl(tailerAggSpace, resourceId, indexer);
        final CancellerListener cancelListener = new RequestCanceller(tailerAggSpace, resourceId);
        LogConfigListener configListener = new LogConfigurationHandler(this, resourceId, watchManager, eventMonitor);
        FieldSetListener  fieldSetListener = new AgentFieldSetListener(resourceId, indexer, aggSpace, oldScheduler);


        loadInitialConfig();

        tailerListenerImpl = new TailerListenerImpl(logSpace, leaseScheduler, cancelListener, configListener, searcherReplayerHandler, hostname, fieldSetListener, new ExploreImpl(indexer));
        try {
            tailerListenerImpl.start();
        } catch (Exception e) {
            LOGGER.warn("TailerListener Failed to start:" + e.toString(), e);
        }

        scheduleIndexCleaner();
        LOGGER.info(String.format("%s Started ", TAG));
    }


    private void scheduleIndexCleaner() {

        oldScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("removeFilesViaAgentLogServiceImpl");
                watchManager.removeFilesNoLongerBeingWatched();
            }

        }, (24 - new DateTime().getHourOfDay())+Integer.getInteger("stop.watch.hour.of.day",3), 24, TimeUnit.HOURS);
    }


    public void stop() {
        if (state == State.STOPPED) return;
        eventMonitor.raise(new Event("AGENT_STOP").with("resourceId", resourceId));
        state = State.STOPPED;
        System.err.println("SHUTTING DOWN LOGTAILER: " + proxyFactory.getEndPoint());
        LOGGER.info("SHUTTING DOWN LOGTAILER: " + proxyFactory.getEndPoint());
        watchVisitor.stop();
        tailerAggSpace.stop();

        for (Tailer tailer : this.tailers.values()) {
            tailer.interrupt();
        }


        shutdownThreadPool((ThreadPoolExecutor) youngScheduler);
        shutdownThreadPool((ThreadPoolExecutor) oldScheduler);

        if (tailerListenerImpl != null) try {tailerListenerImpl.stop();}catch(Exception e){}
        leaseScheduler.shutdownNow();
        searchExecutorPool.shutdownNow();
        try {
            youngScheduler.awaitTermination(30, TimeUnit.SECONDS);
            oldScheduler.awaitTermination(30, TimeUnit.SECONDS);
        }catch(Exception e){}
        indexer.close();
    }


    private void shutdownThreadPool(ThreadPoolExecutor executor) {
        executor.shutdown();
        executor.getQueue().drainTo(new ArrayList<Runnable>());
    }
    @Override
    public int getTailerLimit() {
        return tailerLimit;
    }
    @Override
    public TailerEmbeddedAggSpace getTailerAggSpace() {
        return this.tailerAggSpace;
    }



    private void loadInitialConfig() {
        LOGGER.info(" LoadConfig >>");
        LogConfiguration configuration = null;
        int count = 0;

        while (configuration == null && count++ < 10) {
            try {
                configuration = logSpace.getConfiguration(hostname);
                Thread.sleep(1000);
            } catch (Throwable t) {
                LOGGER.warn("Failed to load LogSpace Config, Retrying:" + t,t);
                try {
                    Thread.sleep(3 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
        if (configuration == null) {
            LOGGER.fatal("Failed to load Configuration from LogSpace");
            System.err.println("Failed to load Configuration from LogSpace");
            throw new RuntimeException("Failed to load Configuration from LogSpace");
        }

        LOGGER.info(" LoadConfig <<");

        LOGGER.info("1 DataSources retrieved:" + configuration.watching().size());
        LOGGER.info("1 DataTypes retrieved:" + configuration.fieldSets().size());

        // Add the initial set of sources...
        watchManager.addWatches(configuration.watching());
        youngScheduler.schedule(loadConfigRunnable(), LogProperties.getSyncDelaySeconds(), TimeUnit.SECONDS);

        this.filters = configuration.filters();
        scheduleDatasourceSync();
    }
    private void scheduleDatasourceSync() {
        /**
         * Need to ensure that the agent is in sync with the LogSpace DataSources - pull them every hour
         */
        Runnable reloadTask = new Runnable() {
            public void run() {
                LogConfiguration configuration = logSpace.getConfiguration(hostname);
                if (watchManager.watchDirSet().size() != configuration.watching().size()) {
                    eventMonitor.raise(new Event("DataSourceCountChanged").with("Given", configuration.watching().size()).with("Currently", watchManager.watchDirSet().size()));
                    watchVisitor.suspend(true);

                    List<WatchDirectory> watching = configuration.watching();
                    for (WatchDirectory watchDirectory : watching) {
                        watchManager.updateWatch(watchDirectory, false);
                    }
                    watchVisitor.suspend(false);

                }
            }};
        oldScheduler.scheduleWithFixedDelay(reloadTask, 1, 1, TimeUnit.HOURS);


    }

    private Runnable loadConfigRunnable() {

        return new Runnable() {
            public void run () {
                if (!loadConfigFromLogSpace())
                    youngScheduler.schedule(loadConfigRunnable(), (long) (LogProperties.getSyncDelaySeconds() + (Math.random() + 15)), TimeUnit.SECONDS);
            }
        }

                ;
    };

    volatile boolean loadedConfig = false;

    private boolean loadConfigFromLogSpace() {
        try {
            LOGGER.info(" SchedLoadConfig >>");
            LogConfiguration finalConfiguration = logSpace.getConfiguration(hostname);
            LOGGER.info(" SchedLoadConfig <<");
            LOGGER.info("2 DataSources retrieved:" + finalConfiguration.watching().size());
            LOGGER.info("2 DataTypes retrieved:" + finalConfiguration.fieldSets().size());
            if (isValidConfig(finalConfiguration)) {
                return false;
            }

            // unlock the loadedConfig here - need to let visitor start adding files so we dont deadlock
            watchVisitor.suspend(true);

            List<FieldSet> givenFieldSets = finalConfiguration.fieldSets();
            List<FieldSet> existingFieldSets = indexer.getFieldSets(new Indexer.AlwaysFilter());

            LOGGER.info("Processing DataSources");
            if (!finalConfiguration.isPopulated()) {
                LOGGER.error("Received DATASOURCE SIZE==0", new RuntimeException("Received empty Watch count"));
            }

            loadedConfig = true;


            List<WatchDirectory> watching = finalConfiguration.watching();
            watchManager.addWatches(watching);
            indexer.sync();

            LOGGER.info("Processing DataTypes");

            for (FieldSet fieldSet : givenFieldSets) {
                LOGGER.info("Processing DataType:" + fieldSet);
                indexer.addFieldSet(fieldSet);
            }

            // now remove any fieldSets that were deleted while we were offline
            for (FieldSet existingFieldSet : existingFieldSets) {
                boolean wasDeleted = true;
                for (FieldSet givenFieldSet  : givenFieldSets) {
                    if (existingFieldSet.id.equals(givenFieldSet.id)) wasDeleted = false;
                }
                if (wasDeleted) {
                    LOGGER.info("Found Deleted FieldSet:" + existingFieldSet);
                    indexer.removeFieldSet(existingFieldSet);
                } else {
                    LOGGER.info("Same FieldSet:" + existingFieldSet);
                }
            }


            LOGGER.info("Processing Done");

            return true;
        } catch (Throwable t) {
            t.printStackTrace();
            LOGGER.fatal("Failed to LOAD Config error:" + t.toString(),t);
            return false;
        } finally {
            loadedConfig = true;
            watchVisitor.suspend(false);
        }
    }

    private boolean isValidConfig(LogConfiguration finalConfiguration) {
        return finalConfiguration == null || finalConfiguration.watching().size() == 0 && finalConfiguration.fieldSets().size() == 0;
    }

    synchronized void startTailing(WatchDirectory watch, String fullFilePath) {
        try {

            while (indexer.isStalling()) Thread.sleep(1000);

            while (!loadedConfig) {
                LOGGER.info("Waiting for Config Load");
                Thread.sleep(10000);
            }
            while (tailerCount() > tailerLimit ) {
                Thread.sleep(10 * 1000);
                LOGGER.warn("Pending tailers:" + tailerCount() + " Limit:" + tailerLimit);
            }


            tailsToSubmit.incrementAndGet();

            if (this.state != State.STARTED) return;
            if (!new File(fullFilePath).exists()) return;

            if (isTailingFile(fullFilePath, new File(fullFilePath))) return;

            if (!watchManager.shouldWatch(fullFilePath)) return;

            boolean indexed = indexer.isIndexed(fullFilePath);

            long pos = 0;
            int line = 1;
            if (indexed) {
                long[] posAndLine = indexer.getLastPosAndLastLine(fullFilePath);
                pos = posAndLine[0];
                line = (int) posAndLine[1];
            }
            File file = new File(fullFilePath);

            // Ignore  files that are already imported with the expected filePos & are older than the Oldest interest time
            if (file.length() == pos && file.lastModified() < new DateTime().minusHours(LogProperties.getMaxTailDequeueHours()).getMillis()) {
                watchVisitor.stoppedIndexing(fullFilePath);
                return;
            }
            // this will happen until the file has padded the maxWatchTime period
            if (file.length() == pos) {
                //LOGGER.info("File not changed, ignoring:" + fullFilePath);
                return;
            }

            FieldSet fieldSet = FieldSets.getBasicFieldSet();
            LogFile logFile = null;
            if (indexed) {
                // existing file
                logFile = indexer.openLogFile(fullFilePath);

                LOGGER.info("Start TAILING EXISTING file:" + fullFilePath + " Watch:" + watch.getTags() + " IndexPos:"+ logFile.getPos() + " File:" + new File(fullFilePath).length());
                if (!LogProperties.isForwarder) fieldSet = indexer.getFieldSet(indexer.openLogFile(fullFilePath).fieldSetId.toString());

            } else {
                // new file = calculate the DataType
                LOGGER.info("Start TAILING NEW file:" + fullFilePath + " Watch:" + watch.getTags());
                if (!LogProperties.isForwarder) fieldSet = new FieldSetAssember().determineFieldSet(fullFilePath, indexer.getFieldSets(new Indexer.AlwaysFilter()), FileUtil.readLines(fullFilePath, LogProperties.detectTypeLines(), watch.getBreakRule()), false, watch.getTags());
                logFile = indexer.openLogFile(fullFilePath, true, fieldSet.getId(), watch.getTags());
            }

            LogReader reader = logReaderFactory.getReader(logFile, indexer, watch, filters, new AtomicLong(), fieldSet.getId());

            TailerImpl tailer = new TailerImpl(file, pos, line, reader, watch, indexer);
            tailer.setFilters(this.filters.includes, this.filters.excludes);
            tailer.setLogService(this);
            // older than 24 hours - do it slower
            if (file.lastModified() < new DateTime().minusDays(1).getMillis()) {
                // do this old files inline so we dont kill the machine - import the new stuff quicker
                //tailer.call();
                tailer.future(oldScheduler.schedule(tailer, getDaysOld(file.lastModified()) * 5, TimeUnit.SECONDS));
            } else {
                tailer.future(youngScheduler.schedule(tailer, getMinutesOld(file.lastModified()) * 2, TimeUnit.SECONDS));
            }
            tailers.put(tailer.filename(), tailer);
            tailsStarted.incrementAndGet();

            // attach live event streaming
            this.liveRequestHandler.attachToTailer(tailer);

            LOGGER.info(String.format("LOGGER - %s Started tailing file[%s] indexedAlready[%b] tailers[%d] tailersToSubmit[%d]",		watch.getTags(), fullFilePath, indexed, tailers.size(), tailsToSubmit.get()));
        } catch (Throwable e) {
            // can happen when permission denied etc
            LOGGER.warn("Failed to Tail file: " + fullFilePath, e);
            watchVisitor.stoppedIndexing(fullFilePath);
        } finally {
            tailsToSubmit.decrementAndGet();
        }
    }

    private long getDaysOld(long time) {
        // only use delay when the scheduler is busy...
        if (oldScheduler.getQueue().size() < 10) return 2;
        long age = Days.daysBetween(new DateTime(time), new DateTime()).getDays();
        if(age >= 60) return 60;
        return age;
    }

    private int getMinutesOld(long time){
        int age = Minutes.minutesBetween(new DateTime(time), new DateTime()).getMinutes();
        if(age >= 300) return 300;
        return age;
    }


    public void enqueue(Tailer tailer, int noDataSeconds, int scheduleSeconds) {
        if (noDataSeconds / (24 * 60 * 60) <= LogProperties.getMaxTailDequeueHours())  {
            if (youngScheduler.isShutdown()) return;
//			if (noDataSeconds > 60) LOGGER.info("Requeue:" + tailer + " NoData:" + noDataSeconds +  " @RunInSecs:" + scheduleSeconds);
            tailer.future(youngScheduler.schedule(tailer, scheduleSeconds, TimeUnit.SECONDS));
        } else {
            stopTailing(tailer);
        }
    }


    public void stopTailing(Tailer tailer) {

        tailers.remove(tailer.filename());
        // only print stop tailing msg for newish files
        //if (new File(tailer.filename()).lastModified() > new DateTime().minusHours(48).getMillis()) {
        LOGGER.info(String.format("Stopped tailer:%d tailing:%s tailers:%d", tailer.hashCode(), tailer.filename(), tailers.size()));
        //}
        tailer.interrupt();
        tailer.stop();

    }
    // this tailer has rolled and depending on the nature it may actually have changed the name it tracks (i.e. numeric roll)
    public void roll(String filename) {
        Tailer removed = tailers.remove(filename);
        if (removed != null) {
            tailers.put(removed.filename(), removed);
        } else {
            LOGGER.warn("Rolling failed to update Tailer:" + filename);
        }


    }

    public void setFilters(LogFilters filters) {
        this.filters = filters;
        int count = 0;
        for (Tailer tailer : tailers.values()) {
            tailer.setFilters(filters.includes(), filters.excludes());
            count++;
        }
        LOGGER.info(String.format("%s NEW FILTERS [%s] updated[%d]", TAG, filters.toString(), count++));
        if (LogProperties.isTestDebugMode()) {
            com.liquidlabs.log.LogProperties.testLog(getClass(), String.format("%s NEW FILTERS [%s] updated[%d]", TAG, filters.toString(), count));
        }
    }
    boolean isTailingFile(String filename, File file) {
        if (tailers.containsKey(filename)) return true;
//        boolean isIndexed = indexer.isIndexed(filename);
        LogFile logFile = indexer.openLogFile(filename);
        if (logFile == null) {
            return false;
        } else  {
            try {

                // doesn't exist - or if it does - you cannot append to it - so ignore the file
                if (!logFile.isAppendable()) return true;

                // Check we have indexed everything and nothing has changed
                if (!FileUtil.isCompressedFile(filename)) {
                    if (new File(filename).length() == logFile.getPos() ) {
                        return true;
                    }
                    File fileO = new File(filename);
                    long lastMod = fileO.lastModified();
                    long lastWatchWindow = new DateTime().minusHours(LogProperties.getMaxTailDequeueHours()).getMillis();
                    boolean isTheFileReallyOld = lastMod < lastWatchWindow;
                    return isTheFileReallyOld;

                } else {
                    // a compressed file - cannot rely on the length -
                    // a ) check the lastMod is newer than what we have seen...
                    File fileO = new File(filename);
                    long lastCompMod = fileO.lastModified();
                    long logFileEndTime = logFile.getEndTime();
                    long twoDaysAgo = new DateTime().minusDays(1).getMillis();
                    long fileSizeDiff = fileO.length() - logFile.getPos();
                    // the file hasnt changed
                    if (Math.abs(fileSizeDiff) < 4 * 1024) return true;
                    if (lastCompMod <= logFileEndTime) {
                        return true;
                    }
                    if (lastCompMod < logFile.getEndTime() + DateUtil.HOUR && lastCompMod < twoDaysAgo && fileSizeDiff == 0  ) {
                        return true;
                    }
                    // - Retail if a) Newish File b) filesystem is vastly different to what we know it to be
                    if (fileO.lastModified() > twoDaysAgo) return true;
                        // else its old and it HAS changed so let is be imported...
                    else return false;

                }
            } catch (Throwable t) {
                LOGGER.error("Failed to read:" + filename,t);
                // try and make it get rewritten
                return false;
            }
        }
    }
    public boolean isRollCandidateForTailer(String file, String tag) throws InterruptedException {
        for (Tailer tailer : tailers.values()) {
            if (tailer.isRollCandidate(file, tag)) return true;
        }
        return false;
    }


    public void unlock(ReadWriteLock lock) {
        if (lock != null) {
            lock.readLock().unlock();
        }
    }


    public ReadWriteLock acquireReadLock(String indexed) {
        Tailer tailer = findTailer(indexed);
        ReadWriteLock lock;
        if (tailer != null) {
            lock = tailer.getLock();
        } else {
            lock = new ReentrantReadWriteLock();
        }
        lock.readLock().lock();
        return lock;
    }

    private Tailer findTailer(String file) {
        if (tailers.containsKey(file)) return tailers.get(file);
        for (Tailer tailer : tailers.values()) {
            if (tailer.isFor(file)) {
                return tailer;
            }
        }
        return null;
    }


    public void remove(Tailer tailer) {
        tailers.remove(tailer);
    }



    public Indexer getIndexer() {
        return this.indexer;
    }
    public int tailerCount() {
        return tailers.size();
    }

    public List<Tailer> getTailers() {
        return new ArrayList<Tailer>(this.tailers.values());
    }
}
