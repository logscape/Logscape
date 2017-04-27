package com.liquidlabs.log.indexer.persistit;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSetAssember;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.*;
import com.liquidlabs.log.indexer.AbstractIndexer;
import com.liquidlabs.log.indexer.DelegatingFileStore;
import com.liquidlabs.log.indexer.LUFileStore;
import com.liquidlabs.log.indexer.LineStore;
import com.logscape.disco.indexer.*;
import com.logscape.disco.indexer.persistit.PersisitDbFactory;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileStore;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Using Linkedin - http://data.linkedin.com/opensource/krati/quick-start
 *
 * Performance: http://data.linkedin.com/opensource/krati/performance                                                                                                Lut
 *
 * how about this?
 * https://github.com/dain/leveldb   (Actually want a java LSM Tree)
 *
 */
public class PIIndexer extends AbstractIndexer  {
    private static final Logger LOGGER = Logger.getLogger(PIIndexer.class);
    protected LoggingEventMonitor eventMonitor = new LoggingEventMonitor();


    private String environment;

    ScheduledExecutorService scheduler = com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool("services");

    public PILineStore lineStore;
    private PIFieldStore fieldStore;

    private DelegatingFileStore fileStore;

    private IndexFeed kvIndexFeed;
    volatile long stallIndexingAt;


    public PIIndexer(String environment) {
        this(environment, KvIndexFactory.get());
    }

    public PIIndexer(String environment, IndexFeed kvIndexFeed) {

        try {

            fileStore = new DelegatingFileStore();
            if (System.getProperty("lucene.file.index","true").equals("false")) {
                fileStore.setDelegate(true, new LUFileStore(environment,  scheduler));
            } else {
                fileStore.setDelegate(false, new PIFileStore(getStore(environment, "LF", PIFileStore.threadLocal), scheduler));
            }

            LOGGER.info("Environment:" + environment);
            FileUtil.mkdir(environment);

            this.kvIndexFeed = kvIndexFeed;
            if (KvIndexFeed.rebuildRequired) {
                LOGGER.error("Cannot create IndexStore - rebuild required:" + environment);
                AbstractIndexer.rebuildIndex(environment);
                forceRestart();
            }

            this.environment = environment;


            // STORE: LogFile[] Buckets[]
            lineStore = new PILineStore(getStoreCKey(environment + "/LI", "LI", PILineStore.threadLocal), scheduler);

            // STORE: FieldSet
            fieldStore = new PIFieldStore(getStore(environment, "DT", PIFieldStore.threadLocal));
        } catch (Throwable e) {
            LOGGER.warn("Corrupted Store", e);
            // failed to open the indexer environment.... rebuilt the DB?
            AbstractIndexer.rebuildIndex(environment);
            new File(environment+DB_SCHEMA_ID).delete();
            forceRestart();
        }


        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    PIIndexer.this.sync();
                } catch (Throwable t ){
                    LOGGER.error("SyncFailed:" + t.getMessage(), t);
                }
            }
        }, 3, Integer.getInteger("pi.store.flush.interval",1), TimeUnit.MINUTES);

        if (!LogProperties.isForwarder) {
            scheduler.schedule(new Runnable() {

                public void run() {
                    try {
                        lastStats = indexStats();
                        scheduleOnIt(scheduler);
                    } catch (Throwable t) {
                        LOGGER.warn("ScheduleStatsFailed:" + t.toString());
                    }
                }
            }, 2, TimeUnit.MINUTES);
        }

        // schedule missing files cleanup
        scheduler.schedule(new Runnable() {

            public void run() {
                try {
                    LOGGER.info("Schedule - cleanup Missing files");
                    cleanupMissingFiles();
                } catch (Throwable t) {
                    LOGGER.warn("ScheduleCleanup:" + t.toString());
                }
            }
        }, 24, TimeUnit.HOURS);


    }

    private void cleanupMissingFiles() {
        final AtomicInteger filesToRemoveCount = new AtomicInteger();
        List<LogFile> files =  indexedFiles(new DateTime().minusDays(Integer.getInteger("disco.missing.max.days",14)).getMillis(), new DateTime().minusDays(2).getMillis(), false, new FilterCallback() {
            @Override
            public boolean accept(LogFile logFile) {
                if (filesToRemoveCount.get() < 10 * 1000 && !new File(logFile.getFileName()).exists()) {
                    filesToRemoveCount.incrementAndGet();
                    return true;
                }
                return false;
            }

        });
        removeFromIndex(files);
    }

    private void forceRestart()  {
        LOGGER.warn("Going to forcing agent restart");
        try {
            Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        LOGGER.warn("Forcing agent restart");

        System.exit(10);
    }


    long lastSync = 0;
    public void sync() {
        Runnable task = new Runnable() {

            @Override
            public void run() {

                if (isReadyForNextSync()) {
                    synchronized (PIIndexer.this) {
                        LOGGER.info("SyncStores: " + this.hashCode() + " NOW: " + new DateTime() + " LAST:" + new DateTime(lastSync));
                        lastSync = new DateTime().getMillis();
                        sync();
                        fileStore.sync();
                        fieldStore.sync();
                    }
                }
            }
        };
        scheduler.submit(task);
    }

    protected boolean isReadyForNextSync() {
        return new DateTime().minusSeconds(10).getMillis() >  lastSync;
    }

    public void update(String filename, Line line) {
        this.add(filename, Arrays.asList(line));
    }

    public void add(String file, int line, long time, long pos) {
        LogFile logfile = fileStore.openLogFile(file, true, "basic", "");

        ArrayList<Line> lines = new ArrayList<Line>();
        lines.add(new Line(logfile.getId(), line, time, pos));
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        add(file, lines);
    }
    public void add(String file, List<Line> lines) {

        if (isStalling()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
        this.lineStore.add(lines);
        fileStore.updateLogFileLines(file, lines);
    }

    public boolean isStalling() {
        return stallIndexingAt > System.currentTimeMillis() - 2000;
    }

    public void removeFromIndex(String file) {
        try {
            if (this.isIndexed(file)) {
                LogFile logFile = this.openLogFile(file);
                LOGGER.info("RemoveFromIndex: Removing " + logFile.getFileName() + " Tag:" + logFile.getTags() + " Age:" + logFile.getAgeInDays());
                eventMonitor.raise(new Event("IndexerRemove").with("logId", logFile.getId()).with("file", logFile.getFileName()).with("age-days",logFile.getAgeInDays()));

                this.lineStore.remove(logFile.getId(), 0, System.currentTimeMillis());
                fileStore.removeFromIndex(file);
                kvIndexFeed.remove(logFile.getId());
            }
        } catch (Throwable t) {
            LOGGER.error("RemoveFromIndexFailed:" + file, t);
        }
    }
    public int removeFromIndex(List<LogFile> logFiles) { // refactor
        eventMonitor.raise(new Event("removeFromIndex").with("Number of Files", logFiles.size()));
        for (LogFile logFile : logFiles) {
            removeFromIndex(logFile.getFileName());
        }
        sync();
        return logFiles.size();
    }


    public void removeFromIndex(String dirName, final String filePattern, boolean recurseDirectory) {
        eventMonitor.raise(new Event("IndexerRemove").with("pattern", filePattern));
        final AtomicInteger removeCount = new AtomicInteger();
        final Set<String> removeMe = new HashSet<String>();
        indexedFiles(new FilterCallback() {
            @Override
            public boolean accept(LogFile file) {
                if (file.getFileName().matches(filePattern)) {
                    removeMe.add(file.getFileName());
                    LOGGER.info("Removing:" + file);
                    removeCount.incrementAndGet();
                }

                return false;
            }
        });
        for (String filename : removeMe) {
            removeFromIndex(filename);
        }
        LOGGER.info(String.format("Removing potential [%d] files from Index - using dir[%s]", removeCount.get(), dirName));
    }
    @Override
    public FieldSet getFieldSet(String fieldSetId) {
        return this.fieldStore.get(fieldSetId);
    }

    @Override
    public void addFieldSet(final FieldSet fieldSet) {

        LOGGER.info("Adding:" + fieldSet);

        final FieldSet existingFieldSet = getFieldSet(fieldSet.getId());

        if (existingFieldSet != null && existingFieldSet.lastModified == fieldSet.lastModified && fieldSet.lastModified != 0) {
            LOGGER.info("Ignoring (same) FieldSet:" + fieldSet.getId() + " lastMod:" + fieldSet.lastModified);
            return;
        }
        long start = System.currentTimeMillis();
        eventMonitor.raise(new Event("IndexerAddFieldSet").with("fieldSetId", fieldSet.getId()).with("timeStamp", DateUtil.shortDateTimeFormat.print(fieldSet.lastModified)));
        if (existingFieldSet != null) eventMonitor.raise(new Event("IndexerAddFieldSet").with("existingFieldSetId", fieldSet.getId()).with("eTimeStamp", DateUtil.shortDateTimeFormat.print(existingFieldSet.lastModified)));
        try {
            this.fieldStore.add(fieldSet);
        } catch (Exception e) {
            throw new RuntimeException("Add FieldSet:" + fieldSet.getId() + " Failed", e);
        }

        if (LogProperties.isForwarder) return;


        // dont do any processing when basic is added - its the default type
        if (fieldSet.getId().equals("basic") || fieldSet.priority <= 1) return;

        // grab the set of FieldSets so we can filter the list quickly
        final List<FieldSet> fieldSets = getFieldSets(new AlwaysFilter());
        final Map<String,FieldSet> fs = new HashMap<String, FieldSet>();
        for (FieldSet set : fieldSets) {
            if (set.id.length() > 0) fs.put(set.id,set);

        }

        // grab the set of files who names and priority match
        fileStore.indexedFiles(new FilterCallback() {

            public boolean accept(LogFile logFile) {
                String fileName = logFile.getFileName();
                if (!new File(fileName).exists()) {
                    LOGGER.warn("Cannot Update:" + fileName + " FileNotFound, It may have been deleted");
                    return false;
                }
                FieldSet existingFieldSet = fs.get(logFile.getFieldSetId());
                if (existingFieldSet == null) existingFieldSet = fs.get("basic");

                // if already assigned to something else with higher priority ignore it
                if (existingFieldSet != null && existingFieldSet.priority > fieldSet.priority) return false;

                boolean evaluate = existingFieldSet.id.equals("basic");
                if (fieldSet.matchesFilenameAndPath(fileName) || fieldSet.matchesFilenameAndTAG(fileName, logFile.getTags())) {
                    evaluate = true;
                    // was already assigned to this and now it doesnt match
                } else if (logFile.getFieldSetId().equals(fieldSet.getId())) {
                    evaluate = true;
                }

                if (evaluate) {

                    try {
                        List<String> lines = null;
                        lines = FileUtil.readLines(fileName, LogProperties.getFieldSetMatchLines(), logFile.getNewLineRule());
                        FieldSet match = new FieldSetAssember().determineFieldSet(fileName, fieldSets, lines, false, logFile.getTags());
//                        System.out.println("FFF:>" + logFile + " Match:" + match);
                        if (match != null && !match.id.equals(logFile.getFieldSetId())) {
                            LOGGER.info("ReAssigning Type File:" + fileName + " type:" + match.getId() + " oldType:" + logFile.getFieldSetId());
                            logFile.setFieldSetId(match.getId());
                            updateLogFile(logFile);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Error updating:" + fileName + " e:" + e.toString(), e);

                    }
                }
                return false;
            }
        });
        eventMonitor.raise(new Event("IndexerAddFieldSet").with("fieldSetId", fieldSet.getId()).with("Elapsed", System.currentTimeMillis() - start));
    }

    @Override
    public List<FieldSet> getFieldSets(Filter<FieldSet> filter) {
        return this.fieldStore.list(filter);
    }

    @Override
    public void removeFieldSet(final FieldSet fieldSet) {
        eventMonitor.raise(new Event("IndexerRemoveFieldSet").with("fieldSetId", fieldSet.getId()));
        this.fieldStore.remove(fieldSet);

        final List<FieldSet> fieldSets = getFieldSets(new AlwaysFilter());
        final FieldSet basicFieldSet = FieldSets.getBasicFieldSet();


        fileStore.indexedFiles(new FilterCallback() {
            public boolean accept(LogFile logFile) {
                if (logFile.getFieldSetId().contains(fieldSet.getId())) {
                    try {
                        List<String> lines = null;
                        if (!new File(logFile.getFileName()).exists()) {
                            return false;
                        }

                        lines = FileUtil.readLines(logFile.getFileName(), 20, logFile.getNewLineRule());
                        FieldSet match = new FieldSetAssember().determineFieldSet(logFile.getFileName(), fieldSets, lines, false, logFile.getTags());
                        if (match != null) {
                            logFile.setFieldSetId(match.getId());
                        } else {
                            logFile.setFieldSetId(basicFieldSet.getId());

                        }
                        LOGGER.info("Rebase-File:" + logFile.getFileName() + " " + fieldSet.getId() + "-to-" + logFile.getFieldSetId());
                        updateLogFile(logFile);

                    } catch (Exception e) {
                        LOGGER.warn("Error updating:" + logFile, e);
                    }
                }
                return false;
            }
        });
        PIIndexer.this.sync();
    }

    @Override
    public void close() {
        LOGGER.info("Close");

        this.fieldStore.close();
        this.lineStore.close();
        this.fileStore.close();
        this.kvIndexFeed.close();;
        PersisitDbFactory.close();
        scheduler.shutdownNow();

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
    public void commit() {
        sync();
        this.kvIndexFeed.commit();
    }

    @Override
    public List<Bucket> find(String file, long startTime, long endTime) {
        LogFile logFile = fileStore.openLogFile(file);


        List<DateTime> times = logFile.getStartEndTimes();

        // called in error?
        if (startTime > times.get(1).getMillis()) return new ArrayList<Bucket>();
        if (endTime  < times.get(0).getMillis()) return new ArrayList<Bucket>();


        if (startTime < times.get(0).getMillis()) startTime = times.get(0).getMillis();
        if (endTime > times.get(1).getMillis()) endTime = times.get(1).getMillis();

        return this.lineStore.find(logFile.getId(), startTime, endTime);
    }


    @Override
    public List<Line> linesForNumbers(String file, int startLine, int endLine) {
        return this.lineStore.linesForNumbers(fileStore.openLogFile(file), startLine, endLine);
    }

    @Override
    public long filePositionForLine(String file, long line) {
        return this.lineStore.filePositionForLine(fileStore.openLogFile(file), line);
    }

    @Override
    public List<Line> linesForTime(String file, long from, int pageSize) {
        return this.lineStore.linesForTime(fileStore.openLogFile(file), from, pageSize);
    }


    @Override
    public long cleanupMissingIndexedFiles() {
        LOGGER.warn("Not implemented:cleanupMissingIndexedFiles:");
        return 0;
    }


    /**
     *
     *
     * Stats collector stuff
     */
    private IndexStats lastStats;
    int indexedFileCount;
    public IndexStats indexStats() {
        indexedFileCount = 0;
        final IndexStats stats = new IndexStats();
        indexedFiles(new FilterCallback() {
            public boolean accept(LogFile logFile) {
                indexedFileCount++;
                stats.update(logFile);
                return false;
            }
        });
        if (lastStats != null) {
            stats.log(lastStats);
        }
        return stats;
    }

    @Override
    public int size() {
        return indexedFileCount;
    }

    @Override
    public LineStore lineStore() {
        return lineStore;
    }

    private void scheduleOnIt(ScheduledExecutorService scheduler) {

        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    Thread.currentThread().setPriority(Thread.NORM_PRIORITY -1);
                    PIIndexer.this.lastStats = indexStats();
                } catch (Throwable t) {
                    LOGGER.warn("IndexStatsFailed:" + t.toString(),t);
                } finally {
                    Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
                }
            }
        }, new DateTime().getMinuteOfHour() % 10, LogProperties.getIndexStatsLogInterval(), TimeUnit.MINUTES);
    }
    /**
     * Used to slow down import because a search is being executed
     */
    public void stallIndexingForSearch(){
        stallIndexingAt = System.currentTimeMillis();
    }


    public IndexStats getLastStats() {
        return this.lastStats;
    }
    public long getLineStoreSize() {
        return this.lineStore.size();
    }


    public long[] getLastPosAndLastLine(String filename) {
        return fileStore.getLastPosAndLastLine(filename);
    }

    public int open(String file, boolean createNew, String fieldSetId, String sourceTags) {
        return fileStore.open(file, createNew, fieldSetId, sourceTags);
    }

    @Override
    public void addFileDeletedListener(FileDeletedListener listener) {
        this.fileStore.addFileDeletedListener(listener);
    }

    public LogFile openLogFile(String file, boolean create, String fieldSetId, String sourceTags) {
        return fileStore.openLogFile(file, create, fieldSetId, sourceTags);
    }

    public LogFile openLogFile(String logFilename) {
        return fileStore.openLogFile(logFilename);
    }

    @Override
    public LogFile openLogFile(int logId) {
        return fileStore.openLogFile(logId);
    }
    public void indexedFiles(FilterCallback callback) {
        fileStore.indexedFiles(callback);
    }

    @Override
    public List<LogFile> indexedFiles(long startTimeMs, long endTimeMs, boolean sortByTime, FilterCallback callback) {
        return fileStore.indexedFiles(startTimeMs, endTimeMs, sortByTime, callback);
    }


    public List<DateTime> getStartAndEndTimes(String filename) {
        return fileStore.getStartAndEndTimes(filename);
    }

    public void updateLogFile(LogFile openLogFile) {
        fileStore.updateLogFile(openLogFile);
    }

    public void updateLogFileLines(String file, List<Line> lines) {
        fileStore.updateLogFileLines(file, lines);
    }

    public boolean isIndexed(String absolutePath) {
        return fileStore.isIndexed(absolutePath);
    }

    public boolean assignFieldSetToLogFile(String filename, String fieldSetId) {
        return fileStore.assignFieldSetToLogFile(filename, fieldSetId);
    }
    public String rolled(String fromFile, String toFile){
        return fileStore.rolled(fromFile, toFile);
    }


}
