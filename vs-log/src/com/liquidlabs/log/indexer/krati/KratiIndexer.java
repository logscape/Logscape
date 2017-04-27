package com.liquidlabs.log.indexer.krati;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSetAssember;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.*;
import com.liquidlabs.log.indexer.AbstractIndexer;
import com.liquidlabs.log.indexer.LineStore;
import com.logscape.disco.indexer.*;
import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.SegmentFactory;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Using Linkedin - http://data.linkedin.com/opensource/krati/quick-start
 *
 * Performance: http://data.linkedin.com/opensource/krati/performance
 *
 * how about this?
 * https://github.com/dain/leveldb
 *
 */
public class KratiIndexer extends KratiFileStore implements Indexer {
    private static final Logger LOGGER = Logger.getLogger(KratiIndexer.class);

    private String environment;

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    private KratiLineStore lineStore;
    private KratiDataTypeStore fieldStore;
    private IndexFeed kvIndexFeed;

    public KratiIndexer(String environment) {
        this(environment, KvIndexFactory.get());
    }

    public KratiIndexer(String environment, IndexFeed kvIndexFeed) {
        super(environment, Executors.newSingleThreadExecutor());

        LOGGER.info("Environment:" + environment);
        FileUtil.mkdir(environment);

        this.kvIndexFeed = kvIndexFeed;
        if (store == null || kvIndexFeed != null && kvIndexFeed.isRebuildRequired()) {
            AbstractIndexer.rebuildIndex(environment);
        }

        this.environment = environment;

        try {
            // STORE: LogFile[] Buckets[]
            lineStore = new KratiLineStore(environment, scheduler);

            // STORE: FieldSet
            fieldStore = new KratiDataTypeStore(environment, scheduler);
        } catch (Throwable e) {
            LOGGER.warn("Corrupted Store", e);
            AbstractIndexer.rebuildIndex(environment);
        }


        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    KratiIndexer.this.syncStores();
                } catch (Throwable t ){
                    LOGGER.error("SyncFailed:" + t.getMessage(), t);
                }
            }
        }, 3, 3, TimeUnit.MINUTES);

        // need to do initial setup
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


    void syncStores() {
        LOGGER.info("SyncStores");
        super.sync();
        this.fieldStore.sync();
        this.lineStore.sync();
    }


    public void update(String filename, Line line) {
        this.add(filename, Arrays.asList(line));
    }
    public long getLineStoreSize() {
        return 100;// this.lineStore.size();
    }

    public void add(String file, int line, long time, long pos) {
        LogFile logfile = openLogFile(file, true, "basic","");

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
        this.lineStore.add(lines);
        super.updateLogFileLines(file, lines);
    }

    public void removeFromIndex(String file) {
        try {
            if (this.isIndexed(file)) {
                LogFile logFile = this.openLogFile(file);

                eventMonitor.raise(new Event("kratiIndexerRemove").with("logId", logFile.getId()).with("file", logFile.getFileName()).with("age-days",logFile.getAgeInDays()));

                List<DateTime> startEndTimes = logFile.getStartEndTimes();

                this.lineStore.remove(logFile.getId(), startEndTimes.get(0).getMillis(), startEndTimes.get(1).getMillis());
                super.removeFromIndex(file);
                kvIndexFeed.remove(logFile.getId());
            }
        } catch (Throwable t) {
            LOGGER.error("RemoveFromIndexFailed:" + file, t);
        }
    }
    public int removeFromIndex(List<LogFile> logFiles) {
        for (int i = 0; i < logFiles.size(); i++) {
            LogFile logFile = logFiles.get(i);
            removeFromIndex(logFile.getFileName());
        }
        return logFiles.size();
    }

    @Override
    public boolean isStalling() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }


    public void removeFromIndex(String dirName, final String filePattern, boolean recurseDirectory) {
        eventMonitor.raise(new Event("kratiIndexerRemove").with("pattern", filePattern));
        final AtomicInteger removeCount = new AtomicInteger();
        indexedFiles(new FilterCallback() {
            @Override
            public boolean accept(LogFile file) {
                if (file.getFileName().matches(filePattern)) {
                    removeFromIndex(file.getFileName());
                    LOGGER.info("Removing:" + file);
                    removeCount.incrementAndGet();
                }

                return false;
            }
        });
        LOGGER.info(String.format("Removing potential [%d] files from Index - using dir[%s]", removeCount.get(), dirName));
    }



    @Override
    public FieldSet getFieldSet(String fieldSetId) {
        return this.fieldStore.get(fieldSetId);
    }

    @Override
    public void addFieldSet(final FieldSet fieldSet) {

        final FieldSet existingFieldSet = getFieldSet(fieldSet.getId());

        if (existingFieldSet != null && existingFieldSet.lastModified == fieldSet.lastModified && fieldSet.lastModified != 0) {
            LOGGER.info("Ignoring (same) FieldSet:" + fieldSet.getId() + " lastMod:" + fieldSet.lastModified);
            return;
        }
        if (fieldSet.lastModified == 0) fieldSet.lastModified = System.currentTimeMillis();

        eventMonitor.raise(new Event("kratiIndexerAddFieldSet").with("fieldSetId", fieldSet.getId()));
        try {
            this.fieldStore.add(fieldSet);
        } catch (Exception e) {
            throw new RuntimeException("Add FieldSet:" + fieldSet.getId() + " Failed", e);
        }

        // dont do any processing when basic is added - its the default type
        if (fieldSet.getId().equals("basic") || fieldSet.priority <= 1) return;

        // grab the set of FieldSets so we can filter the list quickly
        final List<FieldSet> fieldSets = getFieldSets(new Indexer.AlwaysFilter());
        final Map<String,FieldSet> fs = new HashMap<String, FieldSet>();
        for (FieldSet set : fieldSets) {
            fs.put(set.id,set);

        }

        // grab the set of files who names and priority match
        super.indexedFiles(new FilterCallback() {

            public boolean accept(LogFile logFile) {
                String fileName = logFile.getFileName();
                if (!new File(fileName).exists()) {
                    LOGGER.warn("Cannot Update:" + fileName + " FileNotFound in Index, It may have been deleted");
                    removeFromIndex(fileName);
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
                            LOGGER.info("ReAssigning Type File:" + fileName + " type:" + match.getId());
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
        KratiIndexer.this.syncStores();
    }

    @Override
    public List<FieldSet> getFieldSets(Filter<FieldSet> filter) {
        return this.fieldStore.list(filter);
    }

    @Override
    public void removeFieldSet(final FieldSet fieldSet) {
        eventMonitor.raise(new Event("kratiIndexerRemoveFieldSet").with("fieldSetId", fieldSet.getId()));
        this.fieldStore.remove(fieldSet);

        final List<FieldSet> fieldSets = getFieldSets(new Indexer.AlwaysFilter());
        final FieldSet basicFieldSet = FieldSets.getBasicFieldSet();


        super.indexedFiles(new FilterCallback() {
            public boolean accept(LogFile logFile) {
                if (logFile.getFieldSetId().contains(fieldSet.getId())){
                    try {
                        List<String> lines = null;
                        if (!new File(logFile.getFileName()).exists()) {
                            removeFromIndex(logFile.getFileName());
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
        KratiIndexer.this.syncStores();
    }

    @Override
    public void close() {
        LOGGER.info("Close");
        scheduler.shutdownNow();
        this.fieldStore.close();
        this.lineStore.close();
        super.close();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }



    @Override
    public List<Bucket> find(String file, long startTime, long endTime) {
        LogFile logFile = super.openLogFile(file);
        List<DateTime> times = logFile.getStartEndTimes();
        if (startTime < times.get(0).getMillis()) startTime = times.get(0).getMillis();
        if (endTime > times.get(1).getMillis()) endTime = times.get(1).getMillis();
        return this.lineStore.find(logFile.getId(), startTime, endTime);
    }


    @Override
    public List<Line> linesForNumbers(String file, int startLine, int endLine) {
        return this.lineStore.linesForNumbers(super.openLogFile(file), startLine, endLine);
    }

    @Override
    public long filePositionForLine(String file, long line) {
        return this.lineStore.filePositionForLine(super.openLogFile(file), line);
    }

    @Override
    public List<Line> linesForTime(String file, long from, int pageSize) {
        return this.lineStore.linesForTime(super.openLogFile(file), from, pageSize);
    }


    @Override
    public long cleanupMissingIndexedFiles() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }


    /**
     *
     *
     * Stats collector stuff
     */
    private IndexStats lastStats;
    public IndexStats indexStats() {
        final IndexStats stats = new IndexStats();
        indexedFiles(new FilterCallback() {
            public boolean accept(LogFile logFile) {
                stats.update(logFile);
                return false;
            }
        });
        if (lastStats != null) {
            stats.log(lastStats);
        }
        return stats;
    }
    private void scheduleOnIt(ScheduledExecutorService scheduler) {
        int tenMinuteOffsetInHour = new DateTime().getMinuteOfHour();

        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    Thread.currentThread().setPriority(Thread.NORM_PRIORITY -1);
                    KratiIndexer.this.lastStats = indexStats();
                } catch (Throwable t) {
                    LOGGER.warn("IndexStatsFailed:" + t.toString(),t);
                } finally {
                    Thread.currentThread().setPriority(Thread.NORM_PRIORITY);
                }
            }
        }, tenMinuteOffsetInHour % 10, 10, TimeUnit.MINUTES);
    }

    public IndexStats getLastStats() {
        return this.lastStats;
    }
    /**
     * Used to slow down import because a search is being executed
     */
    public void stallIndexingForSearch(){
//        stallIndexingAt = System.currentTimeMillis();
    }


    static String cfClass = System.getProperty("krati.seg.factory", MappedSegmentFactory.class.getName());
	public static SegmentFactory getChannelFactory() {
		try {
			Constructor<?> ctor = Class.forName(cfClass).getConstructor();
			LOGGER.info("Using SegmentType:" + cfClass);
			return (SegmentFactory) ctor.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		 
		return null;
	}

    @Override
    public int size() {
        return 0;
    }

    @Override
    public LineStore lineStore() {
        return this.lineStore;
    }
}
