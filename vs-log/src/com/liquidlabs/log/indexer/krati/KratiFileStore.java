package com.liquidlabs.log.indexer.krati;

import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.transport.serialization.Convertor;
import krati.core.StoreConfig;
import krati.store.DynamicDataStore;
import krati.store.SafeDataStoreHandler;
import krati.util.IndexedIterator;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 10:44
 * To change this template use File | Settings | File Templates.
 */
public class KratiFileStore extends AbstractKratiStore implements LogFileOps {
    private static final Logger LOGGER = Logger.getLogger(KratiFileStore.class);

    public final static String FILE_SEED = "FILE_SEED_KEY";
    private volatile int nextFileId;
    private ExecutorService executor;
    protected LoggingEventMonitor eventMonitor = new LoggingEventMonitor();

    public KratiFileStore(String environment, ExecutorService executor) {
        super.LOGGER = LOGGER;
        this.executor = executor;
        // create env with
        // LogFile
        try {
            StoreConfig _config = new StoreConfig(new File(environment + "/FILE"), 7 /* 7 days */ * 1000 /* 1k per day */);

            //_config.setSegmentFactory(new MemorySegmentFactory());
            _config.setSegmentFactory(KratiIndexer.getChannelFactory());
            _config.setSegmentFileSizeMB(8);
            _config.setDataHandler(new SafeDataStoreHandler());

            store = new DynamicDataStore(_config);

            initFileSeed();

        } catch (Exception e) {
            LOGGER.error("Failed to open File", e);
        }

    }


    private void initFileSeed() {
        byte[] seed = store.get(FILE_SEED.getBytes());
        if (seed != null) {
            try {
                nextFileId = (Integer) (Convertor.deserialize(seed));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public int open(String file, boolean createNew, String fieldSetId, String sourceTags) {
        return openLogFile(file, createNew, fieldSetId, sourceTags).getId();
    }

    @Override
    public void addFileDeletedListener(FileDeletedListener listener) {

    }

    @Override
    public LogFile openLogFile(String file, boolean create, String fieldSetId, String sourceTags) {
        byte[] bytes = store.get(file.getBytes());
        if (bytes != null) {
            try {
                return (LogFile) Convertor.deserialize(bytes);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("");
            }
        }
        if (create) {
            LogFile logFile = new LogFile(file, nextFileId++, fieldSetId, sourceTags);
            writeFileId();
            eventMonitor.raise(new Event("kratiIndexerCreate").with("file", file).with("id", logFile.getId()));

            try {
                store.put(file.getBytes(), Convertor.serialize(logFile));
            } catch (Exception e) {
                e.printStackTrace();
            }

            return logFile;
        }

        return null;

    }

    /**
     * Return the systems known last modified date - WHEN we are up to date.
     * IF we are out of date - then return 0
     *
     * @param logFilename
     * @return
     */
    public long lastMod(String logFilename) {
        if (store == null) throw new RuntimeException("STORE is NULL");
        byte[] obj = store.get(logFilename.getBytes());
        if (obj == null) return 0;
        LogFile logFile = null;
        try {
            logFile = (LogFile) Convertor.deserialize(obj);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
        // We know the file (have tailed it)
        // Tail pos is recent
        // It hasnt changed for 7 days

        // When the file changes - then this rule will fail
        File file = new File(logFilename);
        if (logFile != null && file.lastModified() < new DateTime().minusDays(7).getMillis() && logFile.getPos() > file.length() - 10 * 1024) return logFile.getEndTime();
        return 0;
    }

    public boolean isTailing(String filename) {
        return store.get(filename.getBytes()) != null;
    }

    @Override
    public LogFile openLogFile(String logFilename) {
        byte[] obj = store.get(logFilename.getBytes());
        if (obj == null) return null;//throw new RuntimeException("LogFileNotFoundInStore:" + logFilename);

        try {
            return (LogFile) Convertor.deserialize(obj);
        } catch (Exception e) {
            LOGGER.error("Cannot Load File:" + logFilename, e);
            throw new RuntimeException("Cannot Read File:" + logFilename);
        }
    }

    @Override
    public LogFile openLogFile(int logId) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    // By default track all files we have ever watched
    boolean allowCompleteRemove = true;//Boolean.getBoolean("disco.allow.complete.remove");
    public void removeFromIndex(String file) {
        LOGGER.info("Remove:" + file);
        if (allowCompleteRemove) {
            try {
                store.delete(file.getBytes());
            } catch (Exception e) {
                LOGGER.error("Remove Error:" + file, e);
            }
        }
        sync();
    }


    private void writeFileId() {
        executor.execute(new Runnable() {
            public void run() {
                try {
                    store.put(FILE_SEED.getBytes(), Convertor.serialize(Integer.valueOf(nextFileId)));
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
        });
    }



    public void updateLogFile(LogFile logFile) {
        try {
            store.put(logFile.getFileName().getBytes(), Convertor.serialize(logFile));
        } catch (Exception e) {
            throw new RuntimeException("Failed:" + e, e);
        }
    }



    public void setLogFileTimeFormat(String logCanonical, String timeFormat) {
        LogFile logFile = openLogFile(logCanonical);
        logFile.setTimeFormat(timeFormat);
        updateLogFile(logFile);
    }

    public void updateLogFileLines(String file, List<Line> lines) {
        LogFile logFile = openLogFile(file);
        for (Line line : lines) {
            logFile.update(line.filePos, line);
        }
        updateLogFile(logFile);
    }

    public boolean isIndexed(String absolutePath) {
        return store.get(absolutePath.getBytes()) != null;
    }

    @Override
    public void add(String file, int line, long time, long pos) {
        LogFile logFile = openLogFile(file);
        if (logFile != null) {
            logFile.update(pos, new Line(logFile.getId(), line, time, pos));
            updateLogFile(logFile);
        }
    }

    @Override
    public String rolled(String fromFile, String toFile) {
        LogFile fromLog = openLogFile(fromFile);
        LOGGER.info("Rolling: id:" + fromLog.getId() + ":"+fromFile + " -> " + toFile);
        LogFile rolledTo = new LogFile(toFile, fromLog);
        try {
            store.put(toFile.getBytes(), Convertor.serialize(rolledTo));
            store.delete(fromFile.getBytes());
            store.persist();
        } catch (Exception e) {
            LOGGER.error("RollError:" + fromFile,e);
        }
        return toFile;
    }


    public List<String> indexedFiles() {
        List<String> results = new ArrayList<String>();
        IndexedIterator<byte[]> indexedIterator = store.keyIterator();
        while (indexedIterator.hasNext()) {
            String file = new String(indexedIterator.next());
            if (!file.equalsIgnoreCase(FILE_SEED)) results.add(file);
        }
        return results;
    }


    public List<LogFile> indexedFiles(long startTimeMs, long endTimeMs, boolean sortByTime, FilterCallback callback) {
        SpanningListLogFilesCB worker = new SpanningListLogFilesCB(store, startTimeMs, endTimeMs, sortByTime, callback);
        worker.doWork();
        return worker.getResults();
    }

    @Override
    public void indexedFiles(FilterCallback callback) {
        SpanningListLogFilesCB worker = new SpanningListLogFilesCB(store, 0, System.currentTimeMillis(), false, callback);
        worker.doWork();
    }


    public List<DateTime> getStartAndEndTimes(String filename) {
        LogFile file = openLogFile(filename);
        return file.getStartEndTimes();
    }
    public Set<String> listStores() {
        return new HashSet<String>(indexedFiles());
    }



    public long[] getLastPosAndLastLine(String filename) {
        LogFile file = openLogFile(filename);
        return new long[] { file.getPos(), file.getLineCount()};
    }

    public boolean fileExists(String filename) {
        return store.get(filename.getBytes()) != null;
    }

    public boolean assignFieldSetToLogFile(String filename, String fieldSetId) {
        LogFile file = openLogFile(filename);
        file.setFieldSetId(fieldSetId);
        updateLogFile(file);
        return true;
    }


}
