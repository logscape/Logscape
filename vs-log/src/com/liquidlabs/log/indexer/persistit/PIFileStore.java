package com.liquidlabs.log.indexer.persistit;

import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.index.*;
import com.liquidlabs.log.indexer.AbstractIndexer;
import com.logscape.disco.indexer.*;
import com.liquidlabs.transport.serialization.Convertor;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 10:44
 * To change this template use File | Settings | File Templates.
 */
public class PIFileStore implements LogFileOps {
    private int PI_FILE_CACHE = Integer.getInteger("pi.line.idx.size", 10 * 1024);
    private static final Logger LOGGER = Logger.getLogger(PIFileStore.class);

    Db<String, byte[]> store;
    public final static String FILE_SEED = "FILE_SEED_KEY";
    private volatile int nextFileId;
    private ExecutorService executor;
    protected LoggingEventMonitor eventMonitor = new LoggingEventMonitor();

    public PIFileStore(Db<String, byte[]> lf , ExecutorService executor) {
        this.executor = executor;
//        Db<String, byte[]> lf = AbstractIndexer.getStore(environment, "LF", threadLocal);
        this.store = new CachedDb<String, byte[]>(lf, PI_FILE_CACHE, false);

        initFileSeed();
    }

    public static final ThreadLocal threadLocal = new ThreadLocal();

    private void initFileSeed() {
        byte[] seed = store.get(FILE_SEED);
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
        byte[] bytes = store.get(file);
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
            eventMonitor.raise(new Event("PFIndexerCreate").with("file", file).with("id", logFile.getId()));

            try {
                store.put(file, Convertor.serialize(logFile));
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
//    public long lastMod(String logFilename) {
//        if (store == null) throw new RuntimeException("STORE is NULL");
//        byte[] obj = store.get(logFilename);
//        if (obj == null) return 0;
//        LogFile logFile = null;
//        try {
//            logFile = (LogFile) Convertor.deserialize(obj);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return 0;
//        }
//        // We know the file (have tailed it)
//        // Tail pos is recent
//        // It hasnt changed for 7 days
//
//        // When the file changes - then this rule will fail
//        File file = new File(logFilename);
//        if (logFile != null && file.lastModified() < new DateTime().minusDays(7).getMillis() && logFile.getPos() > file.length() - 10 * 1024) return logFile.getEndTime();
//        return 0;
//    }

    @Override
    public LogFile openLogFile(String logFilename) {
        byte[] obj = store.get(logFilename);
        if (obj == null) return null;//throw new RuntimeException("LogFileNotFoundInStore:" + logFilename);

        try {
            return (LogFile) Convertor.deserialize(obj);
        } catch (Exception e) {
            LOGGER.error("Cannot Load File:" + logFilename, e);
            throw new RuntimeException("Cannot Read File:" + logFilename);
        }
    }
    public LogFile openLogFile(int logFilename) {
        Set<String> keys = store.keySet();
        for (String key : keys) {
            LogFile logFile = openLogFile(key);
            if (logFile.getId() == logFilename) return logFile;
        }
        return new LogFile("NotFound",logFilename, "basic","unknown");

    }
    // By default track all files we have ever watched
    public void removeFromIndex(String file) {
        if(LOGGER.isDebugEnabled()) LOGGER.debug("Remove:" + file);
        try {
            store.remove(file);
        } catch (Exception e) {
            LOGGER.error("Remove Error:" + file, e);
        }
        sync();
    }


    private void writeFileId() {
        executor.execute(new Runnable() {
            public void run() {
                try {
                    store.put(FILE_SEED, Convertor.serialize(Integer.valueOf(nextFileId)));
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
        });
    }

    public void updateLogFile(LogFile logFile) {
        try {
            store.put(logFile.getFileName(), Convertor.serialize(logFile));
        } catch (Exception e) {
            throw new RuntimeException("Failed:" + e, e);
        }
    }



    public void updateLogFileLines(String file, List<Line> lines) {
        LogFile logFile = openLogFile(file);
        for (int i = 0; i < lines.size(); i++) {
            Line line = lines.get(i);
            logFile.update(line.filePos, line);
        }
        updateLogFile(logFile);
    }

    public boolean isIndexed(String absolutePath) {
        return store.get(absolutePath) != null;
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
            rolledTo.setAppendable(false);
            store.put(toFile, Convertor.serialize(rolledTo));
            store.remove(fromFile);
            store.commit();
        } catch (Exception e) {
            LOGGER.error("RollError:" + fromFile,e);
        }
        return toFile;
    }


    public void indexedFiles(FilterCallback callback) {
        indexedFiles(0, System.currentTimeMillis(), false, callback);
    }
    public List<LogFile> indexedFiles(long startTimeMs, long endTimeMs, boolean sortByTime, FilterCallback callback) {
        List<LogFile> results = new ArrayList<LogFile>();
        for (byte[] fileBytes : store.values()) {
            LogFile logFile = null;
            try {
                Object obj = Convertor.deserialize(fileBytes);
                if (!(obj instanceof  LogFile)) continue;
                logFile = (LogFile) obj;
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (ClassNotFoundException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }


            if (logFile.getFileName().equalsIgnoreCase(FILE_SEED)) continue;
            try {

                if (logFile.isWithinTime(startTimeMs, endTimeMs) && callback.accept(logFile)) results.add(logFile);

            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }


        if (sortByTime) {
            Collections.sort(results, new Comparator<LogFile>() {
                @Override
                public int compare(LogFile o1, LogFile o2) {
                    return Long.valueOf(o2.getEndTime()).compareTo(o1.getEndTime());
                }
            });
        }
        LogFile.dedup(results);

        return results;
    }

    public void close() {
        try {
            if (store != null) {
                this.store.commit();
                this.store.close();
                this.store = null;
            }
        } catch (Exception e) {
            LOGGER.error("Close error", e);
        }
    }


    public List<DateTime> getStartAndEndTimes(String filename) {
        LogFile file = openLogFile(filename);
        return file.getStartEndTimes();
    }

    public long[] getLastPosAndLastLine(String filename) {
        LogFile file = openLogFile(filename);
        return new long[] { file.getPos(), file.getLineCount()};
    }

    public boolean assignFieldSetToLogFile(String filename, String fieldSetId) {
        LogFile file = openLogFile(filename);
        file.setFieldSetId(fieldSetId);
        updateLogFile(file);
        return true;
    }


    public void sync() {
        try {
            this.store.commit();;
        } catch (Exception e) {
            LOGGER.error("Sync error", e);
        }
    }
}
