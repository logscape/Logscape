package com.liquidlabs.log.indexer;

import com.liquidlabs.common.collection.cache.ConcurrentLRUCache;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import javolution.util.FastMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Cache-Enabled Facade that uses update-coallescing to reduce IO-Thrashing
 */
public class LruFileStore implements LogFileOps {
    private static final Logger LOGGER = Logger.getLogger(LruFileStore.class);

    public static final int DELAY = 1;
    final private LogFileOps delegate;
    private static int MAX = Integer.getInteger("lru.fs.max",LogProperties.getTailersMax() * Integer.getInteger("lru.scale.cache", 3));
    ConcurrentLRUCache<String, LogFile> cache = new ConcurrentLRUCache<String, LogFile>(MAX, (int)(MAX *.8), Boolean.getBoolean("lru.run.cleanup.thread"));
    ConcurrentLRUCache<Integer, LogFile> idCache = new ConcurrentLRUCache<Integer, LogFile>(MAX, (int)(MAX *.8),  Boolean.getBoolean("lru.run.cleanup.thread"));
    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");
    volatile boolean sync = false;
    FastMap<String, LogFile> queue = new FastMap<String, LogFile>(MAX).shared();
    private FileDeletedListener deletedListener;


    public LruFileStore(final LogFileOps delegate) {
        cache.setName("LruFileStore-LF");
        cache.setName("LruFileStore-ID");
        this.delegate = delegate;
        LOGGER.info("LRUSIZE:" + MAX);

        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (sync) {
                    flushCache();
                    sync = false;
                }
            }
        }, 1, DELAY, TimeUnit.SECONDS);

        // In case the cache ever gets out of step then just clear stuff out
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                cache.clear();
                idCache.clear();
            }
        }, 5, Integer.getInteger("lru.cache.clear.mins", 30), TimeUnit.MINUTES);
    }

    private void flushCache() {
        Set<String> strings = queue.keySet();
        for (String string : strings) {
            try {
                LogFile removed = queue.remove(string);
                delegate.updateLogFile(removed);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
        delegate.sync();
    }

    public LogFile openLogFile(String file, boolean create, String fieldSetId, String sourceTags) {
        LogFile logFile = openLogFile(file);
        if (logFile == null) logFile = delegate.openLogFile(file, create, fieldSetId, sourceTags);
        if (logFile != null) {
            cache.put(logFile.getFileName(), logFile);
            idCache.put(logFile.getId(), logFile);
        } else {
            cache.remove(file);
        }
        return logFile;
    }

    public LogFile openLogFile(String logFilename) {
        LogFile cached = getCached(logFilename);
        if (cached != null) return cached;

        LogFile logFile = delegate.openLogFile(logFilename);
        if (logFile != null) {
            cache.put(logFile.getFileName(), logFile);
            idCache.put(logFile.getId(), logFile);
        } else {
            cache.remove(logFilename);
        }
        return logFile;
    }

    private LogFile getCached(String logFilename) {
        LogFile cached = cache.get(logFilename);
        if (cached != null) return  cached;
        LogFile queuedLogFile = queue.get(logFilename);
        if (queuedLogFile != null) {
            return queuedLogFile;
        }
        return null;
    }

    public LogFile openLogFile(int logId) {
        LogFile logFile1 = idCache.get(logId);
        if (logFile1 != null) {
            return logFile1;
        }

        LogFile logFile = delegate.openLogFile(logId);
        if (logFile != null) {
            LogFile cached = getCached(logFile.getFileName());
            if (cached != null) return cached;
            cache.put(logFile.getFileName(), logFile);
            idCache.put(logFile.getId(), logFile);
        } else {
            idCache.remove(logId);
            throw new RuntimeException("Failed to find logFile with id:" + logId);
        }
        return logFile;
    }

    public void close() {
        flushCache();
        delegate.close();
    }

    public void removeFromIndex(String file) {
        flushCache();
        cache.clear();
        idCache.clear();

        LogFile logFile = openLogFile(file);

        if (logFile != null) {
            if (deletedListener != null) deletedListener.deleted(file);
            LOGGER.info("Removing:" + file);
            cache.remove(file);
            idCache.remove(logFile.getId());
            queue.remove(file);

            delegate.removeFromIndex(file);
            sync();
        } else {
            LOGGER.info("RemoveFromIndex FileNotFound:" + file);
        }

    }

    public void updateLogFile(LogFile logFile) {
        cache.put(logFile.getFileName(), logFile);
        idCache.put(logFile.getId(), logFile);
        queue.put(logFile.getFileName(), logFile);
        sync();
    }

    public int open(String file, boolean createNew, String fieldSetId, String sourceTags) {
        LogFile logFile1 = cache.get(file);
        if (logFile1 != null) return logFile1.getId();

        LogFile logFile = delegate.openLogFile(file, createNew, fieldSetId, sourceTags);
        cache.put(file, logFile);
        return logFile.getId();
    }

    public void updateLogFileLines(String file, List<Line> lines) {
        LogFile logFile = openLogFile(file);
        if (logFile == null) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException iex) {}
            logFile = openLogFile(file);
        }
        if (logFile != null) {
            logFile.update(lines);
        } else {
            throw new RuntimeException("Failed to update lines Missing File:" + file);
        }

        cache.put(file, logFile);
        idCache.put(logFile.getId(), logFile);
        queue.put(file, logFile);
    }

    public boolean isIndexed(String absolutePath) {
        if( cache.getMap().containsKey(absolutePath)) return true;
        else return delegate.isIndexed(absolutePath);
    }

    public void add(String file, int line, long time, long pos) {
        LogFile logFile = openLogFile(file);

        if (logFile != null) {
            logFile.update(pos, new Line(logFile.getId(), line, time, pos));
            cache.put(file, logFile);
            idCache.put(logFile.getId(), logFile);
            queue.put(file, logFile);
        }
    }
    public String rolled(String fromFile, String toFile) {

        LogFile remove1 = cache.remove(fromFile);
        if (remove1 != null) idCache.remove(remove1.getId());
        LogFile remove = queue.remove(fromFile);
        // push consistency changes to the store
        if (remove != null) {
            idCache.remove(remove.getId());
            delegate.updateLogFile(remove);
        }
        return delegate.rolled(fromFile, toFile);
    }

    public boolean assignFieldSetToLogFile(String logFile, String fieldSetId) {
        LogFile logFile1 = openLogFile(logFile);
        logFile1.setFieldSetId(fieldSetId);
        updateLogFile(logFile1);
        return true;
    }

    public void indexedFiles(FilterCallback callback) {
        delegate.indexedFiles(callback);
    }

    public List<LogFile> indexedFiles(long startTimeMs, long endTimeMs, boolean sortByTime, FilterCallback callback) {
        return delegate.indexedFiles(startTimeMs, endTimeMs, sortByTime, callback);
    }

    public List<DateTime> getStartAndEndTimes(String filename) {
        LogFile logFile = cache.get(filename);
        if (logFile == null) logFile = queue.get(filename);
        if (logFile != null) return logFile.getStartEndTimes();

        return delegate.getStartAndEndTimes(filename);
    }

    public long[] getLastPosAndLastLine(String filename) {
        LogFile file = openLogFile(filename);
        return new long[] { file.getPos(), file.getLineCount()};
    }

    public void sync() {
        // enable flag to push changes to delegate
        sync = true;
    }

    @Override
    public void addFileDeletedListener(FileDeletedListener listener) {
        deletedListener = listener;
    }
}
