package com.liquidlabs.log.indexer;

import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 17/02/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class DelegatingFileStore implements LogFileOps {
    private LogFileOps delegate;
    private FileDeletedListener deletedListener;

    public DelegatingFileStore() {
    }
    public void setDelegate(boolean lru, LogFileOps delegate){
        if (lru) {
            this.delegate = new LruFileStore(delegate);
        } else {
            this.delegate = delegate;
        }

    }

    public long[] getLastPosAndLastLine(String filename) {
        return delegate.getLastPosAndLastLine(filename);
    }

    public int open(String file, boolean createNew, String fieldSetId, String sourceTags) {
        return delegate.open(file, createNew, fieldSetId, sourceTags);
    }

    @Override
    public void add(String file, int line, long time, long pos) {
        delegate.add(file, line, time, pos);
    }

    @Override
    public void addFileDeletedListener(FileDeletedListener listener) {
        deletedListener = listener;
    }

    public LogFile openLogFile(String file, boolean create, String fieldSetId, String sourceTags) {
        return delegate.openLogFile(file, create, fieldSetId, sourceTags);
    }

    public LogFile openLogFile(String logFilename) {
        return delegate.openLogFile(logFilename);
    }

    @Override
    public LogFile openLogFile(int logId) {
        return delegate.openLogFile(logId);
    }
    public void indexedFiles(FilterCallback callback) {
        delegate.indexedFiles(callback);
    }

    @Override
    public List<LogFile> indexedFiles(long startTimeMs, long endTimeMs, boolean sortByTime, FilterCallback callback) {
        return delegate.indexedFiles(startTimeMs, endTimeMs, sortByTime, callback);
    }


    public List<DateTime> getStartAndEndTimes(String filename) {
        return delegate.getStartAndEndTimes(filename);
    }

    public void updateLogFile(LogFile openLogFile) {
        delegate.updateLogFile(openLogFile);
    }

    public void updateLogFileLines(String file, List<Line> lines) {
        delegate.updateLogFileLines(file, lines);
    }

    public boolean isIndexed(String absolutePath) {
        return delegate.isIndexed(absolutePath);
    }
    public void removeFromIndex(String file) {
        if (deletedListener != null) deletedListener.deleted(file);
        delegate.removeFromIndex(file);
    }

    public boolean assignFieldSetToLogFile(String filename, String fieldSetId) {
       return delegate.assignFieldSetToLogFile(filename, fieldSetId);
    }
    public String rolled(String fromFile, String toFile){
        return delegate.rolled(fromFile, toFile);
    }
    public void sync() {
        delegate.sync();
    }
    public void close() {
        delegate.close();
    }


}
