package com.liquidlabs.log.indexer.krati;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.indexer.txn.krati.ListLogFiles;
import krati.store.DataStore;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Build a timeseries-descending list of files
 */
public class SpanningListLogFiles {
    private final DataStore<byte[], byte[]> store;
    private final String[] fileFiltersAndTags;
    private final int limit;
    private final long startTimeMs;
    private final long endTimeMs;
    private final boolean sortByTime;
    private final LogFileOps.FilterCallback callback;
    private List<LogFile> results = new ArrayList<LogFile>();

    public SpanningListLogFiles(DataStore<byte[], byte[]> store, String[] fileFiltersAndTags, int limit, long startTimeMs, long endTimeMs, boolean sortByTime, LogFileOps.FilterCallback callback) {
        this.store = store;
        this.fileFiltersAndTags = fileFiltersAndTags;
        this.limit = limit;
        this.startTimeMs = startTimeMs;
        this.endTimeMs = endTimeMs;
        this.sortByTime = sortByTime;
        this.callback = callback;
    }

    public List<LogFile> getResults() {
        return results;
    }

    public void doWork() {
        if (callback == null || endTimeMs - startTimeMs < DateUtil.HOUR * 24) {
            ListLogFiles worker = new ListLogFiles(store, fileFiltersAndTags, limit, startTimeMs, endTimeMs, sortByTime, callback);
            worker.doWork();
            this.results = worker.getResults();
        } else {


            LogFileOps.FilterCallback myFilter = new LogFileOps.FilterCallback() {
                Set<String> scanned = new HashSet<String>();
                @Override
                public boolean accept(LogFile logFile) {
                    if (scanned.contains(logFile.getFileName())) return false;
                    else scanned.add(logFile.getFileName());
                    return callback.accept(logFile);
                }
            };

            // break it down into 20 time series files so we scan the newest stuff first
            long interval = (endTimeMs - startTimeMs)/20l;
            for (long i = endTimeMs ; i > startTimeMs ; i-=interval) {
                ListLogFiles worker = new ListLogFiles(store, fileFiltersAndTags, limit, i, i + interval -1, sortByTime, myFilter);
                worker.doWork();
                this.results.addAll(worker.getResults());
            }
        }
    }
}
