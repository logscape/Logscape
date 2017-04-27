package com.liquidlabs.log.indexer.krati;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.indexer.txn.krati.ListLogFiles;
import com.liquidlabs.log.indexer.txn.krati.ListLogFilesCB;
import krati.store.DataStore;

import java.util.*;

/**
 * Build a timeseries-descending list of files
 */
public class SpanningListLogFilesCB {
    private final DataStore<byte[], byte[]> store;
    private final long startTimeMs;
    private final long endTimeMs;
    private final boolean sortByTime;
    private final LogFileOps.FilterCallback callback;
    private List<LogFile> results = new ArrayList<LogFile>();

    public SpanningListLogFilesCB(DataStore<byte[], byte[]> store, long startTimeMs, long endTimeMs, boolean sortByTime, LogFileOps.FilterCallback callback) {
        this.store = store;
        this.startTimeMs = startTimeMs;
        this.endTimeMs = endTimeMs;
        this.sortByTime = sortByTime;
        this.callback = callback;
    }

    public List<LogFile> getResults() {
        if (sortByTime) {
            Collections.sort(results, new Comparator<LogFile>() {
                @Override
                public int compare(LogFile o1, LogFile o2) {
                    return Long.valueOf(o2.getEndTime()).compareTo(o1.getEndTime());
                }
            });
        }
        return results;
    }

    public void doWork() {
        if (callback == null || endTimeMs - startTimeMs < DateUtil.HOUR * 24) {
            ListLogFilesCB worker = new ListLogFilesCB(store, startTimeMs, endTimeMs, sortByTime, callback);
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
                ListLogFilesCB worker = new ListLogFilesCB(store, i, i + interval -1, sortByTime, myFilter);
                worker.doWork();
                this.results.addAll(worker.getResults());
            }
        }
    }
}
