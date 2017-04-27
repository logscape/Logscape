package com.liquidlabs.log.search.handlers;

import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.HistoEvent;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.space.LogRequest;

public class SearchHistogramHandler implements LogScanningHandler {


    private final int hostHash;
    private final String filenameOnly;
    private final String filenameCanonical;
    private final boolean verbose;
    private final LogScanningHandler next;
    private LogFile logFile;
    private LogRequest request;
    private HistoEvent histo = null;

    public SearchHistogramHandler(LogScanningHandler next, LogFile logFile, LogRequest request) {
        this.next = next;
        this.logFile = logFile;
        this.request = request;
        this.hostHash = MurmurHash3.hashString(logFile.getFileHost(NetworkUtils.getHostname()), 12);
        this.filenameOnly =logFile.getFileNameOnly();
        this.filenameCanonical = logFile.getFileName();
        this.verbose = request.isVerbose();
    }

    public void handle(FieldSet fieldSet, String[] fields, Query query, HistoEvent histo, long bucketTime,
                       int lineNumber, long fileStartTime, long fileEndTime, long eventTime, String lineData, MatchResult matchResult, long requestStartTimeMs, long requestEndTimeMs, long bucketWidth) {


        try {
            if (histo != null) {
                this.histo = histo;
                // bail on hit limit is handled in the control loop - FileScanner
                histo.add(fieldSet, fields, lineData, eventTime, filenameOnly, filenameCanonical, lineNumber, verbose, fileStartTime, fileEndTime, matchResult, requestStartTimeMs, requestEndTimeMs);

                // histo-bucket-time - increment hitlimit
                query.increment(this.hostHash + histo.getBucketTime(bucketTime));
            }

        } finally {
            if (next != null) next.handle(fieldSet, fields, query, histo, bucketTime, lineNumber, fileStartTime, fileEndTime, eventTime, lineData, matchResult, requestStartTimeMs, requestEndTimeMs, bucketWidth);
        }
    }

    @Override
    public void flush() {
        if (this.histo != null) {
            this.histo.writeBucket(false);
            this.histo = null;
        }
        if (next != null) next.flush();
    }

    @Override
    public LogScanningHandler next() {
        return next;
    }

    public String toString() {
        return getClass().getSimpleName();
    }
    @Override
    public LogScanningHandler copy() {
        LogScanningHandler nnn = next != null ? next.copy() : null;
        return new SearchHistogramHandler(nnn, logFile, request);
    }
}
