package com.liquidlabs.log.search.handlers;

import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.HistoEvent;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.vso.VSOProperties;

public class ReplayEventsHandler implements LogScanningHandler {

    private final int hostHash;
    private final String hostname;
    private final String filenameOnly;
    private final String path;
    private final String tags;
    private final int logId;
    private long lastEventTimeMS;
    private final String requestHash;
    private LogScanningHandler next;
    private LogRequest request;
    private final AggSpace aggSpace;
    private final String sourceURI;
    private final String subscriber;
    private LogFile logFile;

    private final String agentType = VSOProperties.getResourceType();
    static private int maxEventsPerHost = LogProperties.getMaxEventsPerHostSource();

    public ReplayEventsHandler(LogScanningHandler next, LogRequest request, AggSpace aggSpace, String subscriber, LogFile logFile) {
        this.next = next;
        this.request = request;
        this.requestHash = MurmurHash3.hashString(request.toString(), 10) + "";
        this.aggSpace = aggSpace;
        this.sourceURI =  LogProperties.getLogHttpServerURI();
        this.subscriber = subscriber;
        this.logFile = logFile;
        this.logId = logFile.getId();
        this.hostname = logFile.getFileHost(NetworkUtils.getHostname());
        this.hostHash = MurmurHash3.hashString(hostname, 12);
        this.filenameOnly = logFile.getFileNameOnly();
        this.path = logFile.getFileName();
        this.tags = logFile.getTags();
    }

    public void handle(FieldSet fieldSet, String[] fields, Query query, HistoEvent histo, long bucketTime,
                       int lineNumber, long fileStartTime, long fileEndTime, long eventTime, String lineData, MatchResult matchResult, long requestStartTimeMs, long requestEndTimeMs, long bucketWidth) {

        try {

            query.increment(hostHash);


            if (lastEventTimeMS == 0) {
                lastEventTimeMS = eventTime;
            }
            if (lastEventTimeMS <= eventTime) {
                lastEventTimeMS = eventTime+1;
                eventTime = lastEventTimeMS;
            }
            ReplayEvent replayEvent = new ReplayEvent(sourceURI, lineNumber, query.getSourcePos(), query.groupId(), subscriber, eventTime, lineData);
            replayEvent.setDefaultFieldValues(fieldSet.getId(), hostname, filenameOnly, path, tags, agentType, sourceURI, new Integer(lineData.length()).toString());
            aggSpace.write(replayEvent, false, requestHash, logId, eventTime);
        } finally {
            if (next != null) next.handle(fieldSet, fields, query, histo, bucketTime, lineNumber, fileStartTime, fileEndTime, eventTime, lineData, matchResult, requestStartTimeMs, requestEndTimeMs, bucketWidth);
        }
    }

    @Override
    public void flush() {
        if (next != null) next.flush();
    }

    public String toString() {
        return getClass().getSimpleName();
    }
    @Override
    public LogScanningHandler copy() {
        LogScanningHandler nnn = next != null ? next.copy() : null;
        return new ReplayEventsHandler(nnn, request, aggSpace, subscriber, logFile);
    }
    @Override
    public LogScanningHandler next() {
        return next;
    }

}
