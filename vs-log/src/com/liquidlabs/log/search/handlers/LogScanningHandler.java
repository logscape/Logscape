package com.liquidlabs.log.search.handlers;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.HistoEvent;
import com.liquidlabs.log.search.Query;


/**
 * Note - each impl used chaining to create a pipeline on siblings
 */
public interface LogScanningHandler {

    /**
     *
     *
     * @param fieldSet
     * @param query
     * @param histo
     * @param bucketTime
     * @param lineNumber
     * @param fileStartTime
     * @param fileEndTime
     * @param eventTime
     * @param lineData TODO
     * @param matchResult TODO
     * @param requestStartTimeMs TODO
     * @param requestEndTimeMs TODO
     * @param bucketWidth
     * @return true to keep going and false to stop
     */
    void handle(FieldSet fieldSet, String[] fields,
                Query query, HistoEvent histo, long bucketTime,
                int lineNumber, long fileStartTime, long fileEndTime, long eventTime, String lineData, MatchResult matchResult, long requestStartTimeMs, long requestEndTimeMs, long bucketWidth);

    void flush();

    LogScanningHandler copy();
    LogScanningHandler next();

}
