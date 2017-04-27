package com.liquidlabs.log.search.handlers;

import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 17/03/2015
 * Time: 08:37
 * To change this template use File | Settings | File Templates.
 */
public class Handlers {

    public static LogScanningHandler getSearchHandlers(LogFile logFile, LogRequest request, AggSpace aggSpace) {

        LogScanningHandler summaryHandler = request.isSummaryRequired() ? getSummaryBucketHandler(logFile, request, aggSpace) : null;
        ReplayEventsHandler replayHandler = request.isReplayRequired() ? new ReplayEventsHandler(summaryHandler, request, aggSpace, request.subscriber(), logFile) : null;
        return new SearchHistogramHandler(replayHandler, logFile, request);
    }

    private static LogScanningHandler getSummaryBucketHandler(LogFile logFile, LogRequest request, AggSpace aggSpace) {
        //return new SummaryBucketHandler(null, request, aggSpace, logFile);
        return new SummaryBucketCachedHandler(null, request, aggSpace, logFile);
    }
}
