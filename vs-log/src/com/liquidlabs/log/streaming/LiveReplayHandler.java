/**
 *
 */
package com.liquidlabs.log.streaming;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;

public class LiveReplayHandler implements LiveHandler {
    static final Logger LOGGER = Logger.getLogger(LiveReplayHandler.class);
    public LogRequest request;
    AggSpace aggSpace;
    private final String sourceURI;

    volatile int sentReplays = 0;
    private LiveCommon common;

    public LiveReplayHandler(Indexer indexer, LogRequest request, AggSpace aggSpace, String sourceURI) {
        this.request = request;
        this.aggSpace = aggSpace;
        this.sourceURI = sourceURI;
        if (request.isVerbose() && LOGGER.isDebugEnabled())	LOGGER.debug("Creating" + request.subscriber());
        common = new LiveCommon(request,LOGGER);
    }
    public String subscriber() {
        return request.subscriber() + "_REPLAY";
    }

    private String agentType = VSOProperties.getResourceType();

    public int handle(LogFile logfile, String path, long time, int line, String nextLine, String fileTag, FieldSet fieldSet, String[] fieldValues) {

        if (request.isVerbose() && LOGGER.isDebugEnabled()) LOGGER.debug("Handle" + request.subscriber() + " path:" + path + ":" + line);

        // Request was from an alert trigger and has small max limit - so we can obey it - remember we need to send more items than the limit as the limit is SOFT - the door is left open for other related events
        if (isReplayTriggerAlert() && sentReplays > request.getReplay().maxItems() * 2) {
            try {
                LOGGER.info("Cancelled:" + request.subscriber());
                request.cancel();
            } catch (Throwable t) {
                LOGGER.warn(t.toString(),t);
            }
            if (request.isVerbose()) LOGGER.info("Expired = Handle:" + path + ":" + line);
            return 0;
        }

        if (common.bail()) {
            try {
                if(!request.isCancelled()) {
                    LOGGER.info("Was Failed, Cancelled or Expired: Cancel:" + request.isCancelled() + " Expire:" + request.isExpired());
                    request.cancel();
                }
            } catch (Throwable t) {
                LOGGER.warn(t.toString(),t);
            }
            return 0;
        }

        final String fileNameOnly = FileUtil.getFileNameOnly(path);

        int i = 0;
        if (request.isVerbose() && LOGGER.isDebugEnabled()) LOGGER.debug("Handle: path" + path + " file:" + fileNameOnly + ":" + line);
        for (Query query : request.queries()) {
            MatchResult matchResult = query.isMatching(nextLine);

            if (matchResult.isMatch()) {

                if (query.isPassedByFilters(fieldSet, fieldValues, nextLine, matchResult, line)){
                    try {
                        ReplayEvent replayEvent = new ReplayEvent(sourceURI, line, query.getSourcePos(), query.groupId(), request.subscriber(), time, nextLine);
                        replayEvent.setDefaultFieldValues(fieldSet.getId(), logfile.getFileHost(NetworkUtils.getHostname()), fileNameOnly, path, fileTag, agentType, "", nextLine.length() + "");
                        aggSpace.write(replayEvent, false, "", 0, 0);
                        sentReplays++;
                        common.resetFailure();
                        i++;
                    } catch (Exception e) {
                        if (e.toString().contains("ReplayAborted")) {
                            request.cancel();
                        }
                        common.incrementFailure();
                        if (!request.isCancelled()) LOGGER.warn("Failed to Stream msg", e);
                    }
                }
            }
        }

        return i;

    }

    /**
     * Replay trigger alerts specify a max item count i.e. 10 etc - streaming searches need to stay open and set a max of 10K etc.
     * @return
     */
    private boolean isReplayTriggerAlert() {
        return request.getReplay().maxItems() < 1000;
    }
    public boolean isExpired() {
        return common.isExpired();
    }

    public String toString() {
        return getClass().getSimpleName() + " " + common.toString();
    }
}