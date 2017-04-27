package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.summaryindex.PersistingSummaryIndex;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.vso.SpaceService;
import javolution.util.FastMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Seconds;

import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.*;

public class HistoAggEventListener implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(HistoAggEventListener.class);


    private final String subscriber;
    private final HistoAggSummaryFeed summaryFeed;
    HistoManager histoManager = new HistoManager();
    private List<Map<String, Bucket>> histo;
    private List<Map<String, Bucket>> busyHisto;

    private FastMap<String, ScanStatus> status = new FastMap<String, ScanStatus>();
    private FastMap<String, Integer> finished = new FastMap<String, Integer>();
    public long lastStatusSize = 0;
    private List<String> lastSentHisto = new ArrayList<String>();
    private final LogRequest request;
    AtomicInteger handledSize = new AtomicInteger(0);
    long lastTime = 0;
    private LogReplayHandler remoteEventHandler;
    boolean cancelled = false;

    int statusCallCount;
    int flushedCount;
    boolean isCompleteSent;
    int errorCount;

    private long startTime;
    private final String providerId;
    private long lastBucketHandledTime = 0;
    private long lastStatusSendTime = 0;



    AtomicBoolean isBusySending = new AtomicBoolean(false);


    private final String handlerId;


    public String listenerId;
    public ScheduledFuture<?> future;
    PersistingSummaryIndex indexMap;

    public HistoAggEventListener(String providerId, String subscriber, LogRequest request, LogReplayHandler replayHandler, String handlerId, boolean isAggSpace) {
        status.shared();
        finished.shared();

        this.providerId = providerId;
        this.subscriber = subscriber;
        this.request = request;
        this.remoteEventHandler = replayHandler;
        this.summaryFeed = new HistoAggSummaryFeed(subscriber, replayHandler);
        this.handlerId = handlerId;
        this.histo = histoManager.newHistogram(new DateTime(request.getStartTimeMs()), new DateTime(request.getEndTimeMs()), request.getBucketCount(), request.copy().queries(), subscriber, null, "sourceURI");
        this.busyHisto = histoManager.newHistogram(new DateTime(request.getStartTimeMs()), new DateTime(request.getEndTimeMs()), request.getBucketCount(), request.copy().queries(), subscriber, null, "sourceURI");
        for (int i = 0; i < histo.size(); i++) {
            lastSentHisto.add("");
        }
        startTime = DateTimeUtils.currentTimeMillis();
        if (!isAggSpace && PersistingSummaryIndex.isWrite(request.query(0).sourceQuery())) {
            indexMap = new PersistingSummaryIndex(LogProperties.getDBRootForSummaryIndex());
        }
    }

    /**
     * Aggregate/rollup
     * @param event
     * @return
     */
    volatile int queued = 0;
    public int handle(Bucket event) {
        if (errorCount > 10 || cancelled || isCompleteSent || remoteEventHandler == null || request.isCancelled() || request.isExpired()) {
            System.out.println("BAIL Can:" + request.isCancelled() + " Exp:" + request.isExpired() + " Error:" + errorCount + " cancelled:" + cancelled + " isComplete:" + isCompleteSent + " Remote:" + remoteEventHandler);
            clearAllState();
            return 0;
        }
        this.lastBucketHandledTime = DateTimeUtils.currentTimeMillis();
        if (isBusySending.get()) {
            queued++;
            histoManager.handle(busyHisto, event, request, handledSize.incrementAndGet());
            return queued;
        }
        histoManager.handle(histo, event, request, handledSize.incrementAndGet());
        if (indexMap != null) indexMap.write(request.cacheKey(), event);

        return 0;
    }


    public void flush(){
        lastTime = 0;
        summaryFeed.flush();
        sendStatusIfNeeded();
        sendHisto();
        sendCompleteMessageIfExpired();
        if (indexMap != null) indexMap.flush(request.cacheKey());
    }
    public void run(){
        try {
            sendCompleteMessageIfExpired();
            if (errorCount > 10 || cancelled || isCompleteSent || remoteEventHandler == null || request.isCancelled() || request.isExpired()) {
                clearAllState();
                return;
            }
            if (isBusySending.get() == true) {
                return;
            }
            isBusySending.set(true);

            sendStatusIfNeeded();
            sendHisto();

        } finally {
            isBusySending.set(false);
        }
    }
    public void sendHisto(){
        try {
            if (histo.size() == 0) return;


            long now = DateTimeUtils.currentTimeMillis();

            boolean readyToFireNow = isReadyToFireNow(handledSize.get(), now, lastTime);
            if (readyToFireNow) {
                lastTime = now;
                flushedCount++;
                if (request.isVerbose()) LOGGER.info(handledSize + " 1111 >>>> Sending to Handler:" + handlerId + " size:" + handledSize.get());
                histoManager.updateFunctionResults(histo, request.isVerbose());

                HashMap<String, Object> params = new HashMap<String, Object>();
                params.put("histo", histo);

                if (request.isVerbose()) LOGGER.info(String.format(" >>>> Sending SEARCH[%s] count[%d] HistoPart ", request.subscriber(), histo.size()));

                // PUSH aggd data to the Dashboard for collection
                remoteEventHandler.handle(providerId, subscriber, handledSize.get(), params);
                handledSize.set(0);
                // reset the data that was sent so we dont double count
                histoManager.resetHisto(histo);

                if (request.isVerbose())
                    LOGGER.info(handledSize + " 2222 <<<< Sending to Handler:" + handlerId);

                if (queued > 0) {
                    queued = 0;
                    histo = busyHisto;
                    this.busyHisto = histoManager.newHistogram(new DateTime(request.getStartTimeMs()), new DateTime(request.getEndTimeMs()), request.getBucketCount(), request.copy().queries(), subscriber, null, "sourceURI");
                }


            }
        } catch (Throwable t){
            errorCount++;
            LOGGER.warn("Update to handler failed:" + t.getMessage() + " ErrorCount:" + errorCount, t);
            if (errorCount > 10 || t.getMessage().contains("RetryInvocationException: SendFailed.Throwable:noSender")) {
                // kill it all
                clearAllState();
                request.cancel();
                remoteEventHandler = null;
            }
        } finally {
        }
    }
    public boolean isVerbose() {
        return request.isVerbose();
    }
    public void writeSummary(Bucket bucket) {

        try {
            summaryFeed.write(bucket);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    long livePauseBeforeComplete = LogProperties.getLivePauseBeforeComplete();
    public long sendStatusIfNeeded() {

        if (status.size() == 0) return 0;

        statusCallCount++;
        long totalScanned = 0;
        int percentScanned = 0;

        for (ScanStatus value : status.values()) {
            totalScanned += value.scanAmount;
            percentScanned += value.fileScanPercent;
        }
        // Get the average
        percentScanned = percentScanned/status.size();

        if (isCompleteSent) return -1;

        HashSet<String> finishedKeys = new HashSet<String>(finished.keySet());
        HashSet<String> workingStatusKeys = new HashSet<String>(status.keySet());
        workingStatusKeys.removeAll(finishedKeys);

        int pcComplete = this.getPercentComplete();

        // try figure out if we are really complete
        if (workingStatusKeys.size() == 0) {
            //	if (flushedCount == 0 && statusCallCount > 10 && lastStatusSize == total && finished.size() > 0) {
            if (request.isVerbose()) {
                String msg = String.format("Stage 1 rSent:%b rRec:%b %s finishSize:%d", isTimeRecent(lastBucketHandledTime),isTimeRecent(lastStatusSendTime),
                        new DateTime(lastBucketHandledTime).toString(), finishedKeys.size() );
                LOGGER.info(msg);
//				if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
            }

            if (pcComplete == 100) {
                //System.out.println("Complete:" + pcComplete);

                summaryFeed.flush();

                long elapsedTime = System.currentTimeMillis() - request.getSubmittedTime();
                // if we havent received any buckets for a while && no status updates... then we must be finished?
                if (subscriber.contains("-LIVE-") && elapsedTime > livePauseBeforeComplete || !isTimeRecent(lastBucketHandledTime) && !isTimeRecent(lastStatusSendTime)) {

                    if (remoteEventHandler != null) {
                        String msg = "Stage 2 - Complete Subscriber[" + subscriber + "] Finished:" + status.size() + "/" + finished.size() + " %Comp:" + percentScanned + " Total:" + totalScanned;
                        LOGGER.info(msg);
                        if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
                        remoteEventHandler.status(providerId, subscriber, "Search Complete:" + finished.size() + " Total:" + totalScanned);
                    }
                    //System.out.println("GOT COMPLETE!!!!!");
                    isCompleteSent = true;
                    // dont expire cause sometimes one will be really late!
                    return -1;
                } else {
                    // log some info to see why we arent flushing the data
                    String msg = String.format("Nearly There, Status:%s Percent:%d lastBucket:%b lastStatus:%b whenlastS:%s", subscriber, pcComplete, isTimeRecent(lastBucketHandledTime), isTimeRecent(lastStatusSendTime), new DateTime(lastStatusSendTime));
                    if (LOGGER.isDebugEnabled()) LOGGER.debug(msg);
                }
            }

        }

        if (totalScanned != lastStatusSize) {
            if (remoteEventHandler != null && totalScanned > 0) {
                LOGGER.info("LOGGER Status:" + subscriber + " total:" +  totalScanned + " resources:" + this.status.size() + " done:" + this.finished.size() + " pc:" + pcComplete + " searchers:" + status);

                remoteEventHandler.status(providerId, subscriber, percentScanned + "%" +  " Events: " + NumberFormat.getInstance().format(totalScanned));
            }
            lastStatusSize = totalScanned;
        } else {
            return -1;
        }
        return totalScanned;

    }

    private void sendCompleteMessageIfExpired() {
        if (errorCount < 10 && request.isCancelled() || request.isExpired()) {
            if (remoteEventHandler != null) {
                remoteEventHandler.status(providerId, subscriber, "Expired, extend to 10 minutes using 'ttl(10)': " + finished.size());
            }
            clearAllState();
        }
    }

    public boolean isTimeRecent(long time) {
        int delaySecs = LogProperties.getSearchCompleteDelaySecs();
        return Seconds.secondsBetween(new DateTime(time), new DateTime(DateTimeUtils.currentTimeMillis())).getSeconds() < delaySecs;
    }


    boolean isReadyToFireNow(int size, long now, long lastTime) {
        return size > 0 && now - lastTime > LogProperties.getHistoUpdateIntervalThreshold();
    }

    public Integer age(long now) {
        return  Long.valueOf((now - startTime)/1000l).intValue();
    }
    public void cancel(SpaceService bucketSpaceService) {
        if (this.cancelled) return;
        if (bucketSpaceService != null) bucketSpaceService.unregisterListener(listenerId);
        if (!request.isCancelled() && finished.size() == 0) LOGGER.info("LOGGER Cancelled:" + this.subscriber);
        clearAllState();
    }

    private void clearAllState() {

        //System.out.println("ClearAllStart");
        //Thread.dumpStack();
        this.cancelled = true;

        if (this.future != null) {
            future.cancel(false);
            future = null;
        }

        this.summaryFeed.cancel();
        this.histo.clear();
        this.busyHisto.clear();
        this.isCompleteSent = true;
        this.finished.clear();
        this.lastSentHisto.clear();
        this.status.clear();
        this.request.cancel();

    }

    public List<Map<String, Bucket>> getHisto() {
        return histo;
    }

    public void status(String resourceId, long scanAmount, int fileScanPercentComplete) {
        if (scanAmount == -1) {
            if (isVerbose()) {
                String msg = this.subscriber + " SearcherComplete:" + resourceId;
                LOGGER.info(msg);
                if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
            }
            finished.put(resourceId,-1);
        } else {
            if (!status.containsKey(resourceId)) {
                if (isVerbose()) {
                    String msg = this.subscriber + " SearcherStarted:" + resourceId;
                    LOGGER.info(msg);
                    if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
                }
                status.put(resourceId, new ScanStatus());
            }
            status.get(resourceId).update(resourceId, scanAmount, fileScanPercentComplete);
        }
        if (isVerbose()) {
            String msg = resourceId + " sub:" + subscriber + " Events: " + scanAmount;
            LOGGER.info(msg);
            if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
        }

        lastStatusSendTime = DateTimeUtils.currentTimeMillis();
    }

    public void close() {
        this.flush();
        this.cancel(null);
    }

    public boolean isExpired() {
        return request.isExpired();
    }

    public LogReplayHandler getReplayHandler() {
        return remoteEventHandler;
    }


    private static class ScanStatus {
        public long scanAmount;
        public int fileScanPercent;
        private String resourceId;
        public void update(String resourceId, long scanAmount, int fileScanPercent) {
            this.resourceId = resourceId;
            this.scanAmount = scanAmount;
            this.fileScanPercent = fileScanPercent;
        }
        public String toString() {
            return "Id:" + resourceId + " Events:" + scanAmount + " FilePC:" + fileScanPercent;
        }
    }

    public boolean isCompleteSent() {
        // need to allow 10 seconds past the expire time so any flushing is allowed to pass through.
        return remoteEventHandler == null || DateTimeUtils.currentTimeMillis() > this.request.expiresAt() + 10 * 1000 || request.isCancelled();
    }

    public int getPercentComplete() {
        double done = this.finished.size();
        double all = this.status.size();

        int i = (int) ((done/all) * 100);
        if (i > 100) i = 100;
        return i;
    }

    public boolean isRunnable() {
        return !isBusySending.get() && !cancelled && !request.isCancelled() && !request.isExpired() ;
    }
    public long getStartTime() {
        return startTime;
    }

    public LogRequest getRequest() {
        return request;
    }
}
