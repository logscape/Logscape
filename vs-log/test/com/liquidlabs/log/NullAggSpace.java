/**
 *
 */
package com.liquidlabs.log;

import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.summaryindex.PersistingSummaryIndex;
import com.liquidlabs.log.space.*;
import com.liquidlabs.log.space.agg.HistoAggSummaryFeed;
import com.liquidlabs.log.space.agg.HistoManager;
import org.joda.time.DateTime;

import java.util.*;

public class NullAggSpace implements AggSpace {

    public List<ReplayEvent> replayEvents = new ArrayList<ReplayEvent>();
    public Map<String,Bucket> buckets = new HashMap<String, Bucket>();
    public Map<String,Bucket> sumBuckets = new HashMap<String, Bucket>();
    public List<LogEvent> events = new ArrayList<LogEvent>();



    public void replay(LogRequest replayRequest,
                       String handlerId, LogReplayHandler replayHandler) {
    }

    public void search(LogRequest replayRequest,
                       String handlerId, LogReplayHandler replayHandler) throws Exception {
    }


    public int write(ReplayEvent replayEvent, boolean cacheable, String requestHash, int logId, long timeSecs) {
        replayEvents.add(replayEvent);
        return 0;
    }

    public void writeReplays(List<ReplayEvent> replayEvents) {
        if (replayEvents.size() > 1000) return;
        this.replayEvents.addAll(replayEvents);
    }

    public void start() {
    }

    public void stop() {
    }

    public int size() {
        return buckets.size();
    }

    PersistingSummaryIndex sumIndex = new PersistingSummaryIndex("build/SIDX");
    public int write(Bucket currentBucket, boolean cacheable, String requestHash, int logId, long eventTime) {

        System.out.println("GOT:" + currentBucket);
        synchronized (this) {
            buckets.put(currentBucket.id() ,currentBucket);
            sumIndex.write(requestHash, currentBucket);
            return buckets.size();
        }
    }

    public void cancel(String subscriberId) {
    }

    public void registerEventListener(LogEventListener eventListener, String listenerId, String filter, int leasePeriod) throws Exception {
    }

    public void unregisterEventListener(String listenerId) {
    }

    public int write(LogEvent logEvent) {
        this.events.add(logEvent);
        return this.events.size();
    }

    public int write(List<Bucket> bucket) {
        System.out.println("NullAggSpace.write(List<Bucket>) GOT:" + bucket);
        synchronized (this) {
            for (Bucket bucket1 : bucket) {
                this.write(bucket1, false, "", 0,0);
            }
        }
        return 0;
    }

    public volatile long amount = 0;
    public int status(String subscriber, String resourceId, long amount, int fileScanPercentComplete) {
        this.amount += amount;
        System.out.println("Status:" + subscriber + " res:" + resourceId + " amount:" + this.amount);
        return 1;

    }

    HistoManager histoManager = new HistoManager();
    public Bucket summaryBucket = null;
    volatile int called = 0;
    HistoAggSummaryFeed summaryFeed = null;
    @Override
    public void writeSummary(Bucket bucket, boolean cacheable, String requestHash, int logId, long eventTime) {
        if (summaryFeed == null) {
            summaryFeed = new HistoAggSummaryFeed(bucket.subscriber(), new LogReplayHandler() {
                public void handle(ReplayEvent event) {
                }

                public void handle(Bucket event) {
                }

                public void handleSummary(Bucket bucketToSend) {
                    System.out.println("Setting SUMM Bucket");
                    if (summaryBucket == null) {
                        summaryBucket = bucketToSend;
                    }
                }

                public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
                    return 1;
                }

                public int handle(List<ReplayEvent> events) {
                    return 100;
                }

                public int status(String provider, String subscriber, String msg) {
                    return 1;
                }

                public String getId() {
                    return null;
                }
            });
        }
        try {
            summaryFeed.write(bucket);
            this.summaryBucket = bucket;
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
    @Override
    public void msg(String id, String string) {
    }

    @Override
    public void flush(String subscriber, boolean finished) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        if (summaryBucket != null) System.out.println("Flushing:"+ subscriber + " Values:" + summaryBucket.getAggregateResults().get("CountFaster DYN_linesPerSec linesPerSec"));
    }

    public Bucket getSummaryBucket() {
        return summaryBucket;
    }

    public List<Bucket> buckets() {
        return new ArrayList<Bucket>(this.buckets.values());
    }

    public void reset() {
        this.sumBuckets.clear();;
        this.summaryBucket = null;
        this.buckets.clear();
        this.replayEvents.clear();
    }
    public List<Map<String, Bucket>> getAggregatedHistogram(LogRequest request) {
        List<Map<String, Bucket>> results = histoManager.newHistogram(new DateTime(request.getStartTimeMs()), new DateTime(request.getEndTimeMs()), request.getBucketCount(), request.copy().queries(), request.subscriber(), null, "sourceURI");
        Collection<Bucket> allBuckets = buckets.values();
        for (Bucket event : allBuckets) {
            histoManager.handle(results, event, request, 1);
        }
        return  results;
    }
}