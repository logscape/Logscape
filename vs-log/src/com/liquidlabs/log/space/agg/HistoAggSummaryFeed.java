package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.functions.CountFaster;
import com.liquidlabs.log.space.LogReplayHandler;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Producer consumer type stuff
 */
public class HistoAggSummaryFeed implements  Runnable {

    private final static Logger LOGGER = Logger.getLogger(HistoAggSummaryFeed.class);

    private final String subscriber;
    private final LogReplayHandler remoteHandler;
    private final ScheduledFuture<?> future;
    volatile boolean cancelled = false;
    HistoManager histoManager = new HistoManager();

    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");

    LinkedBlockingQueue<Bucket> summaryBuckets = new LinkedBlockingQueue<Bucket>(Integer.getInteger("agg.summary.queue.size", 5000));

    public HistoAggSummaryFeed(String subscriber, LogReplayHandler remoteHandler){
        this.subscriber = subscriber;
        this.remoteHandler = remoteHandler;
        this.future = scheduler.scheduleWithFixedDelay(this, 1000, Integer.getInteger("replay.queue.pump", 500), TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        flushSummaryBucketsLoop();
    }

    public void cancel() {
        //System.out.println(new DateTime() + " AGG-CANCELLED!" + subscriber);
        //Thread.dumpStack();
        if (!this.future.isCancelled()) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("CANCEL:" + this.subscriber);
            this.future.cancel(false);
            this.cancelled = true;
            this.summaryBuckets.clear();
        }
    }
    public void write(Bucket bucket) throws InterruptedException {
        if (cancelled) {
            //System.out.println("OOOPs - got summary data");
            return;
        }
//        System.out.println(new DateTime() + " AGG-IN:" +  bucket.getAggregateResult(new CountFaster("_agent", "_agent").toStringId(), false) + " Cancelled:" + cancelled);
        summaryBuckets.put(bucket);
    }
    synchronized private void flushSummaryBucketsLoop() {
        try {
                Bucket consumedMaster = summaryBuckets.poll(100, TimeUnit.MILLISECONDS);
                if (consumedMaster != null) {


                    Bucket taken = null;
                    while ((taken = summaryBuckets.poll(10, TimeUnit.MILLISECONDS)) != null) {
                        histoManager.handle(consumedMaster, taken, 1, false, false);
                    }
                    consumedMaster.convertFuncResults(false);
                    //System.out.println(new DateTime() + " SUM-SEND:" + consumedMaster.getAggregateResult(new CountFaster("_agent", "_agent").toStringId(), false));

                    consumedMaster.getAggregateResults().clear();
                    remoteHandler.handleSummary(consumedMaster);
                }
        } catch (Exception e) {
            LOGGER.warn("Failed To Send:" + e, e);
        }
    }

    public void flush() {
        flushSummaryBucketsLoop();
    }
}
