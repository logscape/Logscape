package com.liquidlabs.log.search.tailer;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.WatchManager;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.SearchRunnerI;
import com.liquidlabs.log.search.summaryindex.PersistingSummaryIndex;
import com.liquidlabs.log.space.*;
import com.liquidlabs.log.space.agg.*;
import com.liquidlabs.log.streaming.StreamingRequestHandlerImpl;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 * 1) Allows single series of flushing on Histos
 * 2) Schedules List<> updates on Replay Events
 */
public class TailerEmbeddedAggSpace implements TailerAggSpace {

    static final Logger LOGGER = Logger.getLogger(TailerEmbeddedAggSpace.class);
    public static final String SUBSCRIBER = "subscriber";
    public static boolean isCreated = false;

    Map<String, LogRequest> requests = new ConcurrentHashMap<String, LogRequest>();
    Map<String, HistoAggEventListener> histoListeners = new ConcurrentHashMap<String, HistoAggEventListener>();
    Map<String, ReplayAggregator> replayPumpers = new ConcurrentHashMap<String, ReplayAggregator>();

    private ScheduledExecutorService scheduler;

    String providerId;
    private final AggSpace aggSpace;

    private SearchRunnerI searchRunner;
    private final EventMonitor eventMonitor;

    private StreamingRequestHandlerImpl liveRequestHandler;

    private final String hostname;
    private State state = State.RUNNING;


    public TailerEmbeddedAggSpace(AggSpace aggSpace, SearchRunnerI searchRunner, ScheduledExecutorService scheduler, final String hostname, EventMonitor eventMonitor) {
        isCreated = true;
        this.aggSpace = aggSpace;
        this.searchRunner = searchRunner;
        this.eventMonitor = eventMonitor;
        this.scheduler = ExecutorService.newScheduledThreadPool(2, new NamingThreadFactory("TailEmbAgg"));
        this.hostname = hostname;
    }

    public void cancel(String subscriber) {

        HistoAggEventListener histoAggPumper = this.histoListeners.remove(subscriber);
        if (histoAggPumper != null) {
            LOGGER.info("LOGGER Cancelling:" + subscriber);
            histoAggPumper.cancel(null);
        }
        LogRequest request = this.requests.remove(subscriber);

        if (request != null) {
            LOGGER.info("LOGGER Cancel Request:" + subscriber);
            request.cancel();
        }
        ReplayAggregator replayPumper = this.replayPumpers.remove(subscriber);
        if (replayPumper != null) replayPumper.cancel();
    }

    /**
     * Called when a search is complete - so similar to above - but we dont want to cancel live streaming stuff
     */
    @Override
    public void cancelRequest(final String subscriber) {
        // check if it was already finished
        if (this.histoListeners.get(subscriber) == null) return;

        flush(subscriber, true);

        LogRequest  request = TailerEmbeddedAggSpace.this.requests.get(subscriber);
        if (request == null) return;
        if (!request.isStreaming()) {
            // give it a bit before cancelling completely..
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    HistoAggEventListener histo = TailerEmbeddedAggSpace.this.histoListeners.remove(subscriber);
                    if (histo != null) histo.cancel(null);
                    ReplayAggregator events = TailerEmbeddedAggSpace.this.replayPumpers.remove(subscriber);
                    if (events != null) events.cancel();
                    LogRequest  request = TailerEmbeddedAggSpace.this.requests.remove(subscriber);
                    if (request != null && !request.isCancelled()) {
                        LOGGER.debug("LOGGER CancelInternal:" + subscriber);
                        request.cancel();
                    }
                    searchRunner.removeCompleteTasks();
                }
            }, 30, TimeUnit.SECONDS);
        }
    }

    public void replay(LogRequest request, String handlerId, LogReplayHandler replayHandler) {

        if (this.state != State.RUNNING) return;

        if(!request.isReplayRequired()) return;

        synchronized (this) {
            if (this.replayPumpers.containsKey(request.subscriber())) return;

            eventMonitor.raise(new Event("REPLAY").with(SUBSCRIBER, request.subscriber()));

            if (request.isVerbose()) {
                String msg = String.format("Starting REPLAY[%s]", request.subscriber());
                LOGGER.info(msg);
                if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
            }

            final ReplayAggregator replayPumper = ReplayAggFactory.get(request, new TailerSender(this.aggSpace), handlerId);
            this.replayPumpers.put(request.subscriber(), replayPumper);
        }

        if (request.isStreaming()) {
            liveRequestHandler.start(request);
        }

        autoCancel(request);
    }

    public void search(final LogRequest request, String handlerId, LogReplayHandler replayHandler) throws Exception {

        if (this.state != State.RUNNING) return;
        LOGGER.info(String.format("LOGGER Starting SEARCH[%s]", request.subscriber()));

        if (WatchManager.PREVENT_SEARCH){
            aggSpace.status(request.subscriber(), handlerId, -1, 100);
            System.out.println("Daily Limit 2GB is hit, SEARCHING is suspended for today");
            return;
        }

        if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), request.toString());

        eventMonitor.raise(new Event("SEARCH").with(SUBSCRIBER, request.subscriber()));

        if (PersistingSummaryIndex.isSummaryIndexAction(request)) {
            long searchFrom = new PersistingSummaryIndex(LogProperties.getDBRootForSummaryIndex()).handle(request, hostname, aggSpace);
            // now search the remaining section
            request.setStartTimeMs(searchFrom);
        }

        synchronized (this) {
            if (this.requests.containsKey(request.subscriber())) {
                LOGGER.info("ExtendingSearch:" + request);
                this.requests.put(request.subscriber(), request);
            }
            else this.requests.put(request.subscriber(), request);
        }




        replay(request, handlerId, replayHandler);



        try {
            // send a status immediately
            aggSpace.status(request.subscriber(), hostname, 0, 0);

            request.createSummaryBucket(this);

            HistoAggEventListener histoAggEventListener = new HistoAggEventListener(providerId, request.subscriber(), request, new TailerSender(this.aggSpace), handlerId, false);
            this.histoListeners.put(request.subscriber(), histoAggEventListener);


            ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(histoAggEventListener, 1, Integer.getInteger("agg.pump.ms",1), TimeUnit.SECONDS);
            histoAggEventListener.future = future;

            // not a live request - but a historical one
            if (!request.isStreaming()) {
                // retrieve existing data
                int searchCount = searchRunner.search(request.copy());
                if (searchCount == 0){
                    // flush and clean out bucket data
                    cancelRequest(request.subscriber());
                    return;
                }
            }


            autoCancel(request);
        } catch (Throwable t) {
            LOGGER.warn(String.format("Failed to start SEARCH[%s] ex:[%s]", request.subscriber(), t.toString()));
        }
    }
    private long lastAmount = 0;
    public int status(String subscriber, String resourceId, long amount, int fileScanPercentComplete) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("STATUS:"  + fileScanPercentComplete + " : " + amount);
        if (fileScanPercentComplete + amount != lastAmount) {
            try {
                this.aggSpace.status(subscriber, hostname, amount, fileScanPercentComplete);
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        lastAmount = fileScanPercentComplete + amount;
        return 1;
    }

    private void autoCancel(final LogRequest requestToCancel) {
        final String subscriber = requestToCancel.subscriber();
        long seconds = requestToCancel.getTimeToLiveMins() * 60;
        if (!requestToCancel.isSearch()) LOGGER.info("Search/Replay will AutoCancel at:" + new DateTime(requestToCancel.getLastChanceTime()) + " expires:" + new DateTime(requestToCancel.expiresAt()) + " secs:" +  seconds);
        scheduler.schedule(new Runnable() {
            public void run() {
                // the request may have been overwritten with a pan or zoom or something
                LogRequest  request = TailerEmbeddedAggSpace.this.requests.get(subscriber);
                if (request.getSubmittedTime() == requestToCancel.getSubmittedTime()) {
                    LOGGER.info("AutoCancelling:" + requestToCancel.subscriber());
                    cancelRequest(subscriber);
                }
            }
        }, seconds + 10, TimeUnit.SECONDS);
    }

    public int size() {
        return 0;
    }
    public int getCurrentSearchCount() {
        return requests.size();
    }
    public int getHistoAggsCount() {
        return this.histoListeners.size();
    }

    public int write(Bucket bucket, boolean cacheable, String requestHash, int logId, long eventTime) {
        HistoAggEventListener listener = this.histoListeners.get(bucket.subscriber());
        if (listener != null) {
            if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), "******** Write:" + bucket.id() + " s:" + bucket.totalScanned());
            listener.handle(bucket);
        }
        return 0;
    }

    public int write(ReplayEvent replayEvent, boolean cacheable, String requestHash, int logId, long timeSecs) {
        ReplayAggregator listener = this.replayPumpers.get(replayEvent.subscriber());
        if (listener == null) {
            throw new RuntimeException("ReplayAborted ReplayEvent Failed to find sub:" + replayEvent.subscriber());
        }
        listener.handle(replayEvent);
        return 0;
    }
    public void writeReplays(List<ReplayEvent> replayEvents) {
    }

    public int write(List<Bucket> bucket) {
        return 0;
    }

    public void start() {
    }

    public void stop() {
        LOGGER.info("Stopped");
        this.state = State.STOPPED;
        this.histoListeners.clear();
        this.requests.clear();
    }

    public void registerEventListener(LogEventListener eventListener, String listenerId, String filter, int leasePeriod) throws Exception {
    }
    public void unregisterEventListener(String listenerId) {
    }
    public int write(LogEvent logEvent) {
        return 0;
    }

    public void flush(String subscriber, boolean finished) {

        if (LOGGER.isInfoEnabled()) LOGGER.info("Flush:" + subscriber + " Finished:" + finished);


        // indicate that we are 'search' complete
        LogRequest request = requests.get(subscriber);
        if (request != null) {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("Flush Final:" + subscriber);
            HistoAggEventListener aggListener = this.histoListeners.get(subscriber);
            ReplayAggregator replayAggregator = replayPumpers.get(subscriber);


            if (aggListener != null) {
                if (finished) aggListener.close(); else aggListener.flush();
            }
            if (replayAggregator != null) {
                if (finished) replayAggregator.close(); else replayAggregator.close();
            }
            //System.out.println("TAILER SENDING COMPLETE!");
            try {
                eventMonitor.raise(new Event("FLUSH").with(SUBSCRIBER, subscriber).with("finished", finished));
                aggSpace.status(subscriber, hostname, -1, 100);
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

            if (request.isVerbose()) LOGGER.info(String.format("Flushed Buckets, SEARCH[%s] elapsedMs[%d] Sent:[%d]",subscriber, DateTimeUtils.currentTimeMillis() - aggListener.getStartTime(), aggListener.lastStatusSize));
        }
    }

    public void setLiveRequestHandler(StreamingRequestHandlerImpl liveRequestHandler) {
        this.liveRequestHandler = liveRequestHandler;
    }
    public void writeSummary(Bucket summaryBucket, boolean cacheable, String requestHash, int logId, long eventTime) {
        try {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("writeSummary:" + summaryBucket);

            summaryBucket.convertFuncResults(false);
            if (LOGGER.isDebugEnabled()) LOGGER.debug("writeSummary Results:" + summaryBucket.getAggregateResults().size());
            if (summaryBucket.getAggregateResults().size() > 0) {
                HistoAggEventListener listener = this.histoListeners.get(summaryBucket.subscriber());
                if (listener != null) listener.writeSummary(summaryBucket);
            }
            summaryBucket.resetResults();

        } catch (Throwable t){
            // in case we hit OOM
            t.printStackTrace();
            LOGGER.warn("Failed to sendSummary:" + t.toString(), t);
        }

    }
    public void msg(String id, String string) {
    }
    public static class TailerSender implements LogReplayHandler {
        private final AggSpace aggSpace2;

        public TailerSender(AggSpace aggSpace) {
            aggSpace2 = aggSpace;
        }

        public String getId() {
            return null;
        }

        public void handle(ReplayEvent event) {
            aggSpace2.write(event, false, "", 0, 0);
        }
        public int handle(List<ReplayEvent> events) {
            aggSpace2.writeReplays(events); return 100;
        }

        public void handle(Bucket event) {
            aggSpace2.write(event, false, "", 0, 0);
        }

        public int handle(String providerId, String subscriber, int size, Map<String, Object> params) {
            List<Map<String, Bucket>> histo = (List<Map<String, Bucket>>) params.get("histo");
            List<Bucket> bb = new ArrayList<Bucket>();
            for (Map<String,Bucket> map : histo) {
                Collection<Bucket> buckets = map.values();
                for (Bucket bucket : buckets) {
                    if (bucket.hits() > 0) bb.add(bucket);
                }
            }
            return aggSpace2.write(bb);
        }

        public int status(String provider, String subscriber, String msg) {
             return 1;
        }

        public void handleSummary(Bucket summaryBucket) {
            aggSpace2.writeSummary(summaryBucket, false, "", 0, 0);
        }
    }

}
