package com.liquidlabs.log.space;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.agg.HistoAggEventListener;
import com.liquidlabs.log.space.agg.ReplayAggFactory;
import com.liquidlabs.log.space.agg.ReplayAggregator;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.Notifier;
import com.liquidlabs.vso.SpaceService;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import javolution.util.FastMap;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AggSpaceImpl implements AggSpace {

    private static final Logger LOGGER = Logger.getLogger(AggSpaceImpl.class);
    int listenerLease = LogProperties.getAggLeaseSubscriberTimeout();

    final SpaceService bucketSpaceService;
    final SpaceService replaySpaceService;
    final SpaceService logEventService;
    FastMap<String, String> cachedStringReferences = new FastMap<String, String>().shared();

    boolean verbose = false;
    AtomicInteger buckets = new AtomicInteger(0);
    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool(LogProperties.getAggSchedulerSize(), new NamingThreadFactory("agg-pool"));

    Map<String, HistoAggEventListener> histoListeners = new ConcurrentHashMap<String, HistoAggEventListener>();
    Map<String, ReplayAggregator> replayListeners = new ConcurrentHashMap<String, ReplayAggregator>();
    private final String providerId;
    State state = State.RUNNING;


    public AggSpaceImpl(String providerId, final SpaceService bucketSpaceService, final SpaceService replaySpaceService, SpaceService logEventSpaceService, ScheduledExecutorService Xscheduler) {
        this.providerId = providerId;
        this.bucketSpaceService = bucketSpaceService;
        this.replaySpaceService = replaySpaceService;
        this.logEventService = logEventSpaceService;
        cachedStringReferences.shared();


        this.scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    Set<String> keySet = new HashSet<String>(histoListeners.keySet());
                    for (String key : keySet) {
                        final HistoAggEventListener eventListener = histoListeners.get(key);
                        if (eventListener == null) {
                            LOGGER.warn("No event Listener for:" + key);
                            continue;
                        }
                        if (eventListener.isCompleteSent()) {
                            eventListener.run();
                            histoListeners.remove(key);
                            eventListener.cancel(bucketSpaceService);
                        }
                        else if (eventListener.isRunnable()) {
                            scheduler.execute(new Runnable() {
                                public void run() {
                                    eventListener.run();
                                };
                            });
                        } else if (eventListener.isExpired()) {
                            cancel(key);
                        }
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    LOGGER.warn("Failed to run Scheduled Thread:" + t.toString(), t);
                }
            }

        }, 1000, LogProperties.aggSpaceHistoFlushMillis(), TimeUnit.MILLISECONDS);

        this.scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {

                try {
                    Set<String> keySet = new HashSet<String>(replayListeners.keySet());
                    for (String key : keySet) {
                        final ReplayAggregator eventListener = replayListeners.get(key);

                        if (eventListener != null && eventListener.isExpired()) {
                            cancel(key);
                        }
                    }
                } catch (Throwable t) {
                    LOGGER.error(t.toString(), t);
                }
            }

        }, 1000, LogProperties.aggSpaceEventFlushMilliseconds(), TimeUnit.MILLISECONDS);

        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                if (cachedStringReferences.size() > 100 * 1000) {
                    cachedStringReferences.clear();
                }
            }

        }, 5, 5, TimeUnit.MINUTES);

        scheduler.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                cachedStringReferences.clear();
            }

        }, 30, 30, TimeUnit.MINUTES);
    }

    public void start() {
        bucketSpaceService.start(this,"vs-log-1.0");
        replaySpaceService.start(this,"vs-log-1.0");
        logEventService.start(this, "vs-log-1.0");
    }

    public void stop() {
        LOGGER.info("LS_EVENT:Stop AggSpace");
        state = State.STOPPED;
        histoListeners.clear();
        this.replayListeners.clear();
        bucketSpaceService.stop();
        replaySpaceService.stop();
        logEventService.stop();
    }
    long count = 0;

    public void search(final LogRequest request, String handlerId, final LogReplayHandler replayHandler) throws Exception {

        if (this.state != State.RUNNING) return;
        LOGGER.info("LOGGER - REGISTER SEARCH:" + handlerId + " req:" + request.subscriber());

        this.verbose = request.isVerbose();
        if (this.verbose) verboseCount = 0;

        buckets.set(0);

        final HistoAggEventListener histoAggEventListener = new HistoAggEventListener(providerId, request.subscriber(), request, replayHandler, handlerId, true);
        this.histoListeners.put(request.subscriber(), histoAggEventListener);
        String id = handlerId + count++;

        histoAggEventListener.listenerId = id;



        final ReplayAggregator replayAgg = ReplayAggFactory.get(request, replayHandler, handlerId);
        this.replayListeners.put(request.subscriber(), replayAgg);

    }

    public int status(String subscriber, String resourceId, long amount, int fileScanPercentComplete) {

        //System.out.println("AggSpace.status:" + resourceId + " amount:" + amount + ":" + fileScanPercentComplete);

        if (amount == -1) {
            ReplayAggregator replayAggregator = this.replayListeners.get(subscriber);
            if (replayAggregator != null) {
                replayAggregator.flush();
            }
        }


        HistoAggEventListener histListener = this.histoListeners.get(subscriber);
        if (histListener != null) histListener.status(resourceId, amount, fileScanPercentComplete);
        return 1;
    }


    protected boolean isReplayCancelled(String subscriber) {
        return !replayListeners.containsKey(subscriber);
    }

    public void replay(final LogRequest request, String handlerId, final LogReplayHandler replayHandler) {

        if (this.state != State.RUNNING) return;
        this.verbose = request.isVerbose();

        LOGGER.info("LOGGER - REGISTER REPLAY:" + handlerId + " sub:" + request.subscriber() + " reqId:" + request.subscriber());
        final ReplayAggregator replayAgg = ReplayAggFactory.get(request, replayHandler, handlerId);
        this.replayListeners.put(request.subscriber(), replayAgg);
    }

    public int write(LogEvent logEvent) {
        logEventService.store(logEvent, 1);
        return logEventService.size();
    }

    public void registerEventListener(final LogEventListener eventListener, final String listenerId, String filter, final int leasePeriod) throws Exception {
        LOGGER.debug("RegisteringListener:" + listenerId);
        logEventService.registerListener(LogEvent.class, filter, new Notifier<LogEvent>() {
            public void notify(Type event, LogEvent result) {
                eventListener.handle(result);
            }}, listenerId, leasePeriod, new Event.Type[] { Type.WRITE });
    }

    public void unregisterEventListener(String listenerId) {
        logEventService.unregisterListener(listenerId);
    }

    int verboseCount;
    public int write(ReplayEvent replayEvent, boolean cacheable, String requestHash, int logId, long timeSecs) {
        if (isReplayCancelled(replayEvent.subscriber())) {
            if (verbose) LOGGER.info("RequestCancelled - Dropping Event:" + replayEvent.subscriber());
            return 0;
        }
        if (LogProperties.isTestDebugMode() && verboseCount++ < 5000) {
            String msg = String.format(" >ReplayEvent:[%s:%d]", replayEvent.getFilePath(), replayEvent.getLineNumber());
            if (LogProperties.isTestDebugMode()) LogProperties.logTest(getClass(), msg);
        }
        replayEvent.cacheValues(cachedStringReferences);

        ReplayAggregator listener = this.replayListeners.get(replayEvent.subscriber());
        if (listener != null)  {
            listener.handle(replayEvent);
        }
        else LOGGER.warn("Null Listener:" + replayEvent.subscriber());
        return 1;
    }
    public void writeReplays(List<ReplayEvent> replayEvents) {
        //System.out.println(new DateTime() + "AggSpace.write(REPLAYS):" + replayEvents.size());
        for (ReplayEvent replayEvent2 : replayEvents) {
            write(replayEvent2, false, "", 0, 0);
        }
    }


    public int write(Bucket bucket, boolean cacheable, String requestHash, int logId, long eventTime) {
        //System.out.println(new DateTime() + "AggSpace.write(BUCKET)");
        buckets.getAndIncrement();
        if (isSearchCancelled(bucket.subscriber())) {
            if (this.verbose) LOGGER.info(String.format(" Was Cancelled >Bucket:%s ", bucket));
            return 0;
        }

        if (this.verbose) LOGGER.info(String.format(" >Bucket:%s ", bucket));

        int result = 0;

        // Store for 1 second and allow the client to throttle off based upon space size;
        HistoAggEventListener listener = this.histoListeners.get(bucket.subscriber());
        if (listener != null) {
            bucket.cache(cachedStringReferences);
            result = listener.handle(bucket);
        }
        return result;
    }

    public int write(List<Bucket> bucket) {
        //System.out.println("AggSpace.write(Bucket-s)");
        int result = 0;
        for (Bucket bucket2 : bucket) {
            result = write(bucket2, false, "", 0, 0);
        }
        return result;
    }

    public void msg(String id, String string) {
    }

    public void writeSummary(Bucket bucket, boolean cacheable, String requestHash, int logId, long eventTime) {
        //System.out.println(new DateTime() + "AggSpace.write(Summary)");
        if (isSearchCancelled(bucket.subscriber())) return;
        HistoAggEventListener listener = this.histoListeners.get(bucket.subscriber());
        listener.writeSummary(bucket);
    }

    @Override
    public void flush(String subscriber, boolean finished) {

    }

    private boolean isSearchCancelled(String subscriber) {
        if (subscriber == null) return true;
        return !this.histoListeners.containsKey(subscriber);
    }

    public void cancel(final String subscriberId) {
        HistoAggEventListener aggListener = this.histoListeners.remove(subscriberId);
        if (aggListener != null) {
            aggListener.cancel(bucketSpaceService);
            LOGGER.info("LOGGER S Cancelled:" + subscriberId);
            if (aggListener.getReplayHandler() != null) bucketSpaceService.unregisterListener(aggListener.getReplayHandler().getId());
        }
        ReplayAggregator replayListener = this.replayListeners.remove(subscriberId);
        if (replayListener != null) {
            replayListener.cancel();
            if (replayListener.getReplayHandler() != null) bucketSpaceService.unregisterListener(replayListener.getReplayHandler().getId());
        }
        LOGGER.info("Unregistering:" + subscriberId);
        bucketSpaceService.unregisterListener(subscriberId);

    }


    public int size() {
        return bucketSpaceService.findObjects(LogEvent.class, null, false, Integer.MAX_VALUE).size();
    }


    private static int MY_PORT = VSOProperties.getPort(VSOProperties.ports.AGGSPACE);

    public static void main(String[] args) {
        try {


            JmxHtmlServerImpl jmxServer = new JmxHtmlServerImpl(VSOProperties.getJMXPort(VSOProperties.ports.AGGSPACE), true);
            jmxServer.start();
            LOGGER.info("Starting JMX Port:" + jmxServer.getURL());

            String lookupSpaceURI = VSOProperties.getLookupAddress();

            ORMapperFactory mapperFactory = new ORMapperFactory(MY_PORT, AggSpace.NAME, 20 * 1024, MY_PORT);

            LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupSpaceURI, mapperFactory.getProxyFactory(),"AggSpaceBoot-" + PIDGetter.getPID() + "-" + NetworkUtils.getHostname());

            LOGGER.info("Using:" + lookupSpace);

            SpaceServiceImpl bucketSpaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, AggSpace.NAME, mapperFactory.getScheduler(), false, false, false);
            SpaceServiceImpl replaySpaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, AggSpace.NAME_REPLAY, mapperFactory.getScheduler(), false, false, false);
            SpaceServiceImpl eventSpaceService = new SpaceServiceImpl(lookupSpace, mapperFactory, LogSpace.NAME_REPLAY, mapperFactory.getScheduler(), false, false, true);

            LOGGER.info("Starting AggSpaceImpl with Proxy:" + mapperFactory.getProxyFactory().getEndPoint());
            final AggSpaceImpl aggSpace = new AggSpaceImpl(mapperFactory.getProxyFactory().getEndPoint(), bucketSpaceService, replaySpaceService, eventSpaceService, mapperFactory.getScheduler());
            aggSpace.start();

            new ResourceProfile().scheduleOsStatsLogging(mapperFactory.getScheduler(), AggSpace.class, LOGGER);

            LOGGER.info("Started");
        } catch (Throwable e) {
            LOGGER.error("Failed to start AggSpace", e);
        }

    }

    static String getArg(String key, String[] args, String defaultResult) {
        for (String arg : args) {
            if (arg.startsWith(key)) return arg.replace(key, "");
        }
        if (defaultResult != null) return defaultResult;
        throw new RuntimeException("Argument:" + key + " was not found in:" + Arrays.toString(args));
    }

    public static AggSpace getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {
        AggSpace remoteService = SpaceServiceImpl.getRemoteService(whoAmI, AggSpace.class, lookupSpace, proxyFactory, AggSpace.NAME, false, false);
        LOGGER.info(String.format("%s Getting AggSpace: %s", whoAmI, remoteService));
        return remoteService;
    }
}
