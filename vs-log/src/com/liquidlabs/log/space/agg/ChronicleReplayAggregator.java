package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.TimeUID;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogRequest;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ChronicleReplayAggregator implements ReplayAggregator {

    public static final int A10_MILLION_ENTRIES = Integer.getInteger("replay.agg.events", 10 * 100 * 1000);
    private final static Logger LOGGER = Logger.getLogger(ChronicleReplayAggregator.class);
	static private ChronicleMap<TimeUID, ReplayEvent> chronoMap = ChronicleMapBuilder
            .of(TimeUID.class, ReplayEvent.class)
            .entries(A10_MILLION_ENTRIES)
            .averageValueSize(1 * 1024)
            .create();

    private LinkedBlockingQueue<TimeUID> outgoing = new LinkedBlockingQueue<TimeUID>();
	private final LogRequest request;
	private LogReplayHandler replayHandler;

	private long startTime = new DateTime().getMillis();

	private volatile boolean cancelled = false;

    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");


	public String listenerId;
    private ScheduledFuture<?> future;

    public ChronicleReplayAggregator(LogRequest request, LogReplayHandler replayHandler, String handlerId) {
		this.request = request;
		this.replayHandler = replayHandler;
		this.listenerId = request.subscriber() + "_id";

        this.future = scheduler.scheduleWithFixedDelay(this, 1, Integer.getInteger("replay.queue.pump",2), TimeUnit.MILLISECONDS);
	}

	public void handle(ReplayEvent event) {
		if (cancelled) {
			return ;
		}

        chronoMap.put(event.getId(), event);
		outgoing.add(event.getId());
	}


	public void run(){

		try {
            int chunkSize = 4 * 1024;

			ArrayList<ReplayEvent> sending = null;
            int bytes = 0;

			while (outgoing.size() > 0) {
				TimeUID item = outgoing.poll(10, TimeUnit.MILLISECONDS);
				if (cancelled) continue;
				if (item != null) {

					if (sending == null) sending = new ArrayList<ReplayEvent>();
					ReplayEvent item1 = chronoMap.remove(item);
                    bytes += item1.getRawData().length();
					sending.add(item1);
				}

				if (bytes >  chunkSize) {
					replayHandler.handle(sending);
					sending = null;
                    bytes = 0;
				}
			}
			if (sending != null && sending.size() > 0) {
				replayHandler.handle(sending);
			}
		} catch (Throwable t){
			LOGGER.warn("Update to handler failed:" + t, t);
			if (t.getMessage().contains("RetryInvocationException: SendFailed.Throwable:noSender")) {
				replayHandler = null;
			}
		} finally {
			if (isExpired() || request.isCancelled()) {
				cancel();
			}
		}
	}

	public Integer age(long now) {
		return Long.valueOf((now - startTime)/1000l).intValue();
	}

	public void cancel() {
		LOGGER.info("LOGGER Cancelled:" + request.subscriber() + " Listener:" + listenerId);
        if (this.future != null) {

            try {
                future.cancel(false);
                Thread.sleep(100);
                this.cancelled = true;
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        /**
         * Make sure CANCELLED is set - and there is no-opportunity for chronicle to be read - or it will kill the JVM
         */
        // drain the events!
        for (TimeUID timeUID : outgoing) {
            chronoMap.remove(timeUID);
        }
	}

	public boolean isExpired() {
		return replayHandler == null ||  this.request.isExpired();
	}

	@Override
	public void close() {
		cancel();
	}

	public boolean isRunnable() {
		boolean itemsQueued = outgoing.size() > 0;
		return itemsQueued && !request.isCancelled() &&  !request.isExpired() ;
	}

	public int size() {
		return outgoing.size();
	}

    @Override
    public void flush() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
	@Override
	public LogReplayHandler getReplayHandler() {
		return replayHandler;
	}
}
