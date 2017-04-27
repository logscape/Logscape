package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class NativeQueuedReplayAggregator implements ReplayAggregator {

	private final static Logger LOGGER = Logger.getLogger(NativeQueuedReplayAggregator.class);

	private LinkedBlockingQueue<ReplayEvent> outgoing = new LinkedBlockingQueue<ReplayEvent>(Integer.getInteger("queued.agg.limit", 10 * 100 * 1000));
	private final LogRequest request;
	private LogReplayHandler replayHandler;
	
	private long startTime;

	private boolean cancelled = false;

	private final String handlerId;
	AtomicInteger received = new AtomicInteger();
	AtomicInteger sent = new AtomicInteger();
    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");


	public String listenerId;
    private ScheduledFuture<?> future;

    public NativeQueuedReplayAggregator(LogRequest request, LogReplayHandler replayHandler, String handlerId) {
		this.request = request;
		this.replayHandler = replayHandler;
		this.handlerId = handlerId;
		startTime = DateTimeUtils.currentTimeMillis();
		this.listenerId = request.subscriber() + "_id";

        this.future = scheduler.scheduleWithFixedDelay(this, 5, Integer.getInteger("replay.queue.pump",2), TimeUnit.MILLISECONDS);
    }

	public void handle(ReplayEvent event) {
		if (cancelled) {
			return;
		}
		received.incrementAndGet();
		outgoing.add(event);
	}


	public void run(){
        try {
            int chunkSize = 4 * 1024;
            int bytes = 0;
            ArrayList<ReplayEvent> sending = null;


            while (outgoing.size() > 0) {
                ReplayEvent item = outgoing.poll(10, TimeUnit.MILLISECONDS);
                if (cancelled) continue;
                if (item != null) {
                    if (sending == null) sending = new ArrayList<ReplayEvent>();
                    sending.add(item);
                    bytes += item.getRawData().length();
                }

                if (bytes > chunkSize) {
                    replayHandler.handle(sending);
                    sent.addAndGet(sending.size());
                    sending = null;
                    bytes = 0;
                }
            }
            if (sending != null) {
                replayHandler.handle(sending);
                sent.addAndGet(sending.size());
            }
        } catch (Throwable t){
            t.printStackTrace();
            LOGGER.warn("Update to handler failed:" + t, t);
            if (t.getMessage().contains("RetryInvocationException: SendFailed.Throwable:noSender")) {
                replayHandler = null;
            }
        } finally {
            if (isExpired() || request.isCancelled()) {
                this.future.cancel(false);
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
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
		this.outgoing.clear();
	}

	public boolean isExpired() {
		return replayHandler == null ||  this.request.isExpired() || this.request.isCancelled();
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
