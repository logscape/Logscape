package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.TimeUID;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MapDBReplayAggregator implements ReplayAggregator {

	private final static Logger LOGGER = Logger.getLogger(MapDBReplayAggregator.class);
	private final Map<TimeUID, ReplayEvent> map;
    static DB dbMaker = DBMaker.newTempFileDB().transactionDisable().cacheWeakRefEnable().mmapFileEnableIfSupported().deleteFilesAfterClose().make();

	private LinkedBlockingQueue<TimeUID> outgoing = new LinkedBlockingQueue<TimeUID>();
	private final LogRequest request;
	private LogReplayHandler replayHandler;

	private long startTime = new DateTime().getMillis();

	private boolean cancelled = false;

    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");


	public String listenerId;
    private ScheduledFuture<?> future;

    public MapDBReplayAggregator(LogRequest request, LogReplayHandler replayHandler, String handlerId) {
		this.request = request;
		this.replayHandler = replayHandler;
		this.listenerId = request.subscriber() + "_id";

        this.future = scheduler.scheduleWithFixedDelay(this, 1, Integer.getInteger("replay.queue.pump",2), TimeUnit.MILLISECONDS);

        map = dbMaker.createHashMap("DB").makeOrGet();
	}

	public void handle(ReplayEvent event) {
		if (cancelled) {
			return ;
		}

        map.put(event.getId(), event);
		outgoing.add(event.getId());
	}

    int chunkSize = 4 * 1024;
	public void run(){
		ReplayEvent lastGoodOne = null;

		try {

			ArrayList<ReplayEvent> sending = null;
            int bytes = 0;

			while (outgoing.size() > 0) {
				TimeUID item = outgoing.poll(10, TimeUnit.MILLISECONDS);
				if (cancelled) return;
				if (item != null) {

					if (sending == null) sending = new ArrayList<ReplayEvent>();
					ReplayEvent item1 = map.remove(item);
					lastGoodOne = item1;
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
			t.printStackTrace();

			if (lastGoodOne != null) System.out.println("LastGoodOne:" + lastGoodOne);
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
        this.cancelled = true;
		LOGGER.info("LOGGER Cancelled:" + request.subscriber() + " Listener:" + listenerId);
        if (this.future != null) {
            future.cancel(false);
        }
        if (map != null) {
            map.clear();

        }
		this.outgoing.clear();

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
