package com.liquidlabs.log.space;

import java.util.List;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.transport.proxy.clientHandlers.*;

public interface AggSpace extends LifeCycle {

	String NAME = AggSpace.class.getSimpleName();
	String NAME_REPLAY = NAME + "_REPLAY";

	@Broadcast
	void search(LogRequest request, String handlerId, LogReplayHandler replayHandler) throws Exception;

	@Broadcast
	void replay(LogRequest request, String handlerId, LogReplayHandler replayHandler);

	@FailFastAndDisable
	@Broadcast
	void cancel(String subscriberId);
	
	@RoundRobin(factor=2)
	int write(Bucket currentBucket, boolean cacheable, String requestHash, int logId, long eventTime);
	
	@RoundRobin(factor=2)
	int write(ReplayEvent replayEvent, boolean cacheable, String requestHash, int logId, long timeSecs);
	
	@RoundRobin(factor=2)
	int write(LogEvent logEvent);

	@RoundRobin(factor=2)
    @Interceptor(clazz ="com.liquidlabs.log.search.DefaultSearchInterceptor")
	int write(List<Bucket> bucket);

	int size();

	@Broadcast
	void registerEventListener(LogEventListener eventListener, String listenerId, String filter, int leasePeriod) throws Exception;

	@Broadcast
	void unregisterEventListener(String listenerId);

    @RoundRobin(factor=2)
    @Interceptor(clazz ="com.liquidlabs.log.search.DefaultSearchInterceptor")
    void writeReplays(List<ReplayEvent> replayEvents);

	/**
	 * statistics pushed from each tailer task - used for progress reporting
	 * @param subscriber
	 * @param resourceId
	 * @param amount = -1 indicated that the search is complete
	 * @param fileScanPercentComplete
	 */
	int status(String subscriber, String resourceId, long amount, int fileScanPercentComplete) throws Exception;

    @Interceptor(clazz ="com.liquidlabs.log.search.DefaultSearchInterceptor")
	void writeSummary(Bucket summaryBucket, boolean cacheable, String requestHash, int logId, long eventTime);

	@Decoupled
	void msg(String id, String string);

    @Decoupled
    void flush(String subscriber, boolean finished) throws Exception;

}
