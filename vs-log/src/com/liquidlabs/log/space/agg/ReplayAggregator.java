package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.search.handlers.ReplayEventsHandler;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.transport.proxy.Remotable;

/**
 * Created by Neiil on 11/7/2015.
 */
public interface ReplayAggregator extends Runnable {
    void cancel();

    void handle(ReplayEvent replayEvent);

    boolean isExpired();

    void close();

    int size();

    void flush();

    LogReplayHandler getReplayHandler();
}
