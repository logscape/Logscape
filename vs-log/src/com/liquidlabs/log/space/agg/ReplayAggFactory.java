package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogRequest;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 09/11/15
 * Time: 13:25
 * To change this template use File | Settings | File Templates.
 */
public class ReplayAggFactory {

    public static final String CHRONICLE_QUEUE = "chronicle.queue";
    private static String type = System.getProperty("event.agg.type", CHRONICLE_QUEUE);

    static public ReplayAggregator get(LogRequest request, LogReplayHandler handler, String handlerId) {
        if (type.equals(CHRONICLE_QUEUE)) {
            return new ChronicleQReplayAggregator(request, handler, handlerId);
        } else if (type.equals("chronicle")) {
            return new ChronicleReplayAggregator(request, handler, handlerId);
        } else if (type.equals("mapdb")) {
            return new MapDBReplayAggregator(request, handler, handlerId);
        } else {
            return new NativeQueuedReplayAggregator(request, handler, handlerId);
        }
    }
}
