package com.liquidlabs.log.space;

import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/07/2014
 * Time: 16:35
 * To change this template use File | Settings | File Templates.
 */
public class AbstractLogReplayHandler implements LogReplayHandler {
    @Override
    public void handle(ReplayEvent event) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void handle(Bucket event) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void handleSummary(Bucket bucketToSend) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int handle(String providerId, String subscriber, int size, Map<String, Object> histo) {
        return 1;
    }

    @Override
    public int handle(List<ReplayEvent> events) {
        return 100;
    }

    @Override
    public int status(String provider, String subscriber, String msg) {
        return 1;
    }

    @Override
    public String getId() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
