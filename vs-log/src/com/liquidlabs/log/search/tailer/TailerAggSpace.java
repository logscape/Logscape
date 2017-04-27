package com.liquidlabs.log.search.tailer;

import com.liquidlabs.log.search.Canceller;
import com.liquidlabs.log.space.AggSpace;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 17/06/2015
 * Time: 14:27
 * To change this template use File | Settings | File Templates.
 */
public interface TailerAggSpace extends  AggSpace, Canceller {
    void cancelRequest(String subscriber);
}
