package com.liquidlabs.log.indexer.txn.krati;

import com.liquidlabs.common.DateUtil;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 12:17
 * To change this template use File | Settings | File Templates.
 */
public class KratiTxnBase {
   public long bucketWidth = DateUtil.MINUTE;
    byte[] getPk(int logId, Long time) {
        // make fixed width integer
        return ("" + logId + "" + findBucketStart(time)).getBytes();
    }

    long findBucketStart(long time) {
        return DateUtil.floorMin(time);
    }
}
