package com.liquidlabs.log.indexer.txn.krati;

import krati.store.DataStore;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 13:36
 * To change this template use File | Settings | File Templates.
 */
public class RemoveLines extends KratiTxnBase {
    private static final Logger LOGGER = Logger.getLogger(RemoveLines.class);

    private DataStore<byte[], byte[]> store;
    int count = 0;

    public RemoveLines(DataStore<byte[], byte[]> store) {
        this.store = store;
    }

    public void go(int id, long startTime, long endTime) {
        for (long start = startTime; start < endTime; start += bucketWidth) {
            byte[] pk = getPk(id, start);
            try {
                if (store != null) store.delete(pk);
                count++;
            } catch (Exception e) {
                if (count < 10) LOGGER.error("RemoveLinesFailed:" + " count:" + count + " msg:" + e.getMessage(), e);
            }

        }
    }
    public int count() {
        return count;
    }
}
