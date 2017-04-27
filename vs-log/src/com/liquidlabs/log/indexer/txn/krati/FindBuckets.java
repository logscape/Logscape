package com.liquidlabs.log.indexer.txn.krati;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.transport.serialization.Convertor;
import krati.store.DataStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 12:16
 * To change this template use File | Settings | File Templates.
 */
public class FindBuckets extends KratiTxnBase {
    private DataStore<byte[], byte[]> store;

    public FindBuckets(DataStore<byte[], byte[]> store) {

        this.store = store;
    }
    public List<Bucket> find(int id, long startTime, long endTime) {
        if(store == null) {
            return Collections.emptyList();
        }
        List<Bucket> results = new ArrayList<Bucket>();
        for (long time = startTime; time < endTime; time += bucketWidth ) {
            byte[] pk = getPk(id, time);
            byte[] bucket = store.get(pk);
            if (bucket != null) {
                try {
                    results.add((Bucket) Convertor.deserialize(bucket));
                } catch (IOException e) {
                    return results;
                } catch (ClassNotFoundException e) {
                    return results;
                }
            }
        }


        return results;
    }
}
