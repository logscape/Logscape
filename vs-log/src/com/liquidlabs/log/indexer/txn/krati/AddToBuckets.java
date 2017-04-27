package com.liquidlabs.log.indexer.txn.krati;

import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.transport.serialization.Convertor;
import krati.store.DataStore;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AddToBuckets extends KratiTxnBase {
    private static final Logger LOGGER = Logger.getLogger(AddToBuckets.class);


    private DataStore<byte[], byte[]> store;
	private final List<Line> lines;

	public AddToBuckets(DataStore store, List<Line> lines) {
		this.store = store;
		this.lines = lines;
	}

	public void doWork() throws Exception {
		
		Map<Long, Bucket> myBuckets = new HashMap<Long, Bucket>();
		for (Line line : lines) {
			long minute = findBucketStart(line.time());
			Bucket bucket = myBuckets.get(minute);
			if (bucket == null) {
				byte[] bb = store.get(getPk(line.pk.logId, line.time));
				if (bb == null) {
					bucket = new Bucket(line.pk.logId, minute);
				} else {
                    bucket = (Bucket) Convertor.deserialize(bb);
                }
				myBuckets.put(minute, bucket);
			}
			bucket.update(line);
		}
		for(Bucket bucket : myBuckets.values()) {
            try {
                store.put(getPk(bucket.key.logId(), bucket.time()), Convertor.serialize(bucket));
            } catch (Throwable t) {

                LOGGER.warn("BucketStoreFailed:" + bucket + " StoreCapacity:" + store.capacity(), t);
                t.printStackTrace();
            }
		}
	}

}
