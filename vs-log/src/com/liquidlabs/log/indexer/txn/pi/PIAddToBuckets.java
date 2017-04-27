package com.liquidlabs.log.indexer.txn.pi;

import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.indexer.persistit.PILineStore;
import com.logscape.disco.indexer.Db;
import org.apache.log4j.Logger;

import java.util.*;

public class PIAddToBuckets extends PITxnBase {
    private static final Logger LOGGER = Logger.getLogger(PIAddToBuckets.class);


    private Db<BucketKey, Bucket> store;
	private final List<Line> lines;
    private PILineStore lineStore;

    public PIAddToBuckets(Db<BucketKey, Bucket> store, List<Line> lines, PILineStore lineStore) {
		this.store = store;
		this.lines = lines;
        this.lineStore = lineStore;
    }

	public void doWork() {
        Map<Long, Bucket> myBuckets = new HashMap<Long, Bucket>();

		for (Line line : lines) {
			long minute = findBucketStart(line.time());
			Bucket bucket = myBuckets.get(minute);
			if (bucket == null) {
                BucketKey bucketKey = new BucketKey(line.pk.logId, line.time);
                bucket = lineStore.get(bucketKey);
				if (bucket == null) {
					bucket = new Bucket(line.pk.logId, minute);
                }
                bucket.setKey(bucketKey);
				myBuckets.put(minute, bucket);
			}
			bucket.update(line);
		}
        List<Bucket> fixedBuckets = new ArrayList<Bucket>();
        TreeMap<Long, Bucket> buckets = new TreeMap<Long, Bucket>(myBuckets);
        int pos = 0;
        for(Bucket bucket : buckets.values()) {
            fixedBuckets.add(bucket);
            if (pos > 1) {
                Bucket previous = fixedBuckets.get(pos-1);
                if (previous.lastLine() != bucket.firstLine() -1) {
                    previous.lastLine = bucket.firstLine-1;
                }

            }
            pos++;

        }



        for(Bucket bucket : fixedBuckets) {
            lineStore.put(bucket.key, bucket);
        }
	}

}
