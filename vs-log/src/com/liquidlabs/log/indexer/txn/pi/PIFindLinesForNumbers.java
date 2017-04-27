package com.liquidlabs.log.indexer.txn.pi;

import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.indexer.persistit.PILineStore;
import com.logscape.disco.indexer.Db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 13:12
 * To change this template use File | Settings | File Templates.
 */
public class PIFindLinesForNumbers extends PITxnBase {
    private Db<BucketKey, Bucket> store;
    private PILineStore lineStore;


    public PIFindLinesForNumbers(Db<BucketKey, Bucket> store, PILineStore lineStore) {
        this.store = store;
        this.lineStore = lineStore;
    }
    Bucket previousBucket = null;
    List<Line> results = new ArrayList<Line>();
    Bucket bucket = null;
    boolean finished = false;

    public List<Line> get(final int id, final long fromTime, final long toTime, final int fromLine, final int toLine) {
        final int length = toLine  - fromLine;
        lineStore.iterateBuckets(id, fromTime, toTime, new PILineStore.Task() {
            @Override
            public boolean run(Bucket bucket) {
                try {
                    int bucketFrom = bucket.firstLine();
                    int bucketTo = bucket.lastLine();

                    // not there yet
                    if (bucketTo < fromLine) return false;

                    // passed the end of point of interest
                    if (bucketFrom > toLine) {
                        return true;
                    }

                    // If the previous bucket intersets the start<=>end then grab it as well
                    if (previousBucket != null && previousBucket.lastLine() > fromTime && results.size() == 0) {
                        for (int cLine = previousBucket.firstLine; cLine < previousBucket.lastLine && !finished; cLine++) {
                            results.add(new Line(id, cLine, previousBucket.time(), previousBucket.startingPosition()));
                            if (results.size() >= (length * 3)) return true;
                        }
                    }

                    // add a line for each bucket-line item
                    for (int cLine = bucketFrom; cLine <= bucketTo && !finished; cLine++) {
                        results.add(new Line(id, cLine, bucket.time(), bucket.startingPosition()));
                        if (results.size() >= (length * 3)) return true;
                    }
                } finally {
                    previousBucket = bucket;
                }
            return false;
            }
        });

        Collections.sort(results, new Comparator<Line>(){
            public int compare(Line o1, Line o2) {
                return Integer.valueOf(o1.number()).compareTo(o2.number());
            }

        });
        return results;
    }
}
