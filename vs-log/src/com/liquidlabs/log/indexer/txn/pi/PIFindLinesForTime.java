package com.liquidlabs.log.indexer.txn.pi;

import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.persistit.PILineStore;
import com.logscape.disco.indexer.Db;

import java.util.ArrayList;
import java.util.List;

import static com.liquidlabs.log.indexer.persistit.PILineStore.Task;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 13:23
 * To change this template use File | Settings | File Templates.
 */
public class PIFindLinesForTime extends PITxnBase {
    private Db<BucketKey, Bucket> store;
    private PILineStore lineStore;

    public PIFindLinesForTime(Db<BucketKey, Bucket> store, PILineStore lineStore) {
        this.store = store;
        this.lineStore = lineStore;
    }

    public List<Line> get(final LogFile logFile, final long fromTime, final int pageSize) {
        final List<Line> results = new ArrayList<Line>();
        if (logFile == null) return results;
        this.lineStore.iterateBuckets(logFile.getId(), fromTime, Long.MAX_VALUE, new Task(){
            @Override
            public boolean run(Bucket bucket) {
                int from = bucket.firstLine();
                int to = bucket.lastLine();
                int startLine = results.size() > 0 ? results.get(results.size() - 1).number() : from;
                int endLine = to;
                for (int i = startLine; i <= endLine; i++) {
                    if (i >= from && results.size() < pageSize) {
                        results.add(new Line(logFile.getId(), i, bucket.time(), bucket.startingPosition()));
                    }
                }
                if (results.size() >= pageSize) {
                    return true;
                }
                return false;
            }
        });

        return results;
    }
}
