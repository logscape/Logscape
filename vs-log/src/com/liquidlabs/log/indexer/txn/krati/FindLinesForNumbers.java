package com.liquidlabs.log.indexer.txn.krati;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.transport.serialization.Convertor;
import krati.store.DataStore;

import java.io.IOException;
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
public class FindLinesForNumbers extends KratiTxnBase {
    private DataStore<byte[], byte[]> store;


    public FindLinesForNumbers(DataStore<byte[], byte[]> store) {
        this.store = store;
    }

    public List<Line> get(int id, long fromTime, long toTime, int fromLine, int toLine) {

        List<Line> results = new ArrayList<Line>();
        Bucket bucket = null;
        boolean finished = false;
        Bucket previousBucket = null;
        final int length = toLine  - fromLine;

        for (long time = fromTime; time < toTime; time += bucketWidth) {
            byte[] pk = getPk(id, time);
            try {
                try {
                    bucket = (Bucket) Convertor.deserialize(store.get(pk));
                }catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    continue;
                }
                // yes it can happen!
                if (bucket == null) continue;



                int bucketFrom = bucket.firstLine();
                int bucketTo = bucket.lastLine();

                // not there yet
                if (bucketTo < fromLine) continue;

                // passed the end of point of interest
                if (bucketFrom > toLine) {
                    finished = true;
                    continue;
                }

                // If the previous bucket intersets the start<=>end then grab it as well
                if (previousBucket != null && previousBucket.lastLine() > fromTime && results.size() == 0) {
                    for (int cLine = previousBucket.firstLine; cLine < previousBucket.lastLine && !finished; cLine++) {
                        results.add(new Line(id, cLine, previousBucket.time(), previousBucket.startingPosition()));
                        if (results.size() >= (length * 3)) finished = true;
                    }
                }

                // add a line for each bucket-line item
                for (int cLine = bucketFrom; cLine <= bucketTo && !finished; cLine++) {
                    results.add(new Line(id, cLine, bucket.time(), bucket.startingPosition()));
                    if (results.size() >= (length * 3)) finished = true;
                }
            } finally {
                previousBucket = bucket;
            }
        }

        Collections.sort(results, new Comparator<Line>(){
            public int compare(Line o1, Line o2) {
                return Integer.valueOf(o1.number()).compareTo(o2.number());
            }

        });
        return results;
    }
}
