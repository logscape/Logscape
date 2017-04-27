package com.liquidlabs.log.indexer.txn.krati;

import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.transport.serialization.Convertor;
import krati.store.DataStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 13:23
 * To change this template use File | Settings | File Templates.
 */
public class FindLinesForTime extends KratiTxnBase {
    private DataStore<byte[], byte[]> store;

    public FindLinesForTime(DataStore<byte[], byte[]> store) {
        this.store = store;
    }

    public List<Line> get(LogFile logFile, long fromTime, int pageSize) {
        List<Line> results = new ArrayList<Line>();


        for (long time = fromTime; time < logFile.getStartEndTimes().get(1).getMillis(); time += bucketWidth) {
            byte[] pk = getPk(logFile.getId(), time);
            if (pk != null) {
                try {
                    Bucket bucket = (Bucket) Convertor.deserialize(store.get(pk));
                    if (bucket == null) continue;

                    int from = bucket.firstLine();
                    int to = bucket.lastLine();
                    int startLine = results.size() > 0 ? results.get(results.size() - 1).number() : from;
                    int endLine = to;
                    for (int i = startLine; i <= endLine; i++) {
                        if (i >= from && results.size() < pageSize) {
                            results.add(new Line(logFile.getId(), i, bucket.time(), bucket.startingPosition()));
                        }
                    }
                    if (results.size() >= pageSize) time = logFile.getEndTime();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }

        }
        return results;
    }
}
