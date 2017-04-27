package com.logscape.disco.indexer.mapdb;

import com.logscape.disco.indexer.Db;
import com.logscape.disco.indexer.Dictionary;
import com.logscape.disco.indexer.KvIndex;
import com.logscape.disco.indexer.TupleIntInt;
import com.logscape.disco.indexer.persistit.PersistitThreadedDb;
import com.persistit.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Stores the line[] that represents the lookups for field-name and field-value pairs
 * TuplieIntInt key represents the LogId.LineNumber Pair
 */
public class KvIndexMapDb implements KvIndex {


    private Db<TupleIntInt, int[]> db;
    private Dictionary dictionary;



    public KvIndexMapDb(Db<TupleIntInt, int[]> db, Dictionary dictionary) {
        this.db = db;
        this.dictionary = dictionary;
    }

    @Override
    public void index(int logId, int lineNo, String[] normalizedData) {

        try {
            TupleIntInt kk = new TupleIntInt(logId, lineNo);
            int[] normalize = dictionary.normalize(kk, normalizedData);

            db.put(new TupleIntInt(logId, lineNo), normalize);

        } catch(Exception pex) {
            pex.printStackTrace();
        }
    }

    int currentLogId = -1;
    HashMap<Integer, int[]> lineFeed = new HashMap<Integer, int[]>();
    private static boolean preloadLines = System.getProperty("kv.index.lines.preload","true").equals("true");

    @Override
    public int[] get(int logId, int lineNo) {
        if (preloadLines) {

            // check the read-ahead.
            if (currentLogId == logId) {
                int[] integers = lineFeed.get(lineNo);
                if (integers != null) return integers;
                else lineFeed.clear();
            } else {
                // changed logId
                lineFeed.clear();
                currentLogId = logId;
            }
            // now iterate over the range
            iterateBuckets(logId, lineNo, lineNo + 50, new Task(){
                @Override
                public boolean run(int lineNo, int[] item) {
                    lineFeed.put(lineNo, item);
                    return false;
                }
            });
            return lineFeed.get(lineNo);
        } else {

            if (nextLineLogId == logId && nextLineNum == lineNo) {
                return nextLineInts;
            }

            try {
                int[] ints = db.get(new TupleIntInt(logId, lineNo));
                return ints;
            } catch(Exception pex) {
                pex.printStackTrace();
                throw new RuntimeException(pex);
            }
        }
    }
    int nextLineLogId = -1;
    int nextLineNum = -1;
    int[] nextLineInts = null;

    @Override
    public void remove(int logId) {
        db.remove(new TupleIntInt(logId, 1), new TupleIntInt(logId, Integer.MAX_VALUE));
    }
    public static interface Task {
        boolean run(int line, int[] lineItem);

    }
    public void iterateBuckets(int id, int from, int to, Task task) {

        try {
            Set<TupleIntInt> tupleIntInts = db.keySet(new TupleIntInt(id, from), new TupleIntInt(id, to));
            boolean finished = false;
            Iterator<TupleIntInt> iterator = tupleIntInts.iterator();
            while (iterator.hasNext() && !finished) {
                TupleIntInt  key = iterator.next();
                int[] line = db.get(key);
                finished = task.run((Integer) key.b, line);
            }
        } catch (Exception pex) {
            throw new RuntimeException("IterationFailed", pex);
        }

    }

    @Override
    public void commit() {
        try {
            db.commit();
        } catch(Exception pex) {
            pex.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            db.close();
        } catch(Exception pex) {
            pex.printStackTrace();
        }
    }
}
