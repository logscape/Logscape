package com.logscape.disco.indexer.persistit;

import com.logscape.disco.indexer.Db;
import com.logscape.disco.indexer.Dictionary;
import com.logscape.disco.indexer.KvIndex;
import com.logscape.disco.indexer.TupleIntInt;
import com.persistit.*;

import java.util.HashMap;

/**
 * Stores the line[] that represents the lookups for field-name and field-value pairs
 * TuplieIntInt key represents the LogId.LineNumber Pair
 */
public class KvIndexDb implements KvIndex {

    private final PersistitThreadedDb ddb;
    private final Persistit pi;

    private Dictionary dictionary;

    public KvIndexDb(Db<TupleIntInt, int[]> db, Dictionary dictionary) {
        ddb = (PersistitThreadedDb) db;
        pi = ddb.getPersistit();

        this.dictionary = dictionary;
    }

    @Override
    public void index(int logId, int lineNo, String[] normalizedData) {

        try {
            TupleIntInt kk = new TupleIntInt(logId, lineNo);
            int[] normalize = dictionary.normalize(kk, normalizedData);

            Exchange exchange = ddb.getExchange();
            exchange.clear();
            exchange.append(logId).append(lineNo).getValue().put(normalize);
            exchange.store();
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
                public boolean run(Exchange exchange, int lineNo, int[] item) {
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
                Exchange exchange = ddb.getExchange();
                Value value = exchange.clear().append(logId).append(lineNo).fetch().getValue();
                if (!value.isDefined()) return null;

                return (int[]) value.get();
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
        iterateBuckets(logId, 0, Integer.MAX_VALUE, new Task(){
            @Override
            public boolean run(Exchange exchange, int line, int[] item) {
                try {
                    exchange.remove();
                } catch (Exception pex) {
                    pex.printStackTrace();
                }
                return false;
            }
        });    }
    public static interface Task {
        boolean run(Exchange exchange, int line, int[] lineItem);

    }
    public void iterateBuckets(int id, int from, int to, Task task) {

        try {
            Exchange exchange = ddb.getExchange();
            exchange.clear();
            KeyFilter filter = new KeyFilter();
            filter = filter.append(KeyFilter.simpleTerm(id));
            filter = filter.append(KeyFilter.rangeTerm(from, to));

            exchange.append(Key.BEFORE);
            boolean finished = false;
            while (exchange.next(filter) && !finished) {
                Key key = exchange.getKey();
                key.decode();
                int line = key.decodeInt();

                Value value = exchange.getValue();
                if (!value.isDefined()) {
                    finished=  true;
                    continue;
                }
                int[] b = (int[]) value.get();

                finished = task.run(exchange, line, b);
            }
        } catch (Exception pex) {
            throw new RuntimeException("IterationFailed", pex);
        }

    }

    @Override
    public void commit() {
        try {
            pi.flush();
        } catch(Exception pex) {
            pex.printStackTrace();
        }

    }

    @Override
    public void close() {
        try {
            pi.close();
        } catch(Exception pex) {
            pex.printStackTrace();
        }
    }
}
