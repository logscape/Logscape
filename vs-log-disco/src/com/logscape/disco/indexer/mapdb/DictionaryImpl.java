package com.logscape.disco.indexer.mapdb;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.cache.ConcurrentLRUCache;
import com.logscape.disco.DiscoProperties;
import com.logscape.disco.indexer.Db;
import com.logscape.disco.indexer.Dictionary;
import com.logscape.disco.indexer.TupleIntInt;

import java.text.DecimalFormat;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 30/01/2014
 * Time: 09:36
 * To change this template use File | Settings | File Templates.
 */
public class DictionaryImpl implements Dictionary {

    private final static int CACHESIZE = 2 * 1024;

    private final String db;
    private final boolean create;

    private final int maxDictionaryLimit = DiscoProperties.getMaxKVDictionaryLimit();
    private final Db<Integer, Integer> countDb;


    public DictionaryImpl(String db, boolean create) {
        this.db = db;
        this.create = create;
        countDb = MapDbFactory.getDictCountDb(db);
    }

    private ConcurrentLRUCache<Integer, Db<Integer, String>> dicts = new ConcurrentLRUCache<Integer, Db<Integer, String>>(200, 100, new ConcurrentLRUCache.EvictionListener<Integer, Db<Integer, String>>() {
        public void evictedEntry(Integer key, Db<Integer, String> value) {
            value.release();
        }
    });
    private ConcurrentLRUCache<Integer, Db<String, Integer>> reverseDicts = new ConcurrentLRUCache<Integer, Db<String, Integer>>(200, 100, new ConcurrentLRUCache.EvictionListener<Integer, Db<String, Integer>>() {
        public void evictedEntry(Integer key, Db<String, Integer> value) {
            value.release();
        }
    });

    @Override
    public Db<Integer, String> get(int logId) {
        Db<Integer, String> found = dicts.get(logId);
        if (found != null) return found;

        synchronized (this) {
            Db<Integer, String> dbStore = MapDbFactory.getDictDb(db + "dict-" + logId);
            dicts.put(logId, dbStore);
        }
        return dicts.get(logId);
    }
    public Db<String, Integer> getReverse(int logId) {
        Db<String, Integer> found = reverseDicts.get(logId);
        if (found != null) return found;

        synchronized (this) {
            Db<String, Integer> dbStore = MapDbFactory.getRevDictDb(db + "reverse-dict-" + logId);
            reverseDicts.put(logId, dbStore);
        }
        return reverseDicts.get(logId);
    }

    DecimalFormat numberFormat = new DecimalFormat("#.####");

    @Override
    public int[] normalize(TupleIntInt kk, String[] normalizedData) {
        Db<Integer, String> integerStringDb = this.get(kk.a);
        Db<String, Integer> reverseDB = this.getReverse(kk.a);

        Integer count = this.countDb.get(kk.a);
        if (count == null) count = 0;
        int size = count;


        int[] results = new int[normalizedData.length];
        int pos = 0;
        for (int source = 1; source < normalizedData.length; source += 2){
            String lineString = normalizedData[source];
            Integer someValue = StringUtil.isInteger(lineString);
            // if we have a positive int value - then just store it as a negative....
            if (someValue != null && someValue > 0) {
                // LUT - Key
                results[pos++] =  StringUtil.isInteger(normalizedData[source-1]);
                // Value - now substituted with
                results[pos++] = someValue * -1;

            } else {

                if (lineString.contains(".")) {
                    // shrink doubles to 4 decimal places
                    Double doubleValue = StringUtil.isDouble(lineString);
                    if (doubleValue != null) {
                        lineString = numberFormat.format(doubleValue);
                    }
                }

                String string = lineString;
                Integer integer = reverseDB.get(string);

                if (integer == null) {
                        integer = size++;
                        integerStringDb.put(integer, string);
                        reverseDB.put(string, integer);
                }
                results[pos++] = Integer.parseInt(normalizedData[source-1]);
                results[pos++] = integer;
            }
        }
        countDb.put(kk.a, size);
        return results;
    }

    @Override
    public void remove(int logId) {

        get(logId).clear();
        Db<String, Integer> revers = getReverse(logId);
        if (revers != null) revers.clear();
        dicts.remove(logId);
        reverseDicts.remove(logId);
        // TODO: remove the tree from persistit.exchange
    }

    @Override
    public void compact() {
    }


    public void commit() {
        countDb.commit();
    }
    public void close() {
        countDb.close();
    }
}

