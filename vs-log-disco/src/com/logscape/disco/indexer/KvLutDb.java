package com.logscape.disco.indexer;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.collection.CompactCharSequence;
import com.logscape.disco.SystemIndexer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KvLutDb implements LookUpTable {
    private static final Logger LOGGER = Logger.getLogger(KvLutDb.class);

    private final Db<Integer, String[]> db;
    private Dictionary dictionary;

    public KvLutDb(Db mapDb, Dictionary dictionary) {
        this.db = mapDb;
        this.dictionary = dictionary;
    }

    @Override
    public String[] normalize(int logId, List<Pair> fieldIs) {
        String[] lut = db.get(logId);
        if(lut == null) {
            lut = new String[0];
        }
        Map<String,String> vMap = new HashMap<String,String>();
        boolean added = false;

        for (Pair fieldI : fieldIs) {
            String name = fieldI.key;
            if(!com.liquidlabs.common.collection.Arrays.contains(lut, name)) {
                added = true;
                lut = Arrays.append(lut, name);
            }
            vMap.put(name, fieldI.value);
        }
        if(added) {
            db.put(logId, lut);
        }
        List<String> results = new ArrayList<String>();

        for (int i = 0; i < lut.length; i++) {
            String key = lut[i];
            String value = vMap.get(key);
// Basic
//            if (value == null) value = "";
//            results.add(value);
// RLE
//            if (value == null && firstItem == -1) continue;
//            else if (firstItem == -1) {
//                // track the first item  - put in in pos-0
//                results.add(i+"");
//                firstItem = i;
//            }
//            results.add(value);

// MAP
            if (value != null) {
                results.add(i + "");
                results.add(value);
            }

        }

        return results.toArray(new String[0]);
    }
// MAP method
    public List<Pair> get(int logId, int[] values, long timeMs) {
        String[] keys = db.get(logId);
        final ArrayList<Pair> results = new ArrayList<Pair>();
        if (keys == null) return results;
        Addit addit = new Addit() {
            @Override
            public void append(String key, String value) {
                results.add(new Pair(key, value));
            }
        };

        buildKeyValues(logId, values, keys, addit, timeMs);
        return results;
    }
    public Map<String,String> getAsMap(int logId, int[] values, long timeMs) {
        String[] keys = getLut(logId);
        if (keys == null) keys = new String[0];

        //final HashObjObjMap<String,String> results = HashObjObjMaps.getDefaultFactory().<String, String>newMutableMap();
        final HashMap<String,String> results = new HashMap<String, String>();

        Addit addit = new Addit() {
            @Override
            public void append(String key, String value) {
                results.put(key, value);
            }
        };
        buildKeyValues(logId, values, keys, addit, timeMs);
        return results;
    }
    private SystemIndexer systemIndexer = new SystemIndexer();

    public static class ADictionary {
        int dictId = -1;
        Db<Integer, String> dictionary;
        public void set(int logId, Db<Integer, String> dictionary) {
            this.dictId = logId;
            this.dictionary = dictionary;
        }
    }
    ADictionary lastDictionary = new ADictionary();

    private void buildKeyValues(int logId, int[] values, String[] keys, Addit addit, long timeMs) {
        int index = 0;
        try {
            if (lastDictionary.dictId != logId) {
                lastDictionary.set(logId, dictionary.get(logId));
            }
            Db<Integer, String> dictionary = lastDictionary.dictionary;

            for(int i=0; i < values.length-1; i+=2) {
                try {
                    int keyId = values[i];
                    String key = keys[keyId];
                    index = i;
                    int value1 = values[i + 1];
                    String value = null;
                    if (value1 >= 0) {
                        value = dictionary.get(value1).toString();

                    } else {
                        value = Integer.toString(value1 * -1);
                    }
                    addit.append(key, value);
                } catch (ArrayIndexOutOfBoundsException aoob) {
                    // ignore it...
                }
            }
            systemIndexer.indexA(timeMs, addit);

        } catch (Exception ex) {
            LOGGER.warn("ArrayEx LogId:" + logId + " Index:" + index + " Keys:" + keys.length + "  Ex:" + ex.toString() );//Arrays.toString(values));

        }
    }

    public interface Addit {
        void append(String key, String value);
    }

    LastCalled lastCalled = new LastCalled(-1, new String[0]);

    public static class LastCalled {
        public LastCalled(int lastId, String[] lastKeys) {
            set(lastId,lastKeys);
        }
        public void set(int lastId, String[] lastKeys) {
            this.lastId = lastId;
            this.lastKeys = lastKeys;
            this.timestamp = System.currentTimeMillis();
        }
        int lastId = -1;
        String[] lastKeys;
        long timestamp = System.currentTimeMillis();
    }

    private String[] getLut(int logId) {
        LastCalled lc = lastCalled;
        if (lc.lastId == logId && lc.lastKeys != null && lc.timestamp > System.currentTimeMillis() - 60 * DateUtil.SECOND) {
            return lc.lastKeys;
        }
        lc.set(logId, db.get(logId));
        return lc.lastKeys;
    }

    @Override
    public void remove(int logId) {
        db.remove(logId);
    }

    @Override
    public void commit() {
        db.commit();
    }

    @Override
    public void close() {
        db.close();
    }


// simple method = full scan
//    public List<FieldI> get(int logId, String[] values) {
//        String[] keys = db.get(logId);
//        ArrayList<FieldI> results = new ArrayList<FieldI>();
//        if (keys == null) return results;
//
//        for(int i=0; i < Math.min(values.length, keys.length); i++) {
//            String value = values[i];
//            if (value != null && value.length() > 0) {
//                results.add(new LiteralField(keys[i], 0,true,true, value,"count"));
//            }
//        }
//        return results;
//    }

    // RLE Method - where [0] is the offset
//    public List<FieldI> get(int logId, String[] values) {
//        String[] keys = db.get(logId);
//        ArrayList<FieldI> results = new ArrayList<FieldI>();
//        if (keys == null) return results;
//
//        int fromPos = Integer.parseInt(values[0]);
//        int scanTo = values.length + fromPos -1;
//        for(int i=fromPos; i < scanTo; i++) {
//            int valueIndex = i - fromPos + 1;
//            String value = values[valueIndex];
//            if (value != null && value.length() > 0) {
//                results.add(new LiteralField(keys[i], 0,true,true, value,"count"));
//            }
//        }
//        return results;
//    }
}
