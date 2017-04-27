package com.logscape.disco.indexer;

import com.logscape.disco.kv.KVExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 23/01/2014
 * Time: 12:53
 * To change this template use File | Settings | File Templates.
 */
public class StubIndexFeed implements IndexFeed {

    private ArrayList<Pair> pairs;

    @Override
    public KVExtractor kvExtractor() {
        return null;
    }

    @Override
    public List<Pair> index(int logId, String filename, int lineNo, long timeMs, String data, boolean isFieldDiscoveryEnabled, boolean grokDiscoveryEnabled, boolean systemFieldsEnabled, List<Pair> indexedFields) {
        pairs = new ArrayList<Pair>();
        return pairs;
    }

    @Override
    public List<Pair> get(int logId, int lineNo) {
        return pairs;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setKvExtractor(KVExtractor kve) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void store(int id, int line, List<Pair> discovered) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, String> getAsMap(int logId, int lineNo, long timeMs) {
        return new HashMap<String,String>();
    }

    @Override
    public void remove(int logId) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void commit() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void compact() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void reset() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isRebuildRequired() {
       return false;
    }

}
