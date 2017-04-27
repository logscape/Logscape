package com.logscape.disco.indexer;


import com.logscape.disco.kv.KVExtractor;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 23/01/2014
 * Time: 12:51
 * To change this template use File | Settings | File Templates.
 */
public interface IndexFeed {


    KVExtractor kvExtractor();

    public enum FIELDS  { _line };

    List<Pair> index(int logId, String filename, int lineNo, long timeMs, String data, boolean isFieldDiscoveryEnabled, boolean grokDiscoveryEnabled, boolean isSystemFieldsEnbled, List<Pair> indexedFields);

    void store(int id, int line, List<Pair> discovered);

    List<Pair> get(int logId, int lineNo);

    Map<String,String> getAsMap(int logId, int lineNo, long timeMs);
    void remove(int logId);
    void commit();
    void compact();
    void close();
    boolean isRebuildRequired();

    void setKvExtractor(KVExtractor kve);

    void reset();

}
