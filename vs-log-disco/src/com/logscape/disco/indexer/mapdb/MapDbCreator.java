package com.logscape.disco.indexer.mapdb;

import com.logscape.disco.grokit.GrokItPool;
import com.logscape.disco.indexer.*;
import com.logscape.disco.kv.RulesKeyValueExtractor;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 06/01/2015
 * Time: 14:27
 * To change this template use File | Settings | File Templates.
 */
public class MapDbCreator implements KvIndexFactory.Creator {

    static public KvIndexFactory.IMPLS INST = KvIndexFactory.IMPLS.MAPDB;

    private Db<Integer,String[]> lutDB;
    private Db<TupleIntInt, int[]> kvIndexDB;
    private static GrokItPool grokItPool = new GrokItPool();
    private DictionaryImpl dictionary;

    @Override
    public void close() {
    }

    @Override
    public IndexFeed get() {
        if (lutDB == null) {
            lutDB = MapDbFactory.getLutDb();
            kvIndexDB = MapDbFactory.getIndexDb();
            dictionary = new DictionaryImpl(KvIndexFactory.env, true);
        }
        KvLutDb kvLut = new KvLutDb(lutDB, dictionary);
        KvIndexMapDb kvIndex = new KvIndexMapDb(kvIndexDB, dictionary);
        return new KvIndexFeed(new RulesKeyValueExtractor(), kvIndex, kvLut, dictionary, grokItPool, lutDB, kvIndexDB);
    }
}
