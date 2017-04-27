package com.logscape.disco.indexer.persistit;

import com.logscape.disco.grokit.GrokItPool;
import com.logscape.disco.indexer.*;
import com.logscape.disco.kv.RulesKeyValueExtractor;
import org.apache.log4j.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 06/01/2015
 * Time: 14:18
 * To change this template use File | Settings | File Templates.
 */
public class PersistitCreator implements KvIndexFactory.Creator {

    static public KvIndexFactory.IMPLS INST = KvIndexFactory.IMPLS.PERSISTIT;

    public static final int MAX_LUT_CACHE_SIZE = Integer.getInteger("pi.lut.cache",1024);
    private static final Logger LOGGER = Logger.getLogger(PersistitCreator.class);

    public static Db<Integer, String[]> lutDB = null;
    public static Db<TupleIntInt, int[]> kvIndexDB = null;
    private static GrokItPool grokItPool = new GrokItPool();


    public void close() {

    }

    @Override
    public IndexFeed get() {
        Dictionary dictionary = null;
        if (lutDB == null) {
            try {

                LOGGER.info("Using STORE:" + PersisitDbFactory.class.getSimpleName());
                lutDB = new CachedDb(PersisitDbFactory.getLutThreadedDb(), MAX_LUT_CACHE_SIZE,  Boolean.getBoolean("lru.run.cleanup.thread"));
                kvIndexDB = PersisitDbFactory.getIndexThreadedDb();

                if (kvIndexDB == null) throw new RuntimeException("Failed to created it");

            } catch (Throwable t) {
                t.printStackTrace();
                LOGGER.warn("Init Failed - Rebuild required?" + KvIndexFeed.rebuildRequired);
                System.out.println("Forcing Rebuild");
                KvIndexFeed.rebuildRequired = true;
                return null;
            }
        }

        dictionary = PersisitDbFactory.getDictionary();

        KvLutDb kvLut = new KvLutDb(lutDB, dictionary);
        KvIndexDb kvIndex = new KvIndexDb(kvIndexDB, dictionary);
        return new KvIndexFeed(new RulesKeyValueExtractor(), kvIndex, kvLut, dictionary, grokItPool, lutDB, kvIndexDB);
    }
}
