package com.logscape.disco.indexer.flatfile;

import com.logscape.disco.grokit.GrokItPool;
import com.logscape.disco.indexer.IndexFeed;
import com.logscape.disco.indexer.KvIndexFactory;
import com.logscape.disco.kv.RulesKeyValueExtractor;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 08/01/2015
 * Time: 15:01
 * To change this template use File | Settings | File Templates.
 */
public class BICreator implements KvIndexFactory.Creator {
    static public KvIndexFactory.IMPLS INST = KvIndexFactory.IMPLS.BI1;

    private GrokItPool grokitPool = new GrokItPool();

    @Override
    public IndexFeed get() {
        return new BIIndexFeed(KvIndexFactory.env, new RulesKeyValueExtractor(), grokitPool);
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
