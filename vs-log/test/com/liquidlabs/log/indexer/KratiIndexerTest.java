package com.liquidlabs.log.indexer;

import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.indexer.persistit.PIIndexerTest;

import static org.mockito.Mockito.mock;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 09:15
 * To change this template use File | Settings | File Templates.
 */
public class KratiIndexerTest extends PIIndexerTest {
    @Override
    protected void getIndexer() {
        indexer = new KratiIndexer(DIR, kvIndexFeed);
    }
}
