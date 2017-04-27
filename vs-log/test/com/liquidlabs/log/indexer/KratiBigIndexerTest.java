package com.liquidlabs.log.indexer;

import com.liquidlabs.log.indexer.krati.KratiIndexer;

public class KratiBigIndexerTest extends PIBigIndexerTest {
    protected void getIndexer() {
        indexer = new KratiIndexer(DIR);
    }
}
