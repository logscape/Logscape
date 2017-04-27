package com.liquidlabs.log.indexer;

import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.indexer.persistit.PIIndexer;

/**
 * Created by neil on 06/08/2015.
 */
public class Indexers {

    public static Indexer get() {
        String dbImpl = System.getProperty("indexer.impl", "persistit");
        if (AbstractIndexer.isSchemaRebuildRequired(LogProperties.getEventsDB())) {
            AbstractIndexer.rebuildIndex(LogProperties.getEventsDB());
        }

        if (dbImpl.equals("persistit")) {
            return new PIIndexer(LogProperties.getEventsDB());
        } else if (dbImpl.equals("krati")) {
            return new KratiIndexer(LogProperties.getEventsDB());
        } else if (dbImpl.equals("babudb")) {
            return new KratiIndexer(LogProperties.getEventsDB());

        }
        throw new RuntimeException("Didnt find a store!");
    }
}
