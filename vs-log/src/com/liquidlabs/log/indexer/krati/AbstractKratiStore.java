package com.liquidlabs.log.indexer.krati;

import com.liquidlabs.log.indexer.AbstractIndexer;
import krati.store.DataStore;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by neil on 06/08/2015.
 */
public abstract class AbstractKratiStore  {
    static Logger LOGGER = Logger.getLogger(AbstractKratiStore.class);

    DataStore<byte[], byte[]> store;

    public void close() {
        try {
            if (store != null) {
                this.store.sync();
                this.store.close();
                this.store = null;
            } else {
                LOGGER.warn("Store is already closed!");
            }
        } catch (IOException e) {
            LOGGER.error("Close error", e);
        }
    }

    public void sync() {
        try {
            if (store != null) this.store.persist();
        } catch (IOException e) {
            LOGGER.error("Sync error", e);
        }
    }
}
