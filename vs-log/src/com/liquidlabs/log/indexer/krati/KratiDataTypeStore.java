package com.liquidlabs.log.indexer.krati;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.transport.serialization.Convertor;
import krati.core.StoreConfig;
import krati.store.DataStore;
import krati.store.DynamicDataStore;
import krati.store.SafeDataStoreHandler;
import krati.util.IndexedIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 12:41
 * To change this template use File | Settings | File Templates.
 */
public class KratiDataTypeStore {
    private DataStore<byte[], byte[]> store;

    public KratiDataTypeStore(String environment, ExecutorService executor) {
        try {
            StoreConfig _config = new StoreConfig(new File(environment + "/DATA_TYPE"), 100);

            //_config.setSegmentFactory(new MemorySegmentFactory());
            _config.setSegmentFactory(KratiIndexer.getChannelFactory());
            _config.setSegmentFileSizeMB(8);
            _config.setDataHandler(new SafeDataStoreHandler());

            store = new DynamicDataStore(_config);

            add(FieldSets.getBasicFieldSet());
        } catch (Exception e) {
            throw new RuntimeException("Failed:", e);
        }

    }

    public FieldSet get(String fieldSetId) {
        try {
            FieldSet result = (FieldSet) Convertor.deserialize(store.get(fieldSetId.getBytes()));
            if (result != null) result.upgrade();
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed:", e);
        }
    }

    public void add(FieldSet fieldSet) throws Exception {
        store.put(fieldSet.getId().getBytes(), Convertor.serialize(fieldSet));
    }

    public List<FieldSet> list(Indexer.Filter<FieldSet> filter) {
        IndexedIterator<byte[]> indexedIterator = store.keyIterator();
        List<FieldSet> results = new ArrayList<FieldSet>();
        while (indexedIterator.hasNext()) {
            byte[] key = indexedIterator.next();
            try {

                FieldSet fieldSet = (FieldSet) Convertor.deserialize(store.get(key));
                if (filter.accept(fieldSet) && fieldSet != null) results.add(fieldSet);
            } catch (Exception e) {
                System.out.println("BOOM:" + new String(key));
                e.printStackTrace();
            }
        }
        Collections.sort(results, new Comparator<FieldSet>() {
            public int compare(FieldSet o1, FieldSet o2) {
                try {
                return Integer.valueOf(o2.priority).compareTo(o1.priority);
                } catch (Throwable t) {
                    return 0;
                }
            }
        });
        return results;

    }

    public void remove(FieldSet data) {
        try {
            store.delete(data.getId().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void close() {
        try {
            if (store != null) {
                this.store.sync();
                this.store.close();
                this.store = null;
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void sync() {
        try {
            this.store.persist();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
