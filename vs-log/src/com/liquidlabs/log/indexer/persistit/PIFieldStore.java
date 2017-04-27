package com.liquidlabs.log.indexer.persistit;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.indexer.AbstractIndexer;
import com.logscape.disco.indexer.*;
import com.liquidlabs.transport.serialization.Convertor;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/02/2013
 * Time: 12:41
 * To change this template use File | Settings | File Templates.
 */
public class PIFieldStore {
    private Db<String, byte[]> store;

    public PIFieldStore(Db<String, byte[]> store) {
        try {
            //store = AbstractIndexer.getStore(environment, "DT", threadLocal);
            this.store = store;
            if (!Boolean.getBoolean("log.db.readonly")) add(FieldSets.getBasicFieldSet());
        } catch (Exception e) {
            throw new RuntimeException("Failed:", e);
        }
    }
    public static final ThreadLocal threadLocal = new ThreadLocal();

    public FieldSet get(String fieldSetId) {
        try {
            FieldSet result = (FieldSet) Convertor.deserialize(store.get(fieldSetId));
            if (result != null) {
                result.upgrade();
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed:", e);
        }
    }

    public void add(FieldSet fieldSet) throws Exception {
        store.put(fieldSet.getId(), Convertor.serialize(fieldSet));
    }

    public List<FieldSet> list(Indexer.Filter<FieldSet> filter) {
        List<FieldSet> results = new ArrayList<FieldSet>();
        Set<String> fieldSets = store.keySet();
        for (String key : fieldSets) {
            FieldSet fieldSet = null;
            try {
                fieldSet = (FieldSet) Convertor.deserialize(store.get(key));
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (filter.accept(fieldSet) && fieldSet != null) results.add(fieldSet);

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
            if (data != null) {
                store.remove(data.getId());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            if (store != null) {
                this.store.commit();
                this.store.close();
                this.store = null;
            }
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void sync() {
        try {
            this.store.commit();
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
