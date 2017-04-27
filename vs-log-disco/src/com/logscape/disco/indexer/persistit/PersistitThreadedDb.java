package com.logscape.disco.indexer.persistit;

import com.liquidlabs.common.collection.cache.ConcurrentLRUCache;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.logscape.disco.indexer.Db;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.PersistitMap;
import com.persistit.exception.PersistitException;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Set;

/**
 * Thread LocalVersion
 */
public class PersistitThreadedDb<K, V> implements Db<K, V> {

    private static final Logger LOGGER = Logger.getLogger(PersistitThreadedDb.class);
    private static final long CACHE_SIZE = Integer.getInteger("persisitit.cache..size", 100);

    final private String db;
    final private String tree;
    final private String dbTree;
    final private Persistit persistit;
    private final String exchangeName;
    private ThreadLocal threadLocal;
    private boolean create;


    public PersistitThreadedDb(String db, String tree, Persistit persistit, ThreadLocal threadLocal, boolean create){
        LOGGER.info("Creating:" + db + ":" + tree);
        this.db = db;
        this.tree = tree;
        this.dbTree = db+tree;
        this.persistit = persistit;
        this.threadLocal = threadLocal;
        this.create = create;
        this.exchangeName = dbTree + "EX";
    }

    @Override
    public String getName() {
        return this.db;
    }

    public Persistit getPersistit() {
        return this.persistit;
    }

    @Override
    public V get(K key) {
        try {
            return getMap().get(key);
        } catch (Throwable t) {
            if (persistit.isClosed()) {
                LOGGER.error("DB was closed:" + this.db + "/" + tree, t);

            }
            LOGGER.error("Fail to get:" + key, t);
            return null;
        }
    }

    @Override
    public void clear() {
        getMap().clear();
//        commit();
    }

    @Override
    public void put(K key, V value) {
        getMap().putFast(key, value);
    }

    @Override
    public void remove(K fromId) {
        getMap().removeFast(fromId);
    }

    @Override
    public void remove(K fromId, K toId) {
        getMap().subMap(fromId, toId).clear();
    }

    @Override
    public void compact() {
        // Not Needed
    }

    @Override
    public Set<K> keySet() {
        return getMap().keySet();
    }
    public Set<K> keySet(K from, K to) {
        return getMap().subMap(from, to).keySet();
    }
    public Collection<V> values() {
        return getMap().values();
    }

    @Override
    public int size() {
        return getMap().size();
    }

    @Override
    public void commit() {
        Thread task = new Thread("persistit-flush-" + getName()) {
            @Override
            public void run() {
                try {
                    persistit.flush();
                } catch (PersistitException e) {
                    e.printStackTrace();
                }
            }
        };

        if (!ExecutorService.isTestMode()) {
            task.setDaemon(true);

        }
//        task.start();
    }

    @Override
    public void close() {
        try {
            LOGGER.warn("Close:" + this.db +"/" + tree);
            // remove the thread local
            resetMap();
            persistit.close();
        } catch (PersistitException e) {
            e.printStackTrace();
        }
    }
    public Exchange getExchange() {
        ConcurrentLRUCache<String, Object> localMap = getThreadLocal();
        Exchange exchange = (Exchange) localMap.get(exchangeName);
        if (exchange == null) {
            try {
                exchange = persistit.getExchange(db, tree, create);
                localMap.put(exchangeName, exchange);
                threadLocal.set(localMap);
            } catch (PersistitException e) {
                e.printStackTrace();
                String message = "GetMapFailed:" + db + "/" + tree;
                System.err.println(message);

                throw new RuntimeException(message, e);
            }
        }
        return exchange;
    }

    public PersistitMap<K, V> getMap() {
        ConcurrentLRUCache<String, Object> localMap = getThreadLocal();
        PersistitMap<K, V> piMap = (PersistitMap<K, V>) localMap.get(dbTree);
        if (piMap != null) return piMap;


            Exchange exchange = getExchange();
            piMap = new PersistitMap<K,V>(exchange);
            piMap.setAllowConcurrentModification(true);
            localMap.put(dbTree, piMap);
            threadLocal.set(localMap);

            return piMap;

    }

    private ConcurrentLRUCache<String, Object> getThreadLocal() {
        ConcurrentLRUCache<String, Object> dbMap = (ConcurrentLRUCache<String, Object>) threadLocal.get();
        if (dbMap == null) {
            dbMap = new ConcurrentLRUCache<String, Object>((int) CACHE_SIZE, 10);
            threadLocal.set(dbMap);
        }
        return dbMap;
    }

    public void resetMap() {
        threadLocal.set(null);
    }

    @Override
    public void release() {
    }
}
