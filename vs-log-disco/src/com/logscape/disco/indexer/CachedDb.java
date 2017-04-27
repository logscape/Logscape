package com.logscape.disco.indexer;

import com.google.common.base.Optional;
import com.liquidlabs.common.collection.cache.ConcurrentLRUCache;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 15/01/2014
 * Time: 17:11
 * To change this template use File | Settings | File Templates.
 */
public class CachedDb<K,V> implements Db<K,V> {

    final private Db<K, V> source;
    private Set<K>  keySet = null;
    final private ConcurrentLRUCache<K, Optional<V>> cache;

    public CachedDb(Db<K, V> source, int maxSize, boolean isCleanupThread) {
        this.source = source;
        cache = new ConcurrentLRUCache<K, Optional<V>>(maxSize, (int)(maxSize * 0.8), isCleanupThread);
        cache.setAlive(false);
        cache.setName(source.getName());
    }

    @Override
    public String getName() {
        return "CachgedDB"+ this.source.getName();
    }

    @Override
    public V get(final K key) {
            Optional<V> vOptional = cache.get(key);
            // Miss
            if (vOptional == null) {
                  vOptional = Optional.fromNullable(source.get(key));
                cache.put(key, vOptional);
            }
            // hit
            if (vOptional.isPresent()) {
                return vOptional.get();
            }
            else return null;
    }


    @Override
    public void put(K key, V value) {
        cache.put(key, Optional.of(value));
        source.put(key, value);
        keySet = null;
    }

    @Override
    synchronized public void remove(K key, K keyTo) {
        Set<K> set = source.keySet(key, keyTo);
        for (K k : set) {
            cache.remove(k);
        }
        source.remove(key, keyTo);
        keySet = null;
    }

    @Override
    synchronized public void remove(K key) {
        cache.remove(key);
        source.remove(key);
    }

    @Override
    public void compact() {
        source.compact();
    }

    @Override
    public Set keySet() {
        if (this.keySet != null) return this.keySet;
        HashSet<K> keySet = new HashSet<K>();
        Set<K> sourceKeySet = source.keySet();
        keySet.addAll(sourceKeySet);
        this.keySet = keySet;
        return keySet;
    }

    @Override
    public Set keySet(K fromKey, K toKey) {
        return source.keySet(fromKey, toKey);
    }

    @Override
    public Collection<V> values() {
        return source.values();
    }

    @Override
    public int size() {
        return source.size();
    }

    @Override
    public void commit() {
        source.commit();
    }
    public void release() {
        cache.clear();
        cache.destroy();
    }

    @Override
    public void close() {
        release();
        source.close();
    }

    @Override
    public void clear() {
        cache.clear();
        source.clear();
    }
}
