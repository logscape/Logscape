package com.logscape.disco.indexer.mapdb;

import com.logscape.disco.indexer.Db;
import org.mapdb.BTreeMap;
import org.mapdb.DB;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

public class MapDb<K, V> implements Db<K,V> {

    private DB db;
    private String tree;
    private final BTreeMap<K,V> treeMap;

    public MapDb(DB db, String tree) {
        this.db = db;
        this.tree = tree;
        treeMap = this.db.getTreeMap(tree);
    }

    @Override
    public String getName() {
        return this.tree;
    }

    @Override
    public V get(K key) {
        return (V) treeMap.get(key);
    }

    @Override
    public void put(K key, V value) {
        treeMap.put(key, value);
    }

    @Override
    public void remove(K fromId, K toId) {
        treeMap.subMap(fromId, toId).clear();
    }

    @Override
    public void remove(K fromId) {
        treeMap.remove(fromId);
    }

    @Override
    public void compact() {
        db.compact();
    }

    @Override
    public Set<K> keySet() {
        return treeMap.keySet();
    }

    @Override
    public Set<K> keySet(K fromId, K toId) {
        ConcurrentNavigableMap<K,V> map = treeMap.subMap(fromId, toId);
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return treeMap.values();
    }

    public int size() {
        return treeMap.size();
    }


    @Override
    public void commit() {
        db.commit();
    }

    public void copy(MapDb from) {
        treeMap.putAll(from.treeMap);
    }

    public void clearAndCompact() {
        treeMap.clear();
        db.commit();
        compact();
    }

    public void close() {
        db.close();
    }

    @Override
    public void release() {
    }

    @Override
    public void clear() {
        treeMap.clear();

    }


}
