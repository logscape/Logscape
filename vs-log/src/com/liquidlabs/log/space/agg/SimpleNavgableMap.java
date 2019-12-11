package com.liquidlabs.log.space.agg;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Makes a non-navigablemap - navigable
 * Shares the underlying chronicle map for re-use
 */
public class SimpleNavgableMap<K,V> implements Map<K,V> {

    private Set<K>keySet = new ConcurrentSkipListSet<K>();
    private java.util.Map<K, V> navMap;

    public SimpleNavgableMap(java.util.Map<K, V> navMap) {
        this.navMap = navMap;
    }
    @Override
    public int size() {
        return keySet.size();
    }

    @Override
    public boolean isEmpty() {
        return keySet.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return keySet.contains(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public V get(Object key) {
        return navMap.get(key);
    }

    @Override
    public V put(K key, V value) {
        V put = navMap.put(key, value);
        keySet.add(key);
        return put;
    }

    @Override
    public V remove(Object key) {
        keySet.remove(key);
        return navMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        navMap.putAll(m);
        keySet.addAll(m.keySet());
    }

    @Override
    public void clear() {
        for (K k : keySet) {
            navMap.remove(k);
        }
        keySet.clear();
    }

    @Override
    public Set<K> keySet() {
        return keySet;
    }

    @Override
    public Collection<V> values() {
        List<V> values = new ArrayList<V>();
        for (K k : keySet) {
            values.add(navMap.get(k));
        }
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        HashSet<Entry<K, V>> entries = new HashSet<>();
        for (K k : keySet) {
            entries.add(new AbstractMap.SimpleEntry<K, V>(k, navMap.get(k)));
        }
        return entries;
    }
    public void close() {
        this.clear();
    }
}
