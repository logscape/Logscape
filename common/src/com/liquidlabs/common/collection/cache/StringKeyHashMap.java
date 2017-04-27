package com.liquidlabs.common.collection.cache;

import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.*;

/**
 *  Uses a double lookup map for handling strings
 */
public class StringKeyHashMap<K,V> implements Map<K,V> {

    TIntObjectHashMap<InternalMapEntry> unique = new TIntObjectHashMap<InternalMapEntry>();
    final Map<K, V> notUnique = new HashMap<K, V>();



    public int size() {
        return unique.size();
    }

    public boolean isEmpty() {
        return unique.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return get(o) != null;
    }

    @Override
    public boolean containsValue(Object o) {
        return get(o) != null;
    }


    @Override
    public V get(Object key) {
        InternalMapEntry o1 = (InternalMapEntry) unique.get(key.hashCode());
        if (o1 != null && o1.getKey().equals(key)) return (V) o1.getValue();
        V o2 = notUnique.get(key);
        if (o2 != null) return (V) o2;
        return null;
    }

    @Override
    public V put(K k, V v) {
        int hash = k.hashCode();
        Map.Entry<K,V> o1 = unique.get(hash);
        if (o1 == null) {
            unique.put(hash, new InternalMapEntry(k, v));
        } else if (o1.getKey().equals(k)) {
            unique.put(hash, new InternalMapEntry(k, v));
        } else {
            // collision
            Map.Entry<K,V> existing = unique.remove(hash);
            notUnique.put(existing.getKey(), existing.getValue());
        }
        return null;
    }

    @Override
    public V remove(Object o) {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        unique.clear();;
        notUnique.clear();
    }

    @Override
    public Set<K> keySet() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Collection<V> values() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return null;
    }

    public static class InternalMapEntry<K,V> implements Entry {
        private final K key;
        private final V value;

        public InternalMapEntry(K key, V value) {

            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return this.value;
        }

        public Object setValue(Object o) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

    }



}
