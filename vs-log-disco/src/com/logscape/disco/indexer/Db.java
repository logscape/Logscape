package com.logscape.disco.indexer;

import java.util.Collection;
import java.util.Set;

public interface Db<K,V> {

    String getName();

    V get(K key);

    void put(K key, V value);

    void remove(K key, K keyTo);

    void remove(K key);

    void compact();

    Set<K> keySet();

    Set<K> keySet(K fromKey, K toKey);

    Collection<V> values();

    int size();

    void commit();

    void release();

    void close();

    void clear();

}
