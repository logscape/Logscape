package com.liquidlabs.log.search.facetmap;


import org.prevayler.Prevayler;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class PersistentMap<K,V> implements Map<K,V> {
    static final long serialVersionUID = 1L;


    private final Map<K,V> map;
    private final Prevayler prevayler;

    public PersistentMap(Prevayler prevayler) {
        this.map = (Map) prevayler.prevalentSystem();
        this.prevayler = prevayler;
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    @Override
    public V get(Object key) {
        return map.get(key);
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    public V put(K k, V v)  {
        map.put(k, v);
        prevayler.execute(new PutTransaction(k, v));
        return v;

    }

    @Override
    public V remove(Object key) {
        // TODO: add another TXN
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {

    }


    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }
}
