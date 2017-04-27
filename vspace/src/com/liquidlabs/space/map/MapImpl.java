package com.liquidlabs.space.map;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import javolution.util.FastMap;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.space.Space;

/**
 * Simple map implementation - state information is backed and used in peer replication strategy
 */
public class MapImpl implements Map {

    transient private ArrayStateSyncer stateSyncer;

    private java.util.Map<String, String> values = new FastMap<String, String>();
    java.util.Map<String, String[]> splitValues = new FastMap<String, String[]>();

    private int size = 10 * 1024;

    public MapImpl() {
    	((FastMap)values).shared();
    	((FastMap)splitValues).shared();

    }

    public MapImpl(String srcUID, String partition, int size, boolean isClustered, ArrayStateSyncer stateSyncer) {
    	((FastMap)values).shared();
    	((FastMap)splitValues).shared();
        this.size = size;
        this.stateSyncer = stateSyncer;
    }

    public void clear() {
        values.clear();
        splitValues.clear();
    }

    public void put(String key, String value) throws MapIsFullException {
        put(key, value, true);

    }

    public void put(String key, String value, boolean passEventToPeers) throws MapIsFullException {

        if (value == null) {
            System.err.println("PUTTING NULL FOR KEY:" + key);
            Thread.dumpStack();
        }

        splitValues.put(key, Arrays.split(Space.DELIM, value));
        String existingValue = values.put(key, value);
        // put == null with new entry
        if (existingValue == null) {
            // PUT
            if (stateSyncer != null && passEventToPeers) stateSyncer.updateToLocal(0, null, null, key, value);
//            postionAge.add(key);
        } else {
//            postionAge.remove(key);
//            postionAge.add(key);
            // OVERWRITE
            if (stateSyncer != null && passEventToPeers) stateSyncer.updateToLocal(0, key, existingValue, key, value);
        }

//		the problem with this is we may endup with items in there because their age was lost...
//		while (postionAge.size() > size) {
//			postionAge.poll();
//		}
    }

    public String remove(String key) {
        return remove(key, true);
    }

    public String remove(String key, boolean passEventToPeers) {
        String existingValue = values.remove(key);
        splitValues.remove(key);
//        postionAge.remove(key);
        // TAKE
        if (stateSyncer != null && passEventToPeers) stateSyncer.updateToLocal(0, key, existingValue, key, null);
        return existingValue;
    }

    public Set<String> keySet() {
        return values.keySet();
    }

    public String get(String key) {
        return values.get(key);
    }

    public String[] getSplit(String key) {
        return splitValues.get(key);
    }

    public Collection<String> getValues() {
        return values.values();
    }

    public int getMaxSize() {
        return size;
    }

    public int size() {
        return values.size();
    }

    public String getOldestKey() {
        return "";//postionAge.peek();
    }

    public boolean containsKey(String key) {
        return values.containsKey(key);
    }

    public void syncBulkUpdate() throws InterruptedException {
        if (this.stateSyncer != null) this.stateSyncer.sendPartitionEvent();
    }

    public java.util.Map<String, String> export() {
        java.util.Map<String, String> map = new HashMap<String, String>();
        for (String key : keySet()) {
            map.put(key, get(key));
        }

        return map;
    }

    public void importData(java.util.Map<String, String> data, boolean merge, boolean overwrite) throws MapIsFullException {
        if (!merge) {
            clear();
        }
        for (String key : data.keySet()) {
            if (overwrite) put(key, data.get(key), true);
            else if (get(key) == null) put(key, data.get(key), true);
        }
    }

    public String toString() {
        return super.toString() + " size:" + values.size();
    }

	public void setStateSyncer(ArrayStateSyncer stateSyncer2) {
		this.stateSyncer = stateSyncer2;
	}
}
