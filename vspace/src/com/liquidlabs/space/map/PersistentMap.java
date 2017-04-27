package com.liquidlabs.space.map;


import java.util.Collection;
import java.util.Set;

import org.prevayler.Prevayler;

public class PersistentMap implements Map {

    private final Map map;
    private final Prevayler prevayler;

    public PersistentMap(Prevayler prevayler) {
        this.map = (Map) prevayler.prevalentSystem();
        this.prevayler = prevayler;
    }

    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    public String get(String key) {
        return map.get(key);
    }

    public int getMaxSize() {
        return map.getMaxSize();
    }

    public String[] getSplit(String key) {
        return map.getSplit(key);
    }

    public Collection<String> getValues() {
        return map.getValues();
    }

    public Set<String> keySet() {
        return map.keySet();
    }

    public void put(String key, String value) throws MapIsFullException {
        put(key, value, true);

    }

    public void put(String newKey, String newValue, boolean passEventToPeers)
            throws MapIsFullException {
    	map.put(newKey, newValue, passEventToPeers);
        prevayler.execute(new Put(newKey, newValue, passEventToPeers));
    }


    public String remove(String key) {
        return remove(key, false);
    }

    public String remove(String key, boolean passEventToPeers) {
        return (String) prevayler.execute(new Remove(key, passEventToPeers));
    }

    public void syncBulkUpdate() {

    }

    public int size() {
        return map.size();
    }

    public java.util.Map<String, String> export() {
       return map.export();
    }

    public void importData(java.util.Map<String, String> data, boolean merge, boolean overwrite) throws MapIsFullException {
        prevayler.execute(new BulkImport(data, merge, overwrite));
    }

	public void setStateSyncer(ArrayStateSyncer stateSyncer) {
		map.setStateSyncer(stateSyncer);
	}
	public Map getMap() {
		return map;
	}


}
