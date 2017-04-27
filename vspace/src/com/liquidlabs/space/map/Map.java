package com.liquidlabs.space.map;

import java.util.Collection;
import java.util.Set;

public interface Map {
	void put(String key, String value) throws MapIsFullException;

	String get(String key);
	
	String[] getSplit(String key);

	Set<String> keySet();

	Collection<String> getValues();

	int getMaxSize();

	int size();

	boolean containsKey(String key);

	void put(String newKey, String newValue, boolean passEventToPeers) throws MapIsFullException;

	String remove(String key);
	String remove(String newKey, boolean passEventToPeers);


	void syncBulkUpdate() throws InterruptedException;

    java.util.Map<String, String> export();

    void importData(java.util.Map<String, String> data, boolean merge, boolean overwrite) throws MapIsFullException;

	void setStateSyncer(ArrayStateSyncer stateSyncer);
}
