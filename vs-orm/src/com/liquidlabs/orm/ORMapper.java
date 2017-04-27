package com.liquidlabs.orm;


import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.transport.proxy.events.Event.Type;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ORMapper {
	
	static final String NAME = ORMapper.class.getSimpleName();

	int purge(String type, String id);
	int purge(String[] template);
	void purge(String type, String... ids);
	
	int count(String type, String[] template);
	int count(String type, String[] template, int limit);
	boolean containsKey(String string, String id);
	
	String[] findIds(String type, String[] template, int limit);
	String[] readObject(String type, String id, boolean cascade);
	String[] takeObject(String type, String id, boolean cascade, long timeout, long expires);
	String[] takeObjects(String type, String[] template, boolean cascade, int limit, long timeout, long expires);
	String storeObject(String type, String id, String contents, long timeout);
	String silentStoreObject(String type, String id, String contents, long timeout);
	void storeMapping(String type, String id, String childId, String mapping, int timeout);
	String registerEventListener(String[] templates, ORMEventListener callback, Type[] eventMask, long expires);
	boolean unregisterEventListener(String listenerId);
	void update(ORMItem[] items, int timeoutLease);
	String update(String type, String key, String updateTemplate, long timeout);
	int updateMultiple(String type, String[] selectQueryString, String[] updateTemplate, int limit, long timeoutLease, String leaseKey);
	String[] findAndUpdate(String name, String[] selectQueryString, String[] updateTemplate, int limit, long timeoutLease, String leaseKey);
	
	void renewLease(String leaseKey, int expires);
	void assignLeaseOwner(String leaseKey, String owner);
	int renewLeaseForOwner(String owner, int timeoutSeconds);
	void cancelLease(String leaseKey);
	
	int size();
	Set<String> keySet();
	Set<String> leaseKeySet();

    Map<String, String> exportData();

    void importData(Map<String, String> data, boolean merge, boolean overwrite);
    
	void addPeer(URI peer);
	URI getReplicationURI();

    void removePeer(URI uri);

    String rawObject(Class<?> type, String id);
    
	Space getSpace();
}
