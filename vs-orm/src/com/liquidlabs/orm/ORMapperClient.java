package com.liquidlabs.orm;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.raw.UpdaterRules;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.Matchers;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ORMapperClient {

	void store(List<Object> target);
	
	/**
	 * Stores an item in the space without a Lease
	 * @param <T> is the type of object
	 * @param target is the instance
	 */
	<T> void store(T target);
	/**
	 * Stores an item in the space with a expirey lease
	 * where the object is removed if lease is not maintained
	 * @param <T> is the type of object
	 * @param target is the instance
	 * @return the leaseKey
	 */
	<T> String store(T target, int leasePeriodSeconds);
	
	/**
	 * doesnt raise events
	 */
	<T> String silentStore(T target, int timeout);

	<T> String[] findIds(Class<T> type, String query, int limit);
	<T> List<T> findObjects(Class<T> type, String query, boolean cascade, int limit);
	
	/**
	 * @param selectQuery Select using "fieldsName operator value AND/OR xxx" See {@link Matchers} for operators ( ==, equals, contains, notContains, <, =<)
	 * @param updateValue using "fieldName operator Value AND xxx" See {@link UpdaterRules} for operators (concat, prepend, replace, += etc)
	 * @param limit determines the maxItems returned, use -1 or Int.Max to disable
	 * @param leaseKey determines the client provided key (must be UUID)
	 */
	<T> List<T> findAndUpdate(Class<T> type, String selectQuery, String updateValue, long timeoutLease, boolean cascade, int limit, String leaseKey);
	
	
	/**
	 * orClient.update(User.class, "id equals userId", "surname replaceWith bob AND firstName replaceWith charlie", 1, -1, "xxx");
	 * can use concat, prepend, *= etc
	 * @see UpdaterRules
	 */
	<T> int updateMultiple(Class<T> type, String selectQuery, String update, int limit, int timeoutLease, String leaseKey);
	<T> String update(Class<T> type, String id, String update, long timeoutLease);
	
	<T> List<T> removeObjects(Class<T> type, String query, boolean cascade, long timeout, long expires, int limit);
	<T> T remove(Class<T> type, String id, boolean cascade, long timeout, long expires);
	
	<T> T retrieve(Class<T> type, String id, boolean cascade);
	
	<T> boolean containsKey(Class<T> type, String id);

	/**
	 * 
	 * @param type
	 * @param template
	 * @param callback
	 * @param types
	 * @param timeout
	 * @return a leaseKey is one was applied
	 */
	String registerEventListener(Class<?> type, String template, ORMEventListener callback, Type[] types, int timeout);
	boolean unregisterEventListener(String listenerId);
	
	int count(Class<?> type, String template);
	int count(Class<?> type, String query, int limit);
	
	<T> int purge(List<T> entries);
	int purge(Class<?> type, String template);

	void cancelLease(String leaseKey);
	void renewLease(String leaseKey, int expires);
	void assignLeaseOwner(String leaseKey, String owner);
	int renewLeaseForOwner(String owner, int timeoutSeconds);

	int size();
	Set<String> keySet();
	Set<String> leaseKeySet();


    Map<String, String> exportData();

    void importData(Map<String, String> data, boolean merge, boolean overwrite);

	void addPeer(URI peer);

	URI getReplicationURI();

    void removePeer(URI uri);

    String stringValue(Class<?> type, String id);

    void importData(Class<?> type, String items);

	Space getSpace();

	Object getObject(String key);
}
