package com.liquidlabs.vso;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.Registrator;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.lookup.ServiceInfo;

public interface SpaceService extends LifeCycle, Registrator {

	Set<String> keySet(Class<?> class1);
	
	void start(Object object, String bundleName);

	<T> String registerListener(Class<T> eventType, String filter, Notifier<T> notifier, String notifierId, int lease, Type[] types);
	
	boolean unregisterListener(String listenerId);

	<T> String store(T object, int timeOutSecs);
	<T> String silentStore(T object, int timeOutSecs);

	<T> List<T> findObjects(Class<T> type, String filter, boolean cascade, int max);

	<T> T findById(Class<T> type, String id);

	<T> String[] findIds(Class<T> type, String query);


	<T> T remove(Class<T> type, String id);
	<T> List<T> remove(Class<T> type, String query, int limit);

	<T> int purge(Class<T> type, String template);

	int purge(List<?> entries);

	int size();
	
	int count(Class<?> type, String template);

	URI getClientAddress();

	void cancelLease(String leaseKey);

	String update(Class<?> type, String id, String updateStatement, int timeoutLease);
	int updateMultiple(Class<?> type, String query, String updateStatement, int limit, int timeoutLease, String leaseKey);

	void addReceiver(String id, Object listener);

	void renewLease(String leaseKey, int timeoutSeconds);
	void assignLeaseOwner(String lease, String owner);
	int renewLeaseForOwner(String owner, int timeoutSeconds);

	ScheduledExecutorService getScheduler();

	boolean containsKey(Class<?> type, String id);

	void stopProxy(String id);

	<Y extends Remotable> Y makeRemoteable(Y object);

    Map<String, String> exportData();

    Map<String, Object> exportObjects(String filter);

    void importData(Map<String, String> config, boolean merge, boolean overwrite);
    void importData2(Map<String, Object> fromXML, boolean merge, boolean overwrite);

    void addPeer(URI uri);

    void removePeer(URI uri);

	URI getReplicationURI();

	ProxyFactory proxyFactory();

    String rawById(Class<?> fieldSetClass, String id);

    void importData(Class<?> type, String item);

	ServiceInfo getServiceInfo();

	boolean isStarted();

	Space getSpace();


    String exportObjectAsXML(String filter, String tagStart, String tagEnd);

    void importFromXML(String xmlConfig, boolean merger, boolean overwrite, String config_start, String config_end);
}
