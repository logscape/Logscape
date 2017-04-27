package com.liquidlabs.space;


import java.util.List;
import java.util.Map;
import java.util.Set;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.lease.LeaseManagerPublic;
import com.liquidlabs.space.lease.Leases;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.space.raw.UpdaterRules;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.PeerSender;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.Matchers;

/**
 * see http://java.sun.com/products/jini/2.0/doc/api/net/jini/space/JavaSpace.html
 * For Specific functionality about this implementation see
 * {@link Matchers}
 * {@link Leases}
 * {@link UpdaterRules}
 *
 */
public interface Space extends LeaseManagerPublic, LifeCycle {
	static final String DELIM =  Config.OBJECT_DELIM;

	/**
	 * add to the space
	 * @param timeout
	 * @return lease key
	 */
	String write(String key, String value, long timeout);
	String internalWrite(String key, String value, long timeout);
	
	/**
	 * Registers an event listener object that will feed all corresponding events
	 * to the client
	 * @param keys
	 * @param templates
	 * @param listener
	 * @param eventMask
	 * @param expires
	 * @return The LeaseID
	 */
	public String notify(String[] keys, String[] templates, EventListener listener, Type[] eventMask, long expires);
	
	boolean removeNotification(String listenerKey);

	/**
	 * Return number of items matching the template
	 * @param limit 
	 */
	int count(String[] templates, int limit);
	
	/**
	 * Read any matching object from the space, blocking until one exists. Return null if the timeout expires. 
	 */
	String read(String key);
	String read(String key, long timeout);
	String[] read(String[] keys);
	
	/**
	 * See {@link Matchers} to understand operators for templates
	 * @param template syntax per template item is "equals:itemValue, gt>200, All:" etc
	 * @param limit TODO
	 * @return
	 */
	String[] readMultiple(String[] template, int limit);
	String[] readMultiple(String[] templates, long timeout, int limit);
	/**
	 * for update template see {@link UpdaterRules}  i.e. - x|x|replace:YY<br>
	 */
	String[] readAndUpdateMultiple(String[] template, String[] update, long timeout, int limit, String leaseKey);
	String[] readKeys(String[] template, int limit);

	/**
	 * Take any matching entry from the space, blocking until one exists.
	 * @param timeout
	 * @param expires
	 */
	String internalTake(String id);
	String take(String key, long timeout, long expires);
	String[] take(String[] keys);
	String[] takeMultiple(String[] template, int limit, long timeout, long expires);

	/**
	 * Complete removes all traces, leases etc of an item
	 * @param string
	 */
	int purge(String key);
	void purge(List<String> keys);
	/**
	 * An update operation against a space entity using an update template<br>
	 * See {@link UpdaterRules}  i.e. - x|x|replace:YY<br>
	 */
	String update(String key, String updateTemplate, long timeout);
	int updateMultiple(String[] templates, String[] updateTemplates, long timeout, int limit, String leaseKey);

	String[] getKeys(String[] searchTemplate, int limit);
	Set<String> keySet();
	Set<String> leaseKeySet();

	EventHandler getEventHandler();
	
	boolean containsKey(String key);
	String internalRead(String key);
	
	int size();
	
	void addPeer(URI uri);
	


	
//	void registerProcessor(Processor processor, String template, long intervalSecs, long timeToLiveSecs);
//	void registerListener(EventListener listener, String template, long timeToLiveSecs);

    java.util.Map<String, String> exportData();

    void importData(Map<String, String> data, boolean merge, boolean overwrite);
	void addPeerListener(PeerSender peerSender);
	void setReplicationURI(URI replicationUri);
	URI getReplicationURI();

    void removePeer(URI uri);
}
