package com.liquidlabs.space.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.LifeCycleManager;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.LeaseManager;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.transport.PeerSender;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;

/**
 * SpaceImpl that supports Leasing (i.e. take and notification operations can expire) 
 */
public class LeasedClusteredSpace implements Space {
	private static final Logger LOGGER = Logger.getLogger(LeasedClusteredSpace.class);
	
	final Space dataSpace;
	final Space clusterEventSpace;
	private LeaseManager leaseManager;
	private NotificationClusterManager notificationClusterManager;
	private LifeCycleManager lifeCycleManager;
	private final String partitionName;
	private PeerSender peerSenderListener;
	private URI replicationUri;

	public LeasedClusteredSpace(String partitionName, Space dataSpace, Space clusterMgmtSpace, LeaseManager leaseManager, ProxyFactory proxyFactory, boolean isClustered, String sourceId) {
		this.partitionName = partitionName;
		this.dataSpace = dataSpace;
		this.clusterEventSpace = clusterMgmtSpace;
		this.leaseManager = leaseManager;
		this.lifeCycleManager = new LifeCycleManager();
		if (isClustered) notificationClusterManager = new NotificationClusterManager(dataSpace, clusterMgmtSpace, leaseManager, proxyFactory, partitionName, sourceId);
	}
	public String notify(String[] keys, String[] templates, EventListener listener, Type[] eventMask, long expires) {
		this.dataSpace.notify(keys, templates, listener, eventMask, expires);
		if (this.notificationClusterManager != null) this.notificationClusterManager.notify(keys, templates, listener.getId(), eventMask, expires);
		return leaseManager.obtainNotifyLease(listener.getId(), expires);
	}
	public boolean removeNotification(String listenerKey) {
		return this.dataSpace.removeNotification(listenerKey);
	}
	
	public int count(String[] templates, int limit) {
		return dataSpace.count(templates, limit);
	}

	public String[] getKeys(String[] searchTemplate, int limit) {
		return dataSpace.getKeys(searchTemplate, limit);
	}

	public Set<String> keySet() {
		return dataSpace.keySet();
	}
	public Set<String> leaseKeySet() {
		return this.clusterEventSpace.keySet();
	}

	public String[] read(String[] keys) {
		return dataSpace.read(keys);
	}

	public String read(String key, long timeout) {
		return dataSpace.read(key,timeout);
	}

	public String read(String key) {
		return dataSpace.read(key);
	}

	public String[] readKeys(String[] template, int limit) {
		return dataSpace.readKeys(template, limit);
	}

	public String[] readMultiple(String[] templates, long timeout, int limit) {
		return dataSpace.readMultiple(templates, timeout, limit);
	}

	public String[] readMultiple(String[] template, int limit) {
		return dataSpace.readMultiple(template, limit);
	}

	public String[] take(String[] keys) {
		return dataSpace.take(keys);
	}

	public String take(String key, long timeout, long expires) {
		String value = dataSpace.take(key, timeout, expires);
		leaseManager.obtainTakeLeaseTxn(key, value, expires);
		return value;
	}
	public String internalTake(String key) {
		return dataSpace.internalTake(key);
	}
	public int purge(String key) {
		int result = dataSpace.purge(key);
		leaseManager.purgeForItem(key);
		return result;
	}
	public void purge(List<String> key) {
		dataSpace.purge(key);
		leaseManager.purgeForItems(key);		
	}
	
	public String[] takeMultiple(String[] searchTemplates, int limit, long timeout, long expires) {
		String[] keys = dataSpace.getKeys(searchTemplates, limit);
		if (limit < 0) limit = Integer.MAX_VALUE;
		List<String> values = new ArrayList<String>();
		int count = 0;
		for (String key : keys) {
			// apply leasing
			if (count++ < limit) values.add(take(key, timeout, expires));
		}
		return Arrays.toStringArray(values);
	}

	public String update(String key, String updateTemplate, long timeout) {
		String oldValue = dataSpace.read(key);
		if (oldValue == null) return null;
		dataSpace.update(key, updateTemplate, timeout);
		if (oldValue != null) {
			return leaseManager.obtainUpdateLease(new String[] { key }, new String[] { oldValue}, timeout, null);	
		}
		return null;
	}
	public String[] readAndUpdateMultiple(String[] template, String[] update, long timeout, int limit, String leaseKey) {
		String[] readAndUpdateMultiple = dataSpace.readAndUpdateMultiple(template, update, timeout, limit, leaseKey);
		String[] readKeys = dataSpace.readKeys(template, limit);
		if (readKeys.length > 0) {
			leaseManager.obtainUpdateLease(readKeys, readAndUpdateMultiple, timeout, leaseKey);
		}
		return readAndUpdateMultiple;
	}

	public int updateMultiple(String[] templates, String[] updateTemplate, long timeout, int limit, String leaseKey) {
		String[] readKeys = dataSpace.readKeys(templates, limit);
		String[] values = dataSpace.readMultiple(templates, limit);
		
//		if (readKeys.length == 0) throw new RuntimeException(String.format("Failed to find any items for template:%s", Arrays.toString(templates)));
		int result = dataSpace.updateMultiple(templates, updateTemplate, timeout, limit, leaseKey);
		leaseManager.obtainUpdateLease(readKeys, values, timeout, leaseKey);
		return result;
	}

	public String write(String key, String value, long timeoutSeconds) {
		dataSpace.write(key, value, -1);
		if (timeoutSeconds == 0) {
			dataSpace.internalTake(key);
			return "";
		}
		return leaseManager.obtainWriteLease(key, value, timeoutSeconds);
	}
	
	public String internalWrite(String key, String value, long timeoutSeconds) {
		dataSpace.internalWrite(key, value, timeoutSeconds);
		if (timeoutSeconds == 0) {
			dataSpace.internalTake(key);
			return "";
		}
		return leaseManager.obtainWriteLease(key, value, timeoutSeconds);

	}
	public String internalRead(String key) {
		return dataSpace.internalRead(key);
	}

	public void renewLease(String leaseKey, long expires) {
		leaseManager.renewLease(leaseKey, expires);
	}
	public void assignLeaseOwner(String leaseKey, String owner) {
		leaseManager.assignLeaseOwner(leaseKey, owner);
	}
	public int renewLeaseForOwner(String owner, int timeoutSeconds) {
		return leaseManager.renewLeaseForOwner(owner, timeoutSeconds);
	}
	public void cancelLease(String leaseKey) {
		leaseManager.cancelLease(leaseKey);
	}
	public EventHandler getEventHandler() {
		return dataSpace.getEventHandler();
	}
	public boolean containsKey(String key) {
		return dataSpace.containsKey(key);
	}
	public void addPeer(URI uri) {
		if (this.peerSenderListener != null && this.peerSenderListener.addPeer(uri)) {
			LOGGER.info(this.replicationUri + " Added Peer:" + uri);
		    dataSpace.addPeer(uri);
		    clusterEventSpace.addPeer(uri);
        }
	}

    public void removePeer(URI uri){
        if (peerSenderListener != null){
            peerSenderListener.removePeer(uri);
        }
        dataSpace.removePeer(uri);
        clusterEventSpace.removePeer(uri);
    }


    public Map<String, String> exportData() {
        return dataSpace.exportData();
    }

    public void importData(Map<String, String> data, boolean merge, boolean overwrite)
    {
        dataSpace.importData(data, merge, overwrite);
    }
    public int size() {
		return dataSpace.size();
	}
	
	public void addLifeCycleListener(LifeCycle listener) {
		this.lifeCycleManager.addLifeCycleListener(listener);
	}
	public void start() {
		this.lifeCycleManager.start();
	}
	public void stop() {
		this.lifeCycleManager.stop();
	}
	public void addPeerListener(PeerSender peerSenderListener) {
		this.peerSenderListener = peerSenderListener;
	}
	public void setReplicationURI(URI replicationUri) {
		this.replicationUri = replicationUri;
	}
	public URI getReplicationURI() {
		return this.replicationUri;
	}
	
}
