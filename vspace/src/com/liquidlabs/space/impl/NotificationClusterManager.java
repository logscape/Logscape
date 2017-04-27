package com.liquidlabs.space.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.space.Space;
import com.liquidlabs.space.lease.LeaseManager;
import com.liquidlabs.space.notify.EventHandler;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.ObjectTranslator;

/**
 * Ensures notification events (which are written into the LeaseMgmtSpace) are sent to peers.
 * Receives incoming notification register events.
 *
 */
public class NotificationClusterManager {
	private static final Logger LOGGER = Logger.getLogger(NotificationClusterManager.class);
	public static final String ID = "ClusterRemoteNotifier";
	
	int distrLeaseTTL = Integer.getInteger("vscape.distr.lease.event.ttl", 10);
	private final Space dataSpace;
	private final Space eventMgmtSpace;
	private final String sourceId;
	ObjectTranslator query = new ObjectTranslator();
	private final ProxyFactory proxyFactory;
	private final String partitionName;
	private Map<String, Map<String, String>> sentEventIds;
	private ThreadPoolExecutor executor;
	
	public NotificationClusterManager(Space dataSpace, Space mgmtSpace, LeaseManager leaseManager, ProxyFactory proxyFactory, String partitionName, String sourceId) {
		this.sourceId = sourceId;
		this.dataSpace = dataSpace;
		this.eventMgmtSpace = mgmtSpace;
		this.proxyFactory = proxyFactory;
		this.partitionName = partitionName;
		this.executor = (ThreadPoolExecutor) com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("remote", partitionName + "-REM");
		
		// Receive RemoteNotifySubscriber is received so it can be wired in
		this.eventMgmtSpace.notify(new String[0], new String[] { "all:" },
				new IncomingRemoteNotifierSetup(this.sourceId), new Type[] { Type.READ, Type.WRITE, Type.UPDATE, Type.TAKE }, -1);

		sentEventIds = new ConcurrentHashMap<String, Map<String,String>>();		
	}
	
	
	/**
	 * Send RemoteNotifySubscriberInfo - local notifier interest and pass to remote
	 */
	public void notify(String[] keys, String[] templates, String listenerId, Type[] eventMask, long expires) {
		RemoteNotifySubscriberInfo notifySubInfo = new RemoteNotifySubscriberInfo(listenerId, keys, templates, eventMask, expires, partitionName+ID, sourceId);
		
		if (LOGGER.isDebugEnabled()) LOGGER.debug(" ** " + this.sourceId + " ****** Send:" + notifySubInfo + " source:" + sourceId);
		
		// write it to the mgmt space to replication peers can discover the incoming event
		// and reconfigure to pass out the related event
		Event event = new Event(notifySubInfo.listenerId, notifySubInfo.listenerId, query.getStringFromObject(notifySubInfo), Type.READ);
		event.setSource(sourceId);
		eventMgmtSpace.write(notifySubInfo.id, event.toString(), expires);
//		if (LOGGER.isDebugEnabled()) LOGGER.debug(getClass().getName() + " - " + sourceURI + " Registering :" + notifySubInfo.getId());
	}
	
	public  class RemoteNotifierProxy implements EventListener {
		private final EventHandler remoteEventHandler;
		private final RemoteNotifySubscriberInfo subInfo;
		boolean disabled = false;
		long startTime = DateTimeUtils.currentTimeMillis();
		boolean allowDisabled = false;
		
		public RemoteNotifierProxy(RemoteNotifySubscriberInfo subInfo, EventHandler eventHandler) {
			this.subInfo = subInfo;
			this.remoteEventHandler = eventHandler;
		}
		public String getId() {
			return getClass().getSimpleName()+"-"+subInfo.notifyURI+subInfo.listenerId+startTime;
		}
		/**
		 * Receive an event that needs to go over the wire
		 */
		long lastError = 0;
		int failCount = 0;
		public void notify(final Event event) {
			executor.execute(new Runnable() {
				public void run() {
					remoteNotify(event);
				}
			});
		}
		public void remoteNotify(Event event) {
			if (disabled) {
				if (LOGGER.isDebugEnabled()) LOGGER.debug("** Disabled **");
				return;
			}
			// time based disable - this might cause events to be dropped though...
			if (lastError > System.currentTimeMillis() - 60 * 1000) {
				LOGGER.warn("------------- TimeDisabled:" + subInfo.listenerId);
				return;
			}
			
			// only dispatch OUR events to others so we dont get recursive loopbacks
			if (!event.getSourceURI().equals(NotificationClusterManager.this.sourceId)){
				//if (LOGGER.isDebugEnabled()) LOGGER.debug(subInfo.notifyURI + " Ignoring ourselves source["+subInfo.notifyURI+"] eventSource[" + event.getSourceURI() + "]");
				return;
			}

			try {
				// prevent the same event from being sent to the same end-point > 1
				if (sentEventIds.get(subInfo.notifyURI).containsKey(event.getId())) return;
				sentEventIds.get(subInfo.notifyURI).put(event.getId(), event.getId());
				
				if (LOGGER.isDebugEnabled()) 
					LOGGER.debug(subInfo.notifyURI + " >>>>>>>>>>> Sending Event:" + event.getId() + " to:" + subInfo.notifyURI);
				remoteEventHandler.handleEvent(event);
				failCount = 0;
			} catch (Throwable t) {
				lastError = System.currentTimeMillis();
				failCount++;
				if (allowDisabled) {
					LOGGER.warn(" ********************* Failed to execute RemoteNotify, disabling notifier:" + t.getMessage() + " id:" + this.hashCode() + " Remote:" + remoteEventHandler, t);
					disabled = true;
				} else {
					LOGGER.warn("NotifyClusterFailed:" + subInfo.listenerId + " ex"+ t.getMessage() + " failCount:" + failCount);
				}
				throw new RuntimeException(t);
			}
		}
	}

	/**
	 * Take "all:" incoming events set up a remoteNotifier - from (!ourSource) and pass them into the dataSpace
	 * i.e. this.dataSpaceEventHandler.notify(new String[0], new String[]{ "all:" }, new MainSpaceToLeaseSpaceNotifier(), new Type[] { Type.READ, Type.WRITE, Type.UPDATE, Type.TAKE }, -1);
	 */
	public class IncomingRemoteNotifierSetup implements EventListener {

		private final String sourceURI2;
		public IncomingRemoteNotifierSetup(String sourceURI) {
			sourceURI2 = sourceURI;
		}
		public String getId() {
			return getClass().getName();
		}
		public void notify(Event event) {
			// dont take our own events from the event space and pass to dataspace
			if (event.getSourceURI().equals(NotificationClusterManager.this.sourceId)) {
//				if (LOGGER.isDebugEnabled()) LOGGER.debug(NotificationClusterManager.this.sourceId + " ****** Ignore Local EVENT:" + event);
//				LOGGER.warn(NotificationClusterManager.this.sourceId + " ****** Ignore Local EVENT:" + event);				
				return;
			}
			if (LOGGER.isDebugEnabled()) LOGGER.debug(NotificationClusterManager.this.sourceId + " GOT EVENT:" + event);
			
			
			// We have RemoteNotifySubscriberInfo object
			if (event.getValue().startsWith(RemoteNotifySubscriberInfo.class.getName())) {
				RemoteNotifySubscriberInfo notifySubInfo = query.getObjectFromFormat(RemoteNotifySubscriberInfo.class, event.getValue());
				
				if (LOGGER.isDebugEnabled()) LOGGER.debug(NotificationClusterManager.this.sourceId + " ***** Received Remote NotifySubInfo:" + notifySubInfo);
				
				if (notifySubInfo == null) {
					LOGGER.warn("Cannot process:" + event.toString() + " NotifyInfo cannot be adapted from:" + event.getValue());
					throw new RuntimeException("Got NULL object from:" + event.getKey() + "\nvalue:" + event.getValue());
				}
				
				sentEventIds.put(notifySubInfo.notifyURI, Collections.synchronizedMap(new LinkedHashMap<String, String>() {
					private static final long serialVersionUID = 1L;
					protected boolean removeEldestEntry(Entry<String, String> eldest) {
						return (this.size() > 100);
					};
				}));

				if (proxyFactory != null) {
					
					if (LOGGER.isDebugEnabled()) LOGGER.debug(sourceURI2 + " ** Register -:" + notifySubInfo.notifyURI + " pf:" + proxyFactory.getAddress());
					// add our own event listener to make sure we pass these events through the cluster
					EventHandler remoteEventHandler = proxyFactory.getRemoteService(notifySubInfo.remoteListenerId, EventHandler.class, notifySubInfo.notifyURI);
					dataSpace.notify(notifySubInfo.keys, notifySubInfo.templates, 
									new RemoteNotifierProxy(notifySubInfo, remoteEventHandler),	notifySubInfo.eventMask, notifySubInfo.expires);
				} else {
					LOGGER.warn("No ProxyFactory FOUND - Cluster notifications cannot work");
				}
			}
			
		}
	}

	public static class RemoteNotifySubscriberInfo {
		String remoteListenerId;
		String notifyURI;
		String[] keys;
		String[] templates;
		String listenerId;
		String id;
		Type[] eventMask;
		long expires;
		
		public RemoteNotifySubscriberInfo() {
		}
		public String getId() {
			return id;
		}
		public RemoteNotifySubscriberInfo(String eventListenerId, String[] keys, String[] templates, Type[] eventMask, long expires, String remoteListenerId, String notifyURI) {
			this.eventMask = eventMask;
			this.expires = expires;
			this.remoteListenerId = remoteListenerId;
			this.id = (eventListenerId + Arrays.toString(keys).hashCode());
			
			this.keys = keys;
			this.templates = templates;
			this.listenerId = eventListenerId;
			this.notifyURI = notifyURI;
		}
		public String toString() {
			return this.getClass().getSimpleName() + " id:" + id + " listenerId:" + listenerId + " keys" + Arrays.toString(keys) + " templates" + Arrays.toString(templates) + " Events:" + Arrays.toString(eventMask);
		}
	}
}
