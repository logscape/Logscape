package com.liquidlabs.log.space;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.liquidlabs.common.UID;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.resource.ResourceGroup;
import com.liquidlabs.vso.resource.ResourceGroupListener;

public class ResourceGroupListenerImpl implements ResourceGroupListener {
	
	private static final Logger LOGGER = Logger.getLogger(ResourceGroupListener.class);
	transient private LogSpace logSpace;
	private String id;
	
	Set<ResourceGroup> changes = new CopyOnWriteArraySet<ResourceGroup>();
	
	public ResourceGroupListenerImpl(LogSpace logSpace, ScheduledExecutorService scheduler) {
		this.logSpace = logSpace;
		this.id = getClass().getSimpleName() + UID.getUUIDWithHostNameAndTime();
		scheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {
				try {
					pumpChanges();
				} catch (Throwable t) {
					LOGGER.warn("Failed to apply Group Updates", t);
				}
			}
		}, 1, 1, TimeUnit.MINUTES);
	}
	protected void pumpChanges() {
		HashSet<ResourceGroup> removed = new HashSet<ResourceGroup>();
		for (ResourceGroup resourceGroup : this.changes) {
			logSpace.resourceGroupUpdated(Type.UPDATE, resourceGroup);
			removed.add(resourceGroup);
		}
		this.changes.removeAll(removed);
	}
	public ResourceGroupListenerImpl() {
	}
	
	public String getId() {
		return id;
	}
	public void resourceGroupUpdated(Type event, ResourceGroup result) {
		changes.add(result);
	}

}
