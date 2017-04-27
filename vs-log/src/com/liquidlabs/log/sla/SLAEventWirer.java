package com.liquidlabs.log.sla;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.container.SLAContainerAdminMBean;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;

public class SLAEventWirer implements Runnable {
	private static final Logger LOGGER = Logger.getLogger(SLAEventWirer.class);
	private final LookupSpace lookup;
	private final LogSpace log;
	private final String query = "interfaceName equals " + SLAContainerAdminMBean.class.getName();
	private final ProxyFactoryImpl proxyFactory;
	
	class SLAFilter {
		SLAEventWriter writer;
		String filter;
		SLAContainerAdminMBean sla;

		public SLAFilter(SLAEventWriter eventWriter, String notifyFilter, SLAContainerAdminMBean slaAdmin) {
			writer = eventWriter;
			filter = notifyFilter;
			sla = slaAdmin;
		}
	}
	
	
	private final Map<ServiceInfo, SLAFilter> slas = new HashMap<ServiceInfo, SLAFilter>();
	private final AggSpace aggSpace;
	
	public SLAEventWirer(LookupSpace lookup, LogSpace log, ProxyFactoryImpl proxyFactory, AggSpace aggSpace) {
		this.lookup = lookup;
		this.log = log;
		this.proxyFactory = proxyFactory;
		this.aggSpace = aggSpace;
	}

	
	public void run() {
		List<ServiceInfo> services = lookup.findService(query);
		registerListeners(services);
		unregisterListeners(services);
	}


	private void unregisterListeners(List<ServiceInfo> services) {
		Set<ServiceInfo> remove = new HashSet<ServiceInfo>();
		for (ServiceInfo serviceInfo : slas.keySet()) {
			if (!services.contains(serviceInfo)) {
				LOGGER.info("SLAEVENT -> Removing: " + serviceInfo.getName());
				SLAFilter filter = slas.get(serviceInfo);
				aggSpace.unregisterEventListener(filter.writer.getId());
				remove.add(serviceInfo);
			}
		}
		
		for (ServiceInfo serviceInfo : remove) {
			slas.remove(serviceInfo);
		}
	}


	private void registerListeners(List<ServiceInfo> services) {
		for (ServiceInfo serviceInfo : services) {
			try {
				if (!slas.containsKey(serviceInfo)) {
						registerNew(serviceInfo);
				} else {
					checkForFilterUpdate(serviceInfo);
				}
			} catch (Exception e) {
				LOGGER.error(e);
			}
		}
	}


	private void checkForFilterUpdate(ServiceInfo serviceInfo) throws Exception {
		SLAFilter filter = slas.get(serviceInfo);
		String notifyFilter = filter.sla.getNotifyFilter();
		if (!filter.filter.equals(notifyFilter)) {
			aggSpace.unregisterEventListener(filter.writer.getId());
			if (notifyFilter != null) {
				LOGGER.debug("SLAEVENT -> Updating: " + serviceInfo.getName());
				proxyFactory.registerMethodReceiver(filter.writer.getId(), filter.writer);
				aggSpace.registerEventListener(filter.writer, filter.writer.getId(), "message contains " + notifyFilter, 10 * 60);
				filter.filter = notifyFilter;
			} else {
				LOGGER.debug("SLAEVENT -> Removing: " + serviceInfo.getName());
				slas.remove(serviceInfo);
			}
		}
		
	}


	private void registerNew(ServiceInfo serviceInfo) throws Exception {
		try {
			SLAContainerAdminMBean slaAdmin = proxyFactory.getRemoteService(serviceInfo.getName(), SLAContainerAdminMBean.class, serviceInfo.getLocationURI());
			String notifyFilter = slaAdmin.getNotifyFilter();
			if (notifyFilter != null) {
				LOGGER.debug("SLAEVENT -> Got remote service for " + serviceInfo.getName() + ", notifyFilter = " + notifyFilter);
				LOGGER.debug("SLAEVENT -> Adding: " + serviceInfo.getName());
				SLAEventWriter eventWriter = new SLAEventWriter(proxyFactory.getEndPoint(), slaAdmin, serviceInfo.getName());
				proxyFactory.registerMethodReceiver(eventWriter.getId(), eventWriter);
				aggSpace.registerEventListener(eventWriter, eventWriter.getId(), "message contains " + notifyFilter, 10 * 60);
				slas.put(serviceInfo, new SLAFilter(eventWriter, notifyFilter, slaAdmin));
			}
		} catch (Throwable t){
			t.printStackTrace();
			LOGGER.info(t.toString());
		}
	}

}
