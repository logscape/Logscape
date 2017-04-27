package com.liquidlabs.transport.proxy;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

public class ProxyFactoryAdminImpl implements ProxyFactoryAdminImplMBean {
	private final static Logger LOGGER = Logger.getLogger(ProxyFactoryAdminImpl.class);

	private final ProxyFactoryImpl proxyFactory;
	
	public String address;
	public String stringId;
	
	public String getAddress() {
		return address;
	}

	public ProxyFactoryAdminImpl(ProxyFactoryImpl proxyFactoryImpl) {
		this.proxyFactory = proxyFactoryImpl;
		
		try {
			MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName = new ObjectName("com.liquidlabs.transport:ProxyFactory=" + proxyFactoryImpl.hashCode());
			if (mbeanServer.isRegistered(objectName)) return;
			mbeanServer.registerMBean(this, objectName);
			this.address = proxyFactory.getAddress().toString();
			this.stringId = proxyFactoryImpl.toString();
			
		} catch (Exception e) {
			LOGGER.error(e);
		}
	}
	public String listProxyClients() {
		Map<String, ProxyClient<?>> clients = proxyFactory.clients;
		StringBuilder results = new StringBuilder();
		int count = 1;
		for (String clientId : clients.keySet()) {
			ProxyClient<?> proxyClient = clients.get(clientId);
			results.append(count++ + ") ").append(proxyClient.toString()).append("<br><br>");
		}
		return results.toString();
	}
	public String listEndPoints() {
		PeerHandler peerHandler = proxyFactory.getPeerHandler();
		StringBuilder results = new StringBuilder();
		if (peerHandler != null) {
			List<String> eps = peerHandler.printEndPoints();
			int count = 0;
			for (String string : eps) {
				results.append(count++).append(") ").append(string).append("<br><br>");
			}
		}
		return results.toString();
	}
}
