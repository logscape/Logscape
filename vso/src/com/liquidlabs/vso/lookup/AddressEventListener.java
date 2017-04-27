/**
 * 
 */
package com.liquidlabs.vso.lookup;

import org.apache.log4j.Logger;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.vso.VSOProperties;

/**
 * Listens to service address updates and sends them back to the client
 * @author neil
 *
 */
public class AddressEventListener implements AddressUpdater {
	
	private static final Logger LOGGER = Logger.getLogger(AddressEventListener.class);

	private String name;
	private String address;
	private transient ProxyClient<?> client;
	// must use EPSpecific ID (i.e. contains the address etc)
	private String id;
	
	public AddressEventListener() {}
	
	public AddressEventListener(String name, String context) {
		this.name = name;
		this.id = name + "_" + context + UID.getSimpleUID(context);
	}

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}

	public void setProxyClient(ProxyClient<?> client) {
		this.client = client;
//		// must use UUID cause there can be multiple listeners per process - so its per ProxyClient
//		this.id = client.getId();
		if (client == null) throw new RuntimeException("Cannot use null ProxyClient");
	    this.address = client.getClientAddress().toString();
	}

	public void updateEndpoint(String address, String replicationAddress) {
		try {
			if (VSOProperties.isVERBOSELookups()) {
				LOGGER.info(VSOProperties.HA_TAG + this.id + ".UpdateEndpoint("  + address + ")" + " client:" + client);
			}
			client.refreshAddresses(address);
		} catch (Throwable t) {
			LOGGER.error("Failed to updateEndpoint:" + address, t);
		}
	}
	public void removeEndPoint(String address, String replicationAddress) {
		client.removeAddress(address);
	}
	
	public void syncEndPoints(String[] addresses, String[] replicationLocations) {
		try {
			if (VSOProperties.isVERBOSELookups()) {
				LOGGER.info(VSOProperties.HA_TAG + this.id + ".SyncEndPoints"  + Arrays.toString(addresses) + " client:" + client);
			}
			client.syncEndpoints(addresses, replicationLocations);
		} catch (Throwable t) {
			String msg = "Fail to update Client:" + Arrays.toString(addresses);
			LOGGER.warn(msg, t);
			throw new RuntimeException(msg);
		}
	}
	
}