package com.liquidlabs.vso;

import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Logger;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyClient;


public class SpaceServiceAddressListener implements AddressUpdater {

    private Logger LOGGER = Logger.getLogger(AddressUpdater.class);

    private String name;
    private transient SpaceService spaceService;
    String id;
    String replAddress;
    String svc = "";

    public SpaceServiceAddressListener() {
	}
    public SpaceServiceAddressListener(String name) {
        this.name = name;
        id = getClass().getSimpleName() + name + UID.getSimpleUID(name);
    }

    public void setSpaceService(SpaceService spaceService) {
        this.spaceService = spaceService;
        setReplAddress(spaceService.getReplicationURI());
    }
	void setReplAddress(URI address) {
		this.replAddress = address.toString();
        String param = com.liquidlabs.common.net.URIUtil.getParam("svc", address);
		if (param != null) svc = param;
	}


    public void updateEndpoint(String address, String replicationAddress) {
        if (replicationAddress != null && replicationAddress.length() > 0) {
            try {
                URI uri = new URI(replicationAddress);
                if (isSameSvc(uri)) {
                	spaceService.addPeer(uri);
                	LOGGER.info(this.replAddress + " Update Endpoint:" + address + " : " + replicationAddress);
                } else {
                	LOGGER.info(this.replAddress + " IGNORE Endpoint:" + address + " : " + replicationAddress);
                }
				
            } catch (Exception e) {
                // log it
            	e.printStackTrace();
            }
        }
    }



	public void removeEndPoint(String address, String replicationAddress) {
        if (replicationAddress != null) {
            try {
            	receivedURIs.remove(replicationAddress);
                URI uri = new URI(replicationAddress);
                if (isSameSvc(uri)) {
	                LOGGER.info(this.replAddress + " Remove  Endpoint:" + address + " : " + replicationAddress);
					spaceService.removePeer(uri);
                }
            } catch (URISyntaxException e) {
                //
            }
        }
    }

	Set<String> receivedURIs = new CopyOnWriteArraySet<String>();
    public void syncEndPoints(String[] addresses, String[] replicationLocations) {
        for (String repl : replicationLocations) {
            try {
            	if (receivedURIs.contains(repl)) continue;
            	URI uri = new URI(repl);
            	if (isSameSvc(uri)) {
            		LOGGER.info(this.replAddress + " Sync Endpoint:" + repl);
            		spaceService.addPeer(uri);
            	} else {
            		LOGGER.debug(this.replAddress + " IGNORE Sync Endpoint:" + repl);
            	}
            	receivedURIs.add(repl);
            } catch (URISyntaxException e) {
                //
            }
        }
    }

    public String getId() {
        return id;
    }

    public void setProxyClient(ProxyClient<?> client) {
    }

	public void setId(String clientId) {
	}
    boolean isSameSvc(URI uri) {
    	String param = com.liquidlabs.common.net.URIUtil.getParam("svc", uri);
		if (param != null) {
			if (param.equals(svc)) return true;
		}
		return false;
	}
}
