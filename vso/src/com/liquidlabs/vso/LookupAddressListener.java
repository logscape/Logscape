package com.liquidlabs.vso;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.AddressUpdater;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.vso.lookup.LookupSpace;

import java.net.URISyntaxException;


public class LookupAddressListener implements AddressUpdater {

    private String name;
    private transient LookupSpace spaceService;

    public LookupAddressListener(String name) {
        this.name = name;
    }

    public void setSpaceService(LookupSpace spaceService) {
        this.spaceService = spaceService;
    }


    public void updateEndpoint(String address, String replicationAddress) {
        if (replicationAddress != null) {
            try {
                spaceService.addLookupPeer(new URI(replicationAddress));
            } catch (URISyntaxException e) {
            }
        }
    }

    public void removeEndPoint(String address, String replicationAddress) {
        if (replicationAddress != null) {
            try {
                spaceService.removePeer(new URI(replicationAddress));
            } catch (URISyntaxException e) {
            }
        }
    }

    public void syncEndPoints(String[] addresses, String[] replicationLocations) {
        for (String repl : replicationLocations) {
            try {
                spaceService.addLookupPeer(new URI(repl));
            } catch (URISyntaxException e) {
            }
        }
    }

    public String getId() {
        return id;
    }

    public void setProxyClient(ProxyClient<?> client) {
    }

    String id;
	public void setId(String id) {
		this.id = id;
	}
}