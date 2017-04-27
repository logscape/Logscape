package com.liquidlabs.transport.proxy;

import java.util.ArrayList;
import java.util.List;

public class RemoteClientCallbackImpl implements RemoteClientCallback {
	
	List<String> payloads = new ArrayList<String>();
	int callCount;
	private String endpointAddress;
	private String id;
	
	public RemoteClientCallbackImpl() {
	}
	public RemoteClientCallbackImpl(String id, String endpointAddress) {
		this.id = id;
		this.endpointAddress = endpointAddress;
	}
	public String getId() {
		return id;
	}

	public void callback(String payload) {
		System.out.println("CalledBack:" + payload);
		callCount++;
		payloads.add(payload);
	}
	
}
