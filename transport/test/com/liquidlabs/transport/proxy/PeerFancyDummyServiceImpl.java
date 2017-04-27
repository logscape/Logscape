package com.liquidlabs.transport.proxy;

import java.util.ArrayList;


public class PeerFancyDummyServiceImpl implements PeerFancyDummyService {
	
	ArrayList<String> received = new ArrayList<String>();
	public int callCount;
	public static int globalCallCount;
	private String instanceId;
	
	public PeerFancyDummyServiceImpl(String instanceId) {
		this.instanceId = instanceId;
		globalCallCount = 0;
	}
	

	public void broadcast(String string) throws Exception {
		System.out.println(instanceId + " >>>>>>>>>>>>>> Broadcast:" + toString() + "****** received call:" + string);
		received.add(string);
		globalCallCount++;
		callCount++;
		if (globalCallCount == 1 && string.equals("throwException")) throw new RetryInvocationException("boom!!");
	}
	
	public void broadCastAndDisable(String string) throws Exception {
		System.out.println(toString() + " BroadcastAndDisable ****** received call:" + string);
		received.add(string);
		globalCallCount++;
		if (globalCallCount == 1 && string.equals("throwException")) {
			System.out.println(" XXXXXXXXXXXXXXXXXXXXXXXXXXXXX Exception XXXXXXXXXXXXXXXXXXXXXXX");
			throw new RuntimeException("boom!!");
		}
		callCount++;
		
	}
	public void shouldRoundRobinWithFactor(String string) throws Exception {
		System.out.println("RoundRobin:" + toString() + "****** received roundRobin:" + string);
		callCount++;
		received.add(string);
		if (string.equals("throwException")) throw new RuntimeException("boom!!");
	}

    public int hashCallReceived;

    @Override
    public void sendHashableParamMessage(String firstParamForHash) throws Exception {
        System.out.println("GOT:" + firstParamForHash);
        received.add(firstParamForHash);
        hashCallReceived++;
    }


	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[PeerSERVICE:");
		buffer.append(" received:");
		buffer.append(received);
		buffer.append(" calls:");
		buffer.append(callCount);
		buffer.append(" instanceId:");
		buffer.append(instanceId);
        buffer.append(" hashCalls:");
        buffer.append(hashCallReceived);

        buffer.append("]");
		return buffer.toString();
	}
	
	
}
