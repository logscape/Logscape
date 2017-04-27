package com.liquidlabs.transport.tcp;

import com.liquidlabs.common.net.URI;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.EndPointFactory;
import com.liquidlabs.transport.Receiver;

public class TcpEndPointFactory implements EndPointFactory {
	
	private final ExecutorService executor;
	ArrayList<EndPoint> endPoints = new ArrayList<EndPoint>();

	public TcpEndPointFactory(ExecutorService executor) {
		this.executor = executor;
	}

	public EndPoint getEndPoint(URI uri, Receiver receiver) {
		TcpEndPoint tcpEndPoint = new TcpEndPoint(uri,receiver, executor);
		tcpEndPoint.start();
		this.endPoints.add(tcpEndPoint);
		return tcpEndPoint;
	}

	public void start() {
	}

	public void stop() {
		for (EndPoint endPoint : endPoints) {
			endPoint.stop();
			
		}
	}

}
