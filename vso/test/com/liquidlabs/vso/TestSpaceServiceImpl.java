package com.liquidlabs.vso;

import java.util.List;
import java.util.concurrent.Executors;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.ORMapperFactory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.lookup.LookupSpace;

public class TestSpaceServiceImpl implements TestSpaceService, LifeCycle {

	String NAME = TestSpaceServiceImpl.class.getSimpleName();
	SpaceService spaceOne;
	private ORMapperFactory mapperFactory;
	private final LookupSpace lookup;
	
	public TestSpaceServiceImpl(LookupSpace lookup, int port) {
		this.lookup = lookup;
		this.mapperFactory = new ORMapperFactory(port, NAME, port);
		
		spaceOne = new SpaceServiceImpl(lookup, mapperFactory, NAME, Executors.newScheduledThreadPool(10), true, false, true);
	}
	
	
	public void putStuff(ClientTestData clientTestData) {
		spaceOne.store(clientTestData, -1);
	}
	public List<ClientTestData> getStuff() {
		return spaceOne.findObjects(ClientTestData.class, "", false, -1);
	}
	public String doStuff() {
		return "Stuff";
	}
	public void start() {
		spaceOne.start();
		spaceOne.start(this, "test-1.0");
		
	}

	public void stop() {
		spaceOne.stop();
		mapperFactory.stop();
	}

	public URI getReplicationURI() {
		return this.spaceOne.getReplicationURI();
	}
	public Object export() {
		return this.spaceOne.exportObjects("");
	}

	public TestSpaceService getClient(ProxyFactory proxyFactory) {
		return SpaceServiceImpl.getRemoteService("TestClient", TestSpaceService.class, lookup, proxyFactory, NAME, true, false);
	}
}
