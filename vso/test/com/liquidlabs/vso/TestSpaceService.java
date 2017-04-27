package com.liquidlabs.vso;

import java.util.List;

import com.liquidlabs.common.net.URI;

public interface TestSpaceService {

	String doStuff();

	URI getReplicationURI();

	void putStuff(ClientTestData clientTestData);

	List<ClientTestData> getStuff();

}
