package com.liquidlabs.transport.proxy.addressing;

import java.util.List;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.RetryInvocationException;


/**
 * AddressHandling that handles EndPoint state and defines behaviour when certain
 * events occurr (failure, new reg, de-reg).
 *
 */
public interface AddressHandler {
	
	public static interface RefreshAddrs {
		String[] getAddresses();
	}

	
	void addEndPoints(String... endPoints);

	List<URI> getEndPointURIs();

	URI getEndPointURI() throws RetryInvocationException;
	URI getEndPointURISafe();

	void registerFailure(URI currentURI);

	void resetReplayFlag();

	boolean isReplayRequired();

	boolean remove(String address);

	boolean isEndPointAvailable();

	void syncEndpoints(String[] addresses);

	void validateEndPoints();

	void registerAddressRefresher(RefreshAddrs update);

	String replayReason();


}
