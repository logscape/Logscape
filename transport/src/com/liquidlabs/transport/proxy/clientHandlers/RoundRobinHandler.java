package com.liquidlabs.transport.proxy.clientHandlers;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

import com.liquidlabs.transport.proxy.ProxyCaller;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import javolution.util.FastMap;

import org.apache.log4j.Logger;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.addressing.AddressHandler;

/**
 * Will roundRobin to all recipients using the given annotation factor for repeat bursts
 *
 */
public class RoundRobinHandler implements ClientHandler {

	Map<Method, State> state = new FastMap<Method, State>().shared();
	private static final Logger LOGGER = Logger.getLogger(RoundRobinHandler.class);
	int errors = 0;

	public static class State {
		public State(URI endPointURI) {
			this.currentURI = endPointURI;
		}
		public int currentCallCount;
		public URI currentURI;
	}

    @RoundRobin(factor = 1)
    public void sample() {
    }

    @Override
    public Object invoke(ProxyCaller client, AddressHandler addressHandler, Method method, Object[] args) throws InterruptedException {
		if (!state.containsKey(method)) {
			state.put(method, new State(addressHandler.getEndPointURISafe()));
		}
		State methodState = state.get(method);
		RoundRobin rrobin = method.getAnnotation(RoundRobin.class);
		
		
		// Use local stack to prevent concurrency problems with currentURI
		URI currentURI = methodState.currentURI;
		
//		LOGGER.warn(methodState.currentURI + " method:" + method.getName() + " count:" + methodState.currentCallCount);
		
		if (methodState.currentCallCount % rrobin.factor() == 0 && methodState.currentCallCount > 0) {
			Collection<URI> endPointURIs = addressHandler.getEndPointURIs();
			currentURI = getNextURI(currentURI, endPointURIs);
//		LOGGER.warn(methodState.currentURI + "^^^^^ROTATE^^^ method:" + method.getName() + " count:" + methodState.currentCallCount);
		}
		
		int attempts = 0;
		while (attempts++ < 5 && currentURI != null) {
			try {
				methodState.currentCallCount++;
				Object clientExecute = client.clientExecute(method, currentURI, args, false, -1);
				if (attempts > 1) {
					LOGGER.info(attempts + " Success>>:" + currentURI + " ::" + addressHandler);
				}
				methodState.currentURI = currentURI;
				
				return clientExecute;
			} catch (InterruptedException e) {
				return null;
			} catch (RetryInvocationException e) {
				addressHandler.registerFailure(currentURI);
				LOGGER.info(attempts + " Retrying>> " + currentURI +" EPS:" + addressHandler.getEndPointURIs());
				currentURI = addressHandler.getEndPointURISafe();
			} catch (Exception e) {
				if (errors < 10) {
					// dont do a toString on the Args - we may cause concurrent modification ex
					LOGGER.warn(method.toString() + " A:" + e.toString(), e);
				} else {
					LOGGER.warn(method.toString() + " A:" + e.toString());
				}
				if (errors > 1000) errors = 0;
			}
		}
		LOGGER.info(attempts + " Failed>>");
				
			
		return null;
	}
	private void pause() throws InterruptedException {
		Thread.sleep(50 + (long)(Math.random() * 100));
	}
	URI getNextURI(URI currentURI, Collection<URI> endPointURIs) {
		boolean found = false;
		if (endPointURIs.size() == 0) return currentURI;
		if (endPointURIs.size() == 1) return endPointURIs.iterator().next();
		URI firstURI = endPointURIs.iterator().next();
		for (URI uri : endPointURIs) {
			if (found) return uri;
			if (uri.equals(currentURI)) found = true;
		}
		// fell off the end of the list
		return firstURI;
	}

}
