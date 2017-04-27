package com.liquidlabs.transport.proxy;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl.ADDRESSING;
import com.liquidlabs.transport.proxy.addressing.AddressHandler.RefreshAddrs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public interface ProxyFactory extends LifeCycle {

	<T> T getRemoteService(String listenerId, Class<T> interfaceClass, String... endPoints);
	
	<T> T getRemoteService(String listenerId, Class<T> interfaceClass, ADDRESSING addressing, String... endPoints);

	<T> T getRemoteService(String listenerId, Class<T> interfaceClass, String[] endPoints, AddressUpdater updater);

	<T> T getRemoteService(String listenerId, Class<T> interfaceClass, String[] endPoints, AddressUpdater updater, RefreshAddrs updateTask );
	
	void registerContinuousEventListener(String listenerId, ContinuousEventListener eventListener);

	void registerMethodReceiver(String listenerId, Object target);

	boolean unregisterMethodReceiver(String listenerId);
	
	void publishAvailability(String listenerId);

	URI getAddress();
	URI getAddress(String path);

	EndPoint getEndPointServer();

	String getEndPoint();

	ScheduledExecutorService getScheduler();
	
	ExecutorService getExecutor();

	/**
	 * Auto Registers the userObject as a listener and allows it to be passed as invocation arguments
	 */
	<T extends Remotable> T makeRemoteable(Remotable userObject);

	boolean stopProxy(Object receiver);

	void unregisterMethodReceiver(Remotable remotable);

	void registerProxyClient(String givenId, Object object);

}
