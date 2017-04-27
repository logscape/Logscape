package com.liquidlabs.transport.proxy;

import java.util.HashMap;
import java.util.List;

import com.liquidlabs.transport.protocol.UDP;
import com.liquidlabs.transport.proxy.DummyServiceImpl.UserType;
import com.liquidlabs.transport.proxy.clientHandlers.Cacheable;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;

public interface DummyService extends Remotable {
	
	
	Number passANumber(Number arg);
	
	void noArgs();
	void oneWay(String arg);
	void oneWayWithEx(String string) throws Exception;
	
	String twoWay(String arg);
	
	@Cacheable(ttl=1)
	String twoWayCached(String arg);
	@Cacheable(ttl=1)
	String twoWayCached();
	
	String[] twoWayWithURIAddressArray(String arg);
	String twoWayWithPause(String string, long l);
	String twoWayWithPauseTWO(String payload, long l);
	
	List<String> doListOfStrings(int size);
	
	UserType doCustomUserType(UserType userType);
	
	int doThrowException();
	void makeCallbackHappend(String string);
	String notify(String[] keys, String[] templates, EventListener listener, Type[] eventMask, long expires);
	String[] doEmptyStringArray();
	
	@ReplayOnAddressChange
	void twoWayWithPauseAndReplay(String string, int i) throws Exception;
	
	void doCustomUserTypeList(List<UserType> arrayList, int i);

	void callbackOnMe(RemoteClientCallback clientObject, String payload);

	void callBackOnMeXtimes(NotifyInterface remoteable, int times);

	@ReplayOnAddressChange
	void oneWayWithReplay(String string) throws Exception;

	HashMap<String, String> passAMap(HashMap<String, String> map);

	byte[] getBytes();

	@UDP
	void udpCall(String stuff);

	byte[] getBytesWithException();

	void registerCallback(EventListener remoteEventListener1);

	void callback();



}
