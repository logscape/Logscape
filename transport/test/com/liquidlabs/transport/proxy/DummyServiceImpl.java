package com.liquidlabs.transport.proxy;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;
import com.liquidlabs.transport.proxy.events.Event.Type;

public class DummyServiceImpl implements DummyService {
	
	TransportFactory transportFactory = new TransportFactoryImpl(Executors.newFixedThreadPool(5), "dummy");
	private static final Logger LOGGER = Logger.getLogger(DummyServiceImpl.class);

	public static int callCount = 0;
	static int callCountWReplay = 0;
	public int instanceCallCount = 0;
    public CountDownLatch instanceLatch = new CountDownLatch(0);
	private String name = "namelessService";
	public static boolean verbose = true;
    private List callList = new CopyOnWriteArrayList<String>();
    public static CountDownLatch latch = new CountDownLatch(10);

    public DummyServiceImpl(String name) {
		this.name = name;
	}
    public String getId() {
    	return name;
    }
	public DummyServiceImpl() {
	}

    public DummyServiceImpl(List list) {
        callList = list;
    }
	
	public void udpCall(String stuff) {
        count();
		if (verbose) System.out.println(new Date() +" UDP Got:" + stuff);
	}
	public byte[] getBytes() {
		return "GotBytes".getBytes();
	}
	public byte[] getBytesWithException() {
		throw new RuntimeException("boom she goes");
	}
	public Number passANumber(Number arg) {
		count();
		return arg;
	}
	public void noArgs() {
		System.out.println("noArgs called!");
		count();
	}
	public void oneWay(String arg) {
		if (verbose) System.out.println(Thread.currentThread().getName() + " oneway called!");
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
		}
		count();
	}
	public void oneWayWithEx(String string) {
		System.out.println("oneway called!");
		count();
	}
	public String twoWay(String arg) {
		if (verbose) System.out.println("twoway called!");
		count();
		return "twoWayResult" + arg;
	}
	public String twoWayCached(String arg) {
		if (verbose) System.out.println("twoway Cached called!");
		count();
		return "twoWayResult" + arg + ":" + callCount;
	}
	public String twoWayCached() {
		if (verbose) System.out.println("twoway Cached called!");
		count();
		return "twoWayResult" + ":" + callCount;
	}


	public void doCustomUserTypeList(List<UserType> arrayList, int i) {
        count();
	}

    private void count() {
        callCount++;
        latch.countDown();
        instanceLatch.countDown();
        instanceCallCount++;
    }

    public UserType doCustomUserType(UserType userType){
		System.out.println("doCustomUserType called!:" + userType.someValue);
		if (userType == null) throw new RuntimeException("boom! - no userType passed!");
		count();
		return userType;
	}
	
	public String[] doEmptyStringArray(){
		return new String[0];
	}
	public List<String> doListOfStrings(int size){
		ArrayList<String> result = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			result.add("item:" + i);
		}
		return result;
	}
	public int doThrowException(){
		System.out.println("doThrowException");
		count();
		throw new RuntimeException("boom! - deliberate exception!");
	}
	public void makeCallbackHappend(final String endPoint) {
		
		
		LOGGER.info("\t\t -- Server Received:" + new DateTime());
		Thread thread = new Thread(){
			@Override
			public void run() {
				
				try {
					ProxyFactoryImpl proxyFactory = new ProxyFactoryImpl(transportFactory, TransportFactoryImpl.getDefaultProtocolURI("", "localhost", Config.TEST_PORT + 100, "dummy"), Executors.newCachedThreadPool(), "ds");
					proxyFactory.start();
					NotifyInterface remoteService = proxyFactory.getRemoteService("notifyId", NotifyInterface.class, new String[] { endPoint });
					for (int i = 0; i < 10; i++) {
						System.out.println(">>>Calling notify:" + i);
						remoteService.notify("payloadFromRemote");
						System.out.println("<<<done:" + i);
					}
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
		};
		count();
		thread.start();
		try {
			Thread.sleep(5000);
			thread.join();
		} catch (InterruptedException e) {
		}
	}
	
	public String[] twoWayWithURIAddressArray(String arg) {
		return new String[] { "tcp://alteredcarbon.local:11005" };
	}
	
	public String twoWayWithPause(String argument, long sleep) {
		try {
            callList.add(argument);
			System.out.println(new DateTime() + " PAUSE:" + name + " " + Thread.currentThread().getName() + " ONE -- received call:" + argument);
			instanceCallCount++;
            count();
			Thread.sleep(sleep);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return argument;
	}
	public String twoWayWithPauseTWO(String argument, long sleep) {
		try {
			//System.out.println(new DateTime() + " PAUSE2:" + name + " " + Thread.currentThread().getName() + " TWO -- received call:" + argument);
			instanceCallCount++;
			count();
			Thread.sleep(sleep);
		} catch (InterruptedException e) {
		}
		return argument;
	}
	
	public void twoWayWithPauseAndReplay(String argument, int sleep) throws Exception {
		try {
			System.out.println(name + " -- received call:" + argument);
			count();
			callCountWReplay++;
			Thread.sleep(sleep);
		} catch (InterruptedException e) {
		}
	}
	/**
	 * Fake a space notification registration...just want to prove proxy objects work!
	 * @return 
	 */
	public String notify(String[] keys, String[] templates, final EventListener remoteListener, Type[] eventMask, long expires) {
		try {
			Thread thread = new Thread(){
				@Override
				public void run() {
					try {
						for (int i = 0; i < 10; i++) {
							System.out.println("Notify  " + i + " asking for remoteListener.getId:" + remoteListener);
							String id = remoteListener.getId();
							System.out.println("Notify  " + i + " thread got Id:" + id);
							if (!id.equals("myListenerId")) throw new RuntimeException("GotTheWrongID:" + id);
							remoteListener.notify(new Event("eventID", "key", "value", Type.READ));
						}
					} catch (Throwable t){
						t.printStackTrace();
					}
				}
			};
			thread.start();
			thread.join();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return "blah";
	}
	
	public static class UserType {
		public String someValue;
		public int someInt;
	}
	public void callbackOnMe(RemoteClientCallback clientObject, String payload) {
		clientObject.callback(payload);
	}
	public void callBackOnMeXtimes(final NotifyInterface remoteable, final int times) {
		Thread thread = new Thread() {
			@Override
			public void run() {
				try {
					for (int i = 0; i < times; i++) {
						System.out.println("Remote - sending:" + i);
						remoteable.notify(i + " payload!!");
						Thread.sleep(100);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		thread.start();
	}
	
	public List<String> oneWaysReceived = new ArrayList<String>();
	public void oneWayWithReplay(String string) {
		System.out.println(" received:" + string + " callcount:" + callCount++);
		oneWaysReceived.add(string);
		if (callCount == 3) {
			System.out.println(" =========== sleeping ===============");
			try {
				Thread.sleep(20 * 1000);
			} catch (InterruptedException e) {
			}
			System.out.println(" =========== done sleeping ===============");
		}
		
	}
	public HashMap<String, String> passAMap(HashMap<String, String> map) {
		return map;
	}
	Set<EventListener> callbacks = new HashSet<EventListener>();
	public void registerCallback(EventListener remoteEventListener1) {
		callbacks.add(remoteEventListener1);
	}
	public void callback() {
		for (EventListener listener : callbacks) {
			listener.notify(new Event("eventID", "key", "value", Type.READ));
			
		}
	}
}
