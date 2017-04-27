package com.liquidlabs.orm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.liquidlabs.common.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactory;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.transport.serialization.ObjectTranslator;

public class RemoteClientSideTest {
	
	private ORMapperClientImpl orClient;
	private ProxyFactoryImpl proxyFactory;
	ObjectTranslator query = new ObjectTranslator();
	private ORMapperFactory mapperFactory;
	int callCount = 0;
	private TransportFactory transport;
	ExecutorService executor = Executors.newFixedThreadPool(5);

	@Before
	public void setUp() throws Exception {
		com.liquidlabs.common.concurrent.ExecutorService.setTestMode();
		mapperFactory = new ORMapperFactory(NetworkUtils.determinePort(12345));
		ORMapper orMapper = mapperFactory.getORMapper(ORMapper.NAME, 11000);
		
		transport = new TransportFactoryImpl(Executors.newFixedThreadPool(10), "test");
		transport.start();

		proxyFactory = new ProxyFactoryImpl(transport, Config.TEST_PORT, executor, "testService");
		proxyFactory.start();
		orClient = new ORMapperClientImpl(orMapper, proxyFactory);
		proxyFactory.registerMethodReceiver("ORMClient", orClient);
		
	}
	
	@After
	public void tearDown() throws Exception {
		transport.stop();
		mapperFactory.stop();
	}
	

	@Test
	public void testShouldRemoteNotifyCorrectly() throws Exception {
		ORMapperFactory testMapper = new ORMapperFactory();
		ProxyFactoryImpl testProxyFactory = testMapper.getProxyFactory();
		ORMapperClient remoteClient = testProxyFactory.getRemoteService("ORMClient", ORMapperClient.class, proxyFactory.getAddress().toString());
		
		
		ORMEventListener eventListener2 = new ORMEventListener(){
			public String getId() {
				return "ORMEvent-Listener";
			}
			public void notify(String key, String payload, Type event, String source) {
				System.err.println("event:" +payload);
				User objectFromFormat = query.getObjectFromFormat(User.class, payload);
				System.err.println("user:" + objectFromFormat.id);
				callCount++;
				// now need to retrieve full object graph
			}
		};
		
		remoteClient.registerEventListener(User.class,  "", eventListener2 , new Type[] { Type.WRITE }, -1);
		
		User testUser = createTestObject("userId");
		orClient.store(testUser);
		Thread.sleep(100);
		assertEquals(1, callCount);
	}
	
	@Test
	public void testShouldRemoteNotifyCorrectlyWhenANDIsInTemplate() throws Exception {
		
		ORMapperFactory testMapper = new ORMapperFactory();
		ProxyFactoryImpl testProxyFactory = testMapper.getProxyFactory();
		ORMapperClient remoteClient = testProxyFactory.getRemoteService("ORMClient", ORMapperClient.class, proxyFactory.getAddress().toString());
		
		
		ORMEventListener eventListener2 = new ORMEventListener(){
			public String getId() {
				return "ORMEvent-Listener";
			}
			public void notify(String key, String payload, Type event, String source) {
				System.err.println("event:" +payload);
				User objectFromFormat = query.getObjectFromFormat(User.class, payload);
				System.err.println("user:" + objectFromFormat.id);
				callCount++;
				// now need to retrieve full object graph
			}
		};
		
		remoteClient.registerEventListener(User.class,  "id equals 'Alan AND Jason'", eventListener2 , new Type[] { Type.WRITE }, -1);
		
		User testUser = createTestObject("Alan AND Jason");
		orClient.store(testUser);
		Thread.sleep(100);
		assertEquals(1, callCount);
	}

	@Test
	public void testShouldUnregisterCorrectly() throws Exception {
		
		ORMEventListener eventListener2 = new ORMEventListener(){
			public String getId() {
				return "ORMEvent-Listener";
			}
			public void notify(String key, String payload, Type event, String source) {
				System.out.println("event:" +payload);
				User objectFromFormat = query.getObjectFromFormat(User.class, payload);
				System.out.println("user:" + objectFromFormat.id);
				callCount++;
				// now need to retrieve full object graph
			}
		};
		
		this.orClient.registerEventListener(User.class,  "", eventListener2 , new Type[] { Type.WRITE }, -1);
		
		this.orClient.unregisterEventListener("ORMEvent-Listener");
		User testUser = createTestObject("userId");
		orClient.store(testUser);
		Thread.sleep(100);
		// 
		assertEquals(0, callCount);
	}
	
	@Test
	public void testShouldNotifyCorrectly() throws Exception {
		
		ORMEventListener eventListener2 = new ORMEventListener(){
			public String getId() {
				return "ORMEvent-Listener";
			}
			public void notify(String key, String payload, Type event, String source) {
				System.out.println("event:" +payload);
				User objectFromFormat = query.getObjectFromFormat(User.class, payload);
				System.out.println("user:" + objectFromFormat.id);
				callCount++;
				// now need to retrieve full object graph
			}
		};
		
		this.orClient.registerEventListener(User.class,  "", eventListener2 , new Type[] { Type.WRITE }, -1);
		
		User testUser = createTestObject("userId");
		orClient.store(testUser);
		Thread.sleep(100);
		assertEquals(1, callCount);
	}

	@Test
	public void testShouldRetrieveStoredObject() throws Exception {
		User testUser = createTestObject("userId");
		orClient.store(testUser);
		
		User user = orClient.retrieve(User.class, "userId", true);
		
		assertNotNull(user);
		assertNotNull(user.homeAddress);
		assertNotNull(user.workAddress);
		assertEquals(user.homeAddress.city.id, user.workAddress.city.id);
	}

	@Test
	public void testShouldFindObjectIds() throws Exception {
		User testUser = createTestObject("userId1");
		orClient.store(testUser);
		User testUser2 = createTestObject("userId2");
		orClient.store(testUser2);
		User testUser3 = createTestObject("userId3");
		orClient.store(testUser3);
		
		String[] findIds = orClient.findIds(User.class, null, 999);
		
		assertEquals(3, findIds.length);
	}
	
	@Test
	public void testShouldFindObjectIdWithBooleanParam() throws Exception {
		User testUser = createTestObject("userId1");
		testUser.foo = true;
		orClient.store(testUser);
		User testUser2 = createTestObject("userId2");
		orClient.store(testUser2);
		
		String[] findIds = orClient.findIds(User.class, "foo equals true", 999);
		
		assertEquals(1, findIds.length);
	}


	private User createTestObject(String id) {
		User user = new User(id, "Joe", "bloggs", "25-10-70", 10);
		user.homeAddress = new Address("1000", 32, "alwyne road", "N1 6DG");
		user.workAddress = new Address("1010", 6, "bishopsgate", "N1 6DG");
		City london = new City("2000", "London999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");
		user.homeAddress.city = london;
		user.workAddress.city = london;

		return user;
	}
	
	public static class User {
		public User(){
		}
		public User(String id, String firstName, String surname, String dob, int age){
			this.id = id;
			this.firstName = firstName;
			this.surname = surname;
			this.dob = dob;
			this.age = age;
		}
		@Id
		String id;
		String type = "User";
		String firstName;
		String surname;
		String dob;
		int age;
		boolean foo;
		
		@Map
		Address homeAddress;
		@Map
		Address workAddress;

	}
	public static class Address {
		public Address(){
		}
	
		public Address(String id, int streetNumber, String street, String postcode) {
			this.id = id;
			this.streetNumber = streetNumber;
			this.street = street;
			this.postcode = postcode;
		}
		@Id
		String id;
		int streetNumber;
		String street;
		String postcode;
		
		@Map
		City city;
	}
	public static class City {
		public City(){
		}
		public City(String id, String string) {
			this.id = id;
			cityName = string;
		}
		@Id
		String id;
		String cityName;
	}

}
