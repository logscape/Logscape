package com.liquidlabs.orm;

import java.util.Arrays;
import java.util.List;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.orm.ORMapperClient;
import com.liquidlabs.orm.ORMapperFactory;

import junit.framework.TestCase;

public class NestedClientSideTest extends TestCase {

	private ORMapperClient orClient;
	private ORMapperFactory mapperFactory;
	int limit = 999;

	@Override
	protected void setUp() throws Exception {
		ExecutorService.setTestMode();
		mapperFactory = new ORMapperFactory(NetworkUtils.determinePort(34215));
		orClient = mapperFactory.getORMapperClient();
	}
	@Override
	protected void tearDown() throws Exception {
		mapperFactory.stop();
	}
	

	public void testShouldRetrieveStoredObject() throws Exception {
		User testUser = createTestObject("userId");
		orClient.store(testUser);
	
		User user = orClient.retrieve(User.class, "userId", true);
	
		assertNotNull(user);
		assertNotNull(user.id);
		assertNotNull(user.homeAddress);
		assertNotNull(user.workAddress);
		assertEquals(user.homeAddress.city.id, user.workAddress.city.id);
	
	}

	public void testShouldRetrieveAndUpdateSingleObject() throws Exception {
		User testUser = createTestObject("userId");
		orClient.store(testUser);
	
		List<User> users = orClient.findAndUpdate(User.class, "id equals userId", "firstName replace someNewName AND surname replace jones", 60, true, 1000, null);
		assertEquals(1, users.size());
		List<User> users2 = orClient.findObjects(User.class, "id equals userId", true, limit);
		assertEquals(users2.get(0).firstName, "someNewName");
		assertEquals(users2.get(0).surname, "jones");
	}
	public void testShouldRetrieveAndUpdateMultipleObjects() throws Exception {
		User testUser = createTestObject("userId1");
		testUser.firstName = "sumpn";
		testUser.surname = "surnameKey";
		orClient.store(testUser);
		User testUser2 = createTestObject("userId2");
		testUser2.firstName = "somthing";
		testUser2.surname = "surnameKey";
		orClient.store(testUser2);
		
		List<User> users = orClient.findAndUpdate(User.class, "surname equals surnameKey", "firstName replace someNewName", -1, true, -1, null);
		assertEquals(2, users.size());
		String names = users.get(0).firstName + "," + users.get(1).firstName;
		
		// VERIFY a direct read has the items
		List<User> users2 = orClient.findObjects(User.class, "surname equals surnameKey", true, limit);
		assertEquals(2, users2.size());
		assertEquals(users2.get(0).firstName, "someNewName");
		assertEquals(users2.get(1).firstName, "someNewName");
	}

	public void testShouldRetrieveObjects() throws Exception {
		User testUser = createTestObject("userId1");
		orClient.store(testUser);
		User testUser2 = createTestObject("userId2");
		orClient.store(testUser2);
		User testUser3 = createTestObject("userId3");
		orClient.store(testUser3);

		List<User> foundObjects = orClient.findObjects(User.class, "id equals userId1 OR id equals userId2 OR id equals userId3", true, limit);

		assertNotNull(foundObjects);
		assertEquals(3, foundObjects.size());
		assertEquals("userId1", foundObjects.get(0).id);
		assertEquals("userId2", foundObjects.get(1).id);
		assertEquals("userId3", foundObjects.get(2).id);
	}

	public void testShouldFindObjectIdsUsingQuery() throws Exception {
		User testUser = createTestObject("userId1");
		orClient.store(testUser);
		User testUser2 = createTestObject("userId2");
		orClient.store(testUser2);
		User testUser3 = createTestObject("userId3");
		orClient.store(testUser3);

		String[] findIds = orClient.findIds(User.class, "", limit);

		assertEquals(3, findIds.length);
		assertTrue(Arrays.toString(findIds).contains("userId1"));
	}

	public void testShouldStoreTestObject() throws Exception {
		User testObject = createTestObject("userId");
		orClient.store(testObject);
	}

	public void testShouldRetrieveShallowStoredObject() throws Exception {
		User testUser = createTestObject("userId");
		orClient.store(testUser);

		User user = orClient.retrieve(User.class, "userId", false);

		assertNotNull(user);
		assertNotNull("User.Id was null", user.id);
		assertNotNull(user.homeAddress);
		assertNotNull(user.workAddress);

	}

	private User createTestObject(String id) {
		User user = new User(id, "Joe", "bloggs", "25-10-70", 10);
		user.homeAddress = new Address("1000", 32, "alwyne road", "N1 6DG");
		user.workAddress = new Address("1010", 6, "bishopsgate", "N1 6DG");
		City london = new City("2000", "London");
		user.homeAddress.city = london;
		user.workAddress.city = london;

		return user;
	}

	public static class BaseId {
		@Id
		String id;
	}

	public static class User extends BaseId {
		public User() {
		}

		public User(String id, String firstName, String surname, String dob, int age) {
			this.id = id;
			this.firstName = firstName;
			this.surname = surname;
			this.dob = dob;
			this.age = age;
		}

		String firstName;
		String surname;
		String dob;
		int age;

		@Map
		Address homeAddress;
		@Map
		Address workAddress;

	}

	public static class Address {
		public Address() {
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
		public City() {
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
