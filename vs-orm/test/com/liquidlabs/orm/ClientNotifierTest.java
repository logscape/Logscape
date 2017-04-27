package com.liquidlabs.orm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClientNotifierTest  {
	
	private ORMapperClient orClient;
	private ORMapperFactory mapperFactory;

	@Before
	public void setUp() throws Exception {
		ExecutorService.setTestMode();
		mapperFactory = new ORMapperFactory(NetworkUtils.determinePort(54321));
		orClient = mapperFactory.getORMapperClient();
	}
	
	@After
	public void tearDown() throws Exception {
		mapperFactory.stop();
	}
	
	
	@Test
	public void testContainsKey() throws Exception {
		orClient.store(createTestObject("u1", "Joe"));
		assertTrue(orClient.containsKey(User.class, "u1"));
	}
	
	@Test
	public void testShouldWorkWithTopLevelAND1() throws Exception {
			orClient.store(createTestObject("u1", "Joe"));
			orClient.store(createTestObject("u2","Jim"));
			String[] result = orClient.findIds(User.class, "(surname equals bloggs) AND (firstName equals Joe)", -1);
			assertTrue(result.length == 1);
			assertEquals("u1", result[0]);
	}
	public void testShouldWorkWithTopLevelOR1() throws Exception {
		orClient.store(createTestObject("u1", "Joe"));
		orClient.store(createTestObject("u2","Jim"));
		String[] result = orClient.findIds(User.class, "(surname equals bloggs) OR (age == 10)", -1);
		assertEquals(2, result.length);
	}
	
	public void testShouldDoUpdate() throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
		
		orClient.updateMultiple(User.class, "id equals userId", "surname replace bob", 1, -1, "xxx");
		List<User> users = orClient.findObjects(User.class, "id equals userId", true, -1);
		assertEquals("Should have updated User surname", "bob", users.get(0).surname);
		
	}
	public void testShouldDoUpdate2 () throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
		
		orClient.updateMultiple(User.class, "id equals userId", "surname replaceWith bob AND firstName replaceWith charlie", 1, -1, "xxx");
		List<User> users = orClient.findObjects(User.class, "id equals userId", true, -1);
		assertEquals("Should have changed User surname", "bob", users.get(0).surname);
		assertEquals("Should have changed User firsname", "charlie", users.get(0).firstName);
		
	}
	public void testShouldDoZeroLengthString () throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
		
		orClient.updateMultiple(User.class, "id equals userId", "firstName replaceWith bob AND surname replaceWith ''", 1, -1, "xxx");
		List<User> users = orClient.findObjects(User.class, "id equals userId", true, -1);
		assertEquals("Should have changed User surname", "", users.get(0).surname);

	}
	
	@Test
	public void testShouldDoUpdateWithWhiteSpace () throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
		
		orClient.updateMultiple(User.class, "id equals userId", "surname replaceWith bob AND firstName replaceWith 'org.codehaus.groovy.runtime.metaclass.MissingPropertyExceptionNoStack: No such property: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX for class: Script1'", 1, -1, "xxx");
		List<User> users = orClient.findObjects(User.class, "id equals userId", true, -1);
		assertEquals("Should have changed User surname", "bob", users.get(0).surname);
		assertTrue("Should have changes User firsname: updateValue["+users.get(0).firstName+"]", users.get(0).firstName.contains("No such property"));
		
	}
	@Test
	public void testShouldDoSimplePurge() throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
		ArrayList<User> arrayList = new ArrayList<User>();
		arrayList.add(testObject);
		orClient.purge(arrayList);
		String[] findIds = orClient.findIds(User.class, "id equals userId", -1);
		assertTrue("Should have removed User but didnt", findIds.length == 0);
		
	}
	
	@Test
	public void testShouldFindIdsSortedByQueryField() throws Exception {
		User user1 = createTestObject("uOne", "userAA");
		User user2 = createTestObject("uTwo", "userBB");
		User user3 = createTestObject("uThree", "userCC");
		orClient.store(user3);
		orClient.store(user1);
		orClient.store(user2);

		String[] findIds = orClient.findIds(User.class, "firstName containsAny AA,BB,CC", -1);
		assertTrue("Should have removed User but didnt", findIds.length == 3);
		assertEquals("uOne",findIds[0]);
		assertEquals("uTwo",findIds[1]);
		assertEquals("uThree",findIds[2]);
		
	}
	
	@Test
	public void testShouldPurgeObjectUsingQuery() throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
		
		User user = orClient.retrieve(User.class, "userId", true);
		assertNotNull(user);
		int purge = orClient.purge(User.class, "id equals userId");
		assertEquals("Should have purged  item", 1, purge);
		String[] findIds = orClient.findIds(User.class, "id equals userId", -1);
		assertTrue("Should have removed User but didnt", findIds.length == 0);
		
	}
	
	@Test
	public void testShouldRemoveStoredTestObject() throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
		
		User user = orClient.retrieve(User.class, "userId", true);
		assertNotNull(user);
		List<User> removeObjects = orClient.removeObjects(User.class, "id equals userId", true, -1, -1, 100);
		assertTrue(removeObjects.size() == 1);
		
		int count = orClient.count(User.class, null);
		assertEquals(0, count);
		
	}


	public void testShouldCountRightObject() throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
		User testObject2 = createTestObject();
		testObject2.id = "id2";
		orClient.store(testObject2);
		int count = orClient.count(User.class, null);
		assertEquals(2, count);
	}
	
	@Test
	public void testShouldStoreListTestObject() throws Exception {
		User testObject = createTestObject();
		orClient.store(Arrays.asList(new User[] { testObject }));
	}

	@Test
	public void testShouldStoreTestObject() throws Exception {
		User testObject = createTestObject();
		orClient.store(testObject);
	}
	@Test
	public void testShouldRetrieveStoredObject() throws Exception {
		User testUser = createTestObject();
		orClient.store(testUser);
		
		User user = orClient.retrieve(User.class, "userId", true);
		
		assertNotNull(user);
		assertNotNull(user.homeAddress);
		assertEquals("32, alywne road, canondale", user.homeAddress.street);
		assertNotNull(user.workAddress);
		assertEquals("2002", user.workAddress.city.id);
	}
	
	private User createTestObject() {
		User user = new User("userId", "Joe", "bloggs", "25-10-70", 10);
		user.homeAddress = new Address("1000", 32, "32, alywne road, canondale", "N1 6DG");
		user.workAddress = new Address("1010", 6, "32, bishopsgate, london", "N1 6DG");
		City london1 = new City("2001", "London");
		user.homeAddress.city = london1;
		City london2 = new City("2002", "London");
		user.workAddress.city = london2;

		return user;
	}
	private User createTestObject(String id, String firstName) {
		User user = new User(id, firstName, "bloggs", "25-10-70", 10);
		user.homeAddress = new Address("1000", 32, "32, alywne road, canondale", "N1 6DG");
		user.workAddress = new Address("1010", 6, "32, bishopsgate, london", "N1 6DG");
		City london1 = new City("2001", "London");
		user.homeAddress.city = london1;
		City london2 = new City("2002", "London");
		user.workAddress.city = london2;
		
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
