package com.liquidlabs.orm;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.liquidlabs.common.NetworkUtils;
import org.junit.Test;

import junit.framework.TestCase;

import com.liquidlabs.common.collection.PropertyMap;
import com.liquidlabs.common.concurrent.ExecutorService;

public class ORMFunctionalTest extends TestCase {


	private ORMapperClient orClient;
	private ORMapperFactory mapperFactory;

	@Override
	protected void setUp() throws Exception {
		ExecutorService.setTestMode();
		System.out.println("==================== Starting:" + getName() + " =============================================");
		mapperFactory = new ORMapperFactory(NetworkUtils.determinePort(21345));
		orClient = mapperFactory.getORMapperClient();
	}
	protected void tearDown() throws Exception {
		mapperFactory.stop();
	}
	
	@Test
	public void testshouldReadObjectWithoutType() throws Exception {
		ResourceProfile profile1 = new ResourceProfile();
		profile1.resourceId = "100";
		profile1.role = "Agent";
		orClient.store(profile1);
		Set<String> keySet = orClient.keySet();
		for (String key : keySet) {
			Object obj = orClient.getObject(key);
		}
		
	}
	
	@Test
	public void shouldListResourcesInCorrectOrderWithContainsAny() throws Exception {
		ResourceProfile profile1 = new ResourceProfile();
		profile1.resourceId = "100";
		profile1.role = "Agent";
		orClient.store(profile1);
		ResourceProfile profile2 = new ResourceProfile();
		profile2.resourceId = "200";
		profile2.role = "Manager";
		orClient.store(profile2);
		ResourceProfile profile3 = new ResourceProfile();
		profile3.resourceId = "300";
		profile3.role = "DiffAgent";
		orClient.store(profile3);
		ResourceProfile profile4 = new ResourceProfile();
		profile4.resourceId = "400";
		profile4.role = "Manager";
		orClient.store(profile4);
		ResourceProfile profile5 = new ResourceProfile();
		profile5.resourceId = "500";
		profile5.role = "OtherAgent";
		orClient.store(profile5);
		String[] findIds = orClient.findIds(ResourceProfile.class, "role containsAny DiffAgent,OtherAgent,Manager", -1);
		assertEquals("300", findIds[0]);
		assertEquals("500", findIds[1]);
		assertEquals("200", findIds[2]);
		assertEquals("400", findIds[3]);

	}
	
	public void testShouldStoreObjectWithMap() throws Exception {
		ObjectWithMap objectWithMap = new ObjectWithMap();
		objectWithMap.myName = "id";
		objectWithMap.map.put("a", "b");
		objectWithMap.map.put("a2", "***||&&&>><<");
		orClient.store(objectWithMap);
		List<ObjectWithMap> findObjects = orClient.findObjects(ObjectWithMap.class, "myName equals id", false, 1);
		assertNotNull(findObjects);
		ObjectWithMap result = findObjects.get(0);
		assertEquals("id", result.myName);
		assertEquals("b", result.map.get("a"));
	}
	
	
	public static class ObjectWithMap {
		@Id
		public String myName;
		public java.util.Map<String, String> map = new HashMap<String, String>();
	}
	public void testShouldStoreBooleanArray() throws Exception {
		ResourceProfile profile = new ResourceProfile("1");
		profile.testBooleanArray = new boolean[] { true, false };
		orClient.store(profile);
		ResourceProfile retrieve = orClient.retrieve(ResourceProfile.class, "1", false);
		assertEquals(2, retrieve.testBooleanArray.length);
		assertTrue(retrieve.testBooleanArray[0]);
		assertFalse(retrieve.testBooleanArray[1]);
	
		
	}

	public void testShouldFindUsingFieldLevelANDOperatorWithNumbers() throws Exception {
		ResourceProfile profile = new ResourceProfile("0");
		
		orClient.store(profile);
		
		String[] findIds = orClient.findIds(ResourceProfile.class, "aStuff == 100 AND aStuff > 0", -1);
		assertEquals(1, findIds.length);
	}
	
	
	public void testShouldFindUsingFieldLevelANDOperator() throws Exception {
		ResourceProfile profile = new ResourceProfile("0");
		orClient.store(profile);
		
		String[] findIds = orClient.findIds(ResourceProfile.class, "resourceId contains 0 AND resourceId notContains Z", -1);
		assertEquals(1, findIds.length);
	}
	
	public void testShouldPerformFieldLevelANDProperly() throws Exception {
		ResourceProfile profile = new ResourceProfile("0");
		profile.setCustomProperty("grid", "http://jars.db.com:80");
		profile.setCustomProperty("config", "active");
		profile.setCustomProperty("nonmanaged", "true");
		orClient.store(profile);
		
		String[] ids = orClient.findIds(ResourceProfile.class, "customProperties contains nonmanaged=true AND customProperties notContains ServiceType=System", -1);
		assertEquals(1,ids.length);
		
	}
	
	public void testShouldFindRightObjectCount() throws Exception {
		
		int populationSize = 100;
		for (int i = 0; i < populationSize; i++) {
			orClient.store(new ResourceProfile("Resource=" + i));
		}
		String resourceIds = "Resource=0,Resource=1,Resource=10,Resource=13,Resource=14,Resource=15,Resource=17,Resource=18,Resource=19,Resource=2,Resource=20,Resource=21,Resource=22,Resource=24,Resource=25,Resource=26,Resource=27,Resource=28,Resource=29,Resource=3,Resource=34,Resource=39,Resource=4,Resource=42,Resource=44,Resource=45,Resource=5,Resource=50,Resource=54,Resource=58,Resource=6,Resource=60,Resource=61,Resource=62,Resource=64,Resource=65,Resource=67,Resource=68,Resource=69,Resource=7,Resource=70,Resource=71,Resource=72,Resource=73,Resource=8,Resource=80,Resource=81,Resource=83,Resource=9,Resource=98";
		
		String[] results = orClient.findIds(ResourceProfile.class, "resourceId equalsAny \"" + resourceIds + "\"", 50);
		
		ArrayList<String> arrayList1 = new ArrayList<String>(Arrays.asList(results));
		Collections.sort(arrayList1);
		System.err.println(" AllocValues:" + arrayList1);
		
		ArrayList<String> arrayList2 = new ArrayList<String>(Arrays.asList(resourceIds.split(",")));
		Collections.sort(arrayList2);
		System.err.println(" AllocValues:" + arrayList2);
		
		assertEquals(arrayList1.size(), arrayList2.size());
		assertEquals("Contents to not match!", arrayList1.toString(), arrayList2.toString());

		
	}
	
	public static class ResourceProfile {
		
		public ResourceProfile() {
		}
		public ResourceProfile(String resourceId) {
			this.resourceId = resourceId;
		}
		public int aStuff = 100;
		@Id
		public String resourceId;
		public int zStuff = 0;
		public String role;
		String customProperties = "";
		boolean[] testBooleanArray = new boolean[0];
		
		public void setCustomProperty(String key, String value) {
			PropertyMap propertyMap = new PropertyMap(this.customProperties);
			propertyMap.put(key, value);
			this.customProperties = propertyMap.toString();
		}

	}
}
