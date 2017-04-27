package com.liquidlabs.orm;


import com.liquidlabs.common.NetworkUtils;
import junit.framework.TestCase;

import com.liquidlabs.transport.serialization.ObjectTranslator;


public class SpaceSideAPITest extends TestCase {
	
	private ORMapper orMapper;
	private ObjectTranslator query = new ObjectTranslator();
	private ORMapperFactory mapperFactory;
	int timeout = -1;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		mapperFactory = new ORMapperFactory(NetworkUtils.determinePort(34567));
		orMapper = mapperFactory.getORMapper(ORMapper.NAME, 11000);
	}
	@Override
	protected void tearDown() throws Exception {
		mapperFactory.stop();
	}
	
	public void testShouldStoreObjectOnSpaceSide() throws Exception {
		RootTestClass rootTestClass = makeTestObject();
		
		orMapper.storeObject(RootTestClass.class.getName(), rootTestClass.id, query.getStringFromObject(rootTestClass), timeout);
		orMapper.storeObject(InnerTestClass.class.getName(), rootTestClass.someField3.id, query.getStringFromObject(rootTestClass.someField3), timeout);
		Mapping mapping = new Mapping(rootTestClass, "someField3", rootTestClass.someField3);
		orMapper.storeMapping(RootTestClass.class.getName(), rootTestClass.id, "childId", query.getStringFromObject(mapping), timeout);
	}

	public void testShouldReassembleObjectFromSpaceSide() throws Exception {
		RootTestClass rootTestClass = makeTestObject();
		
		orMapper.storeObject(RootTestClass.class.getName(), rootTestClass.id, query.getStringFromObject(rootTestClass), timeout);
		orMapper.storeObject(InnerTestClass.class.getName(), rootTestClass.someField3.id, query.getStringFromObject(rootTestClass.someField3), timeout);
		Mapping mapping = new Mapping(rootTestClass, "someField3", rootTestClass.someField3);
		orMapper.storeMapping(RootTestClass.class.getName(), rootTestClass.id, "childId", query.getStringFromObject(mapping), timeout);
		
		String[] parts = orMapper.readObject(RootTestClass.class.getName(), rootTestClass.id, true);
		assertEquals("Parsing the mapping should have given 3 results", 3, parts.length);
		
		RootTestClass	rootResult = (RootTestClass) query.getObjectFromFormat(RootTestClass.class, parts[0], 1);
		Mapping rMapping = (Mapping) query.getObjectFromFormat(Mapping.class, parts[1]);
		InnerTestClass	innerResult = (InnerTestClass) query.getObjectFromFormat(InnerTestClass.class, parts[2]);
		
		assertNotNull(rMapping);
		rMapping.apply(rootResult, innerResult);
		
		assertNotNull(rootResult.someField3);
	}
	
	private RootTestClass makeTestObject() {
		RootTestClass rootTestClass = new RootTestClass();
		rootTestClass.someField3 = new InnerTestClass();
		rootTestClass.someField3.innerTCField = "field3-SetInTest";
		return rootTestClass;
	}
	
	
	public static class RootTestClass {
		@Id
		String id = "123";
		String someField1 = "valueA";
		String someField2 = "valueB";
		InnerTestClass someField3 = null;;
	}
	public static class InnerTestClass {
		@Id
		String id = "456";
		String innerTCField = "valueC";
	}
	
}
