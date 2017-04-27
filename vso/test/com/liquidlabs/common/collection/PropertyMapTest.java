package com.liquidlabs.common.collection;

import com.liquidlabs.common.collection.PropertyMap;

import junit.framework.TestCase;

public class PropertyMapTest extends TestCase {
	
	
	public void testShouldAddPropertyAndContainInString() throws Exception {
		PropertyMap propertyMap = new PropertyMap("a=b");
		assertTrue(propertyMap.toString().contains("a=b"));
	}
	public void testShouldAddPropertiesAndContainInString() throws Exception {
		PropertyMap propertyMap = new PropertyMap("a=b, c = d");
		assertTrue("GotString:" + propertyMap.toString(), propertyMap.toString().contains("a=b"));
		assertTrue("GotString:" + propertyMap.toString(), propertyMap.toString().contains("c=d"));
		assertTrue("GotString:" + propertyMap.toString(), propertyMap.toString().contains(","));
	}
	public void testShouldAddPropertiesAndContainInStringWithLotsOfWhiteSpace() throws Exception {
		PropertyMap propertyMap = new PropertyMap("  a  =   b , c            = d");
		assertTrue("GotString:" + propertyMap.toString(), propertyMap.toString().contains("a=b"));
		assertTrue("GotString:" + propertyMap.toString(), propertyMap.toString().contains("c=d"));
		assertTrue("GotString:" + propertyMap.toString(), propertyMap.toString().contains(","));
	}
	public void testShouldHandleNULLAndContainInString() throws Exception {
		PropertyMap propertyMap = new PropertyMap(null);
		assertTrue(propertyMap.toString().length() == 0);
	}
	public void testShouldHandleZeroStringAndContainInString() throws Exception {
		PropertyMap propertyMap = new PropertyMap("");
		assertTrue(propertyMap.toString().length() == 0);
	}

}
