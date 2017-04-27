package com.liquidlabs.common.collection;

import java.util.Map;
import java.util.TreeMap;

import javolution.util.FastMap;

import org.junit.Assert;
import org.junit.Test;

import com.thoughtworks.xstream.XStream;

public class XStreamFastMapConvertorTest {
	
	
	@Test
	public void shouldUseConvertorForXML() throws Exception {
		
		
		XStream stream = new XStream();
		
		stream.registerConverter(new LLMapConvertor(stream.getMapper()));
		
		MyClass myClass = new MyClass();
		myClass.data.put("one", "v1");
		myClass.data.put("two", "v2");
		String xml = stream.toXML(myClass);
		System.out.println(xml);
		Assert.assertTrue(xml.contains("one"));
		
	}
	
	public static class MyClass {
		Map<String, String> data = new FastMap<String, String>();
	}

}
