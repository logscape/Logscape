package com.liquidlabs.transport.serialization;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class ObjectTranslatorTest {

	private ObjectTranslator translator;

	@Before
	public void setup() {
		translator = new ObjectTranslator();
	}
	
	@Test
	public void testShouldNotFailOnSchemaWithMissingFieldValue() throws Exception {
		SimpleClassWithSchema first = new SimpleClassWithSchema("one", "two", "three");
		String stringFromObject = translator.getStringFromObject(first);
		String truncatedStringSimulatingMissingField = stringFromObject.substring(0, stringFromObject.indexOf("two") + 3);
		SimpleClassWithSchema second = translator.getObjectFromFormat(SimpleClassWithSchema.class, truncatedStringSimulatingMissingField);
		assertEquals(first.firstString, second.firstString);
		assertEquals(first.secondString, second.secondString);
		assertNotNull(second.thirdString);
		assertEquals("default", second.thirdString);
		
	}
	
	@Test
	public void testSchemaObjectExtraction() throws Exception {
		SimpleClassWithSchema first = new SimpleClassWithSchema("one", "two","three");
		String stringFromObject = translator.getStringFromObject(first);
		SimpleClassWithSchema second = translator.getObjectFromFormat(SimpleClassWithSchema.class, stringFromObject);
		assertEquals(first, second);
	}

	@Test
	public void testNormalObjectExtraction() throws Exception {
		SimpleClass first = new SimpleClass("one", "two");
		String stringFromObject = translator.getStringFromObject(first);
		SimpleClass second = translator.getObjectFromFormat(SimpleClass.class, stringFromObject);
		assertEquals(first, second);
	}
	
	@Test
	public void testSchemaBasedFieldExtraction() throws Exception {
		List<Field> schemaBasedFields = translator.getSchemaBasedFields(SimpleClassWithSchema.class);
		assertTrue(schemaBasedFields.get(0).getName().equals("firstString"));
		assertTrue(schemaBasedFields.get(1).getName().equals("secondString"));
	}

	@Test
	public void testSchemaBasedFieldExtractionAndInheretence() throws Exception {
		List<Field> schemaBasedFields = translator.getSchemaBasedFields(SimpleClassWithSchemaWithInh.class);
		assertTrue(schemaBasedFields.get(0).getName().equals("firstString"));
		assertTrue(schemaBasedFields.get(1).getName().equals("secondString"));
		assertTrue(schemaBasedFields.get(2).getName().equals("thirdString"));
	}
	@Test
	public void testNONSchemaBasedFieldExtraction() throws Exception {
		List<Field> schemaBasedFields = translator.getSchemaBasedFields(SimpleClass.class);
		assertNull(schemaBasedFields);
	}
	
	public static class SimpleClass {
		String firstString;
		String secondString;
		public SimpleClass() {
		}
		public SimpleClass(String firstString, String secondString) {
			this.firstString = firstString;
			this.secondString = secondString;
		}
		@Override
		public boolean equals(Object obj) {
			SimpleClass other = (SimpleClass) obj;
			return this.firstString.equals(other.firstString) &&
					this.secondString.equals(other.secondString);
		}
	}
	public static class SimpleClassWithSchema {
		enum SCHEMA { firstString, secondString, thirdString };
		String firstString;
		String secondString;
		String thirdString = "default";
		public SimpleClassWithSchema() {
		}
		public SimpleClassWithSchema(String firstString, String secondString, String thirdString) {
			this.firstString = firstString;
			this.secondString = secondString;
			this.thirdString = thirdString;
		}
		@Override
		public boolean equals(Object obj) {
			SimpleClassWithSchema other = (SimpleClassWithSchema) obj;
			return this.firstString.equals(other.firstString) &&
			this.thirdString.equals(other.thirdString) &&
			this.secondString.equals(other.secondString);
		}
	}
	public static class SimpleClassWithSchemaWithInh extends BaseClass{
		enum SCHEMA { firstString, secondString, thirdString };
		String firstString;
		String secondString;

		public SimpleClassWithSchemaWithInh() {
		}
		public SimpleClassWithSchemaWithInh(String firstString, String secondString, String thirdString) {
			this.firstString = firstString;
			this.secondString = secondString;
		}
		@Override
		public boolean equals(Object obj) {
			SimpleClassWithSchema other = (SimpleClassWithSchema) obj;
			return this.firstString.equals(other.firstString) &&
					this.thirdString.equals(other.thirdString) &&
					this.secondString.equals(other.secondString);
		}
	}
	public static class BaseClass {
		String thirdString = "default";
	}
}
