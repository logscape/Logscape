package com.liquidlabs.space.map;

import static org.junit.Assert.*;
import org.junit.Test;

import com.liquidlabs.space.map.NewState;
import com.liquidlabs.transport.serialization.Convertor;

public class NewStateTest {
	
	@Test
	public void testFromNullShouldBeOk() throws Exception {
		NewState orig = new NewState("123", "defaultSpace",4312, 0, null, null, "key","newValue");
		byte[] serialize = Convertor.serialize(orig);
		NewState copy = NewState.deserialize(serialize);
		
		assertEquals(orig.id, copy.id);
		assertEquals(orig.index, copy.index);
		
		assertEquals(orig.newKey, copy.newKey);
		assertEquals(orig.newValue, copy.newValue);
		assertEquals(orig.existingValue, copy.existingKey);
		assertEquals(orig.existingValue, copy.existingValue);
		assertEquals(orig.partition, copy.partition);
	}
	
}
