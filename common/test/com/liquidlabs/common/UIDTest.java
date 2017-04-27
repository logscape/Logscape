package com.liquidlabs.common;

import static org.junit.Assert.assertNotSame;

import org.junit.Test;

public class UIDTest {

	@Test
	public void testShouldDumpSomeUIDS() throws Exception {
		for (int i = 0; i < 10; i++) {
			System.out.println(UID.getUUIDWithTime().toUpperCase());
		}
		
	}
	@Test
	public void testShouldWork() throws Exception {
		assertNotSame(UID.getUUID(), UID.getUUID());
	}
	@Test
	public void testShouldWork2() throws Exception {
		assertNotSame(UID.getUUIDWithTime(), UID.getUUIDWithTime());
	}
	@Test
	public void testShouldWork3() throws Exception {
		assertNotSame(UID.getUUIDWithHostNameAndTime(), UID.getUUIDWithHostNameAndTime());
	}
}
