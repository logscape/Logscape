package com.liquidlabs.common.regex;

import static org.junit.Assert.*;

import org.junit.Test;

public class MatchResultTest {

	@Test
	public void shouldGetGoodGroups() throws Exception {
		MatchResult mr = new MatchResult("one");
		assertNotNull(mr.getGroup(0));
		assertNull(mr.getGroup(1));
		
	}
}
