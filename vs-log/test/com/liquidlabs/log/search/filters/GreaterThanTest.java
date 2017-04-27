package com.liquidlabs.log.search.filters;


import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GreaterThanTest {
	
	FieldSet fieldSet = FieldSets.get();
    private int lineNumber;

    @Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	@Test
	public void shouldHandleGroup() throws Exception {
		GreaterThan filter = new GreaterThan("XXX","1", 50);
		boolean execute = filter.isPassed(fieldSet, new String [0], "", new MatchResult(new String[] { "group0", "25" } ), lineNumber);
		assertFalse(execute);
		
		execute = filter.isPassed(fieldSet, new String [0], "", new MatchResult(new String[] { "group0", "100" } ), lineNumber);
		assertTrue(execute);
	}

	@Test
	public void shouldPassFIELD() throws Exception {
		GreaterThan greaterThan = new GreaterThan("XXX", FieldSets.fieldName, 2);
		boolean execute = greaterThan.isPassed(fieldSet, new String [] { "100" }, "", null, lineNumber);
		assertTrue(execute);
	}
	@Test
	public void shouldRejectFIELD() throws Exception {
		GreaterThan greaterThan = new GreaterThan("XXX", FieldSets.fieldName, 2);
		boolean execute = greaterThan.isPassed(fieldSet, new String [] { "0" }, "", null, lineNumber);
		assertFalse(execute);
	}
	
	@Test
	public void shouldNotWorkWithEmptyGroup() throws Exception {
		GreaterThan greaterThan = new GreaterThan("XXX", FieldSets.fieldName, 2);
		boolean execute = greaterThan.isPassed(fieldSet, new String [] { "crap" }, "", null, lineNumber);
		assertFalse(execute);
	}

}
