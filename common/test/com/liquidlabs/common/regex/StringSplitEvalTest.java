package com.liquidlabs.common.regex;

import static org.junit.Assert.*;

import org.junit.Test;

public class StringSplitEvalTest {

	@Test
	public void shouldSplitTabString() throws Exception {
		StringSplitEval eval = new StringSplitEval("split(\\t,3)");
		MatchResult evaluate = eval.evaluate("one\ttwo\tthree");
		assertTrue(evaluate.isMatch());
		assertEquals("one\ttwo\tthree", evaluate.groups[0]);
		assertEquals("one", evaluate.groups[1]);
		assertEquals("two", evaluate.groups[2]);
		assertEquals("three", evaluate.groups[3]);
	}
	
	@Test
	public void shouldSplitString() throws Exception {
		StringSplitEval eval = new StringSplitEval("split( ,3)");
		MatchResult evaluate = eval.evaluate("one two three");
		assertTrue(evaluate.isMatch());
		assertEquals("one two three", evaluate.groups[0]);
		assertEquals("one", evaluate.groups[1]);
		assertEquals("two", evaluate.groups[2]);
		assertEquals("three", evaluate.groups[3]);
	}
}
