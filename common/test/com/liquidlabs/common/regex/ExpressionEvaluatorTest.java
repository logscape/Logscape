package com.liquidlabs.common.regex;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ExpressionEvaluatorTest {




    @Test
    public void shouldSplitStuffOnComma() throws Exception {
        ExpressionEvaluator eval = new ExpressionEvaluator("split(,)");
        MatchResult evaluate = eval.evaluate("a,bb,ccc,ddd");
        assertTrue(evaluate.isMatch());
        assertEquals("a", evaluate.getGroup(1));
        assertEquals("bb", evaluate.getGroup(2));
        assertEquals("ccc", evaluate.getGroup(3));
        assertEquals("ddd", evaluate.getGroup(4));
        assertNull(evaluate.getGroup(5));

    }
    @Test
    public void shouldSplitStuffOnPipe() throws Exception {
        ExpressionEvaluator eval = new ExpressionEvaluator("split(\\|)");
        MatchResult evaluate = eval.evaluate("a|bb|ccc|ddd");
        assertTrue(evaluate.isMatch());
        assertEquals("a", evaluate.getGroup(1));
        assertEquals("bb", evaluate.getGroup(2));
        assertEquals("ccc", evaluate.getGroup(3));
        assertEquals("ddd", evaluate.getGroup(4));
        assertNull(evaluate.getGroup(5));

    }

    @Test
    public void shouldSplitStuffNoQuotes() throws Exception {
        ExpressionEvaluator eval = new ExpressionEvaluator("split(\\s+)");
        MatchResult evaluate = eval.evaluate("a bb ccc ddd");
        assertTrue(evaluate.isMatch());
        assertEquals("a", evaluate.getGroup(1));
        assertEquals("bb", evaluate.getGroup(2));
        assertEquals("ccc", evaluate.getGroup(3));
        assertEquals("ddd", evaluate.getGroup(4));
        assertNull(evaluate.getGroup(5));

    }



    @Test
    public void shouldSplitStuff() throws Exception {
        ExpressionEvaluator eval = new ExpressionEvaluator("split(\"\\s+\")");
        MatchResult evaluate = eval.evaluate("a bb ccc ddd");
        assertTrue(evaluate.isMatch());
        assertEquals("a", evaluate.getGroup(1));
        assertEquals("bb", evaluate.getGroup(2));
        assertEquals("ccc", evaluate.getGroup(3));
        assertEquals("ddd", evaluate.getGroup(4));
        assertNull(evaluate.getGroup(5));

    }

	
	@Test
	public void shouldMatchThis() throws Exception {
		ExpressionEvaluator eval = new ExpressionEvaluator("Netty AND Stats:(*) AND \\s+(*)kb AND \\s+(*)kb");
		MatchResult evaluate = eval.evaluate("2012-07-06 14:02:09,009 INFO orm-SHARED-4-1 (netty.NettyEndPoint)     Stats:SHARED/stcp://192.168.70.8:11003 Send[114msg 111kb] Recv[110msg 222kb]");
		assertTrue(evaluate.isMatch());
		assertEquals("SHARED/stcp://192.168.70.8:11003", evaluate.getGroup(1));
		assertEquals("111", evaluate.getGroup(2));
		assertEquals("222", evaluate.getGroup(3));
		
	}
	
	@Test
	public void shouldMatchEscapedString() throws Exception {
		ExpressionEvaluator eval = new ExpressionEvaluator("load\\(\\)");
		MatchResult evaluate = eval.evaluate("this is my line load() stuff");
		assertTrue(evaluate.isMatch());
	}

	@Test
	public void matchesRegExpBased_OR_() throws Exception {
		ExpressionEvaluator eval = new ExpressionEvaluator("data:(A OR B) (*)");
		MatchResult evaluate = eval.evaluate("data:A stuff");
		assertTrue(evaluate.isMatch());
		assertTrue(eval.evaluate("data:B stuff").isMatch());
		assertFalse(eval.evaluate("data:C stuff").isMatch());
	}
	
	
	@Test
	public void matchesRegExpPattern() throws Exception {
		ExpressionEvaluator eval = new ExpressionEvaluator(".*?(A|B|C).*");
		assertTrue(eval.evaluate("B A C").isMatch());
		assertTrue(eval.evaluate("A B Z").isMatch());
		assertTrue(eval.evaluate("Z B A").isMatch());
		assertFalse(eval.evaluate("Z D E ").isMatch());
	}
	
	@Test
	public void matchesRegExpPattern2() throws Exception {
		ExpressionEvaluator eval = new ExpressionEvaluator("ip:(*)");
		assertTrue(eval.evaluate("ip:word1").isMatch());
		assertTrue(eval.evaluate("ip:word 33333").isMatch());
		assertFalse(eval.evaluate("ipp:word1").isMatch());
	}
	@Test
	public void matchesRegExpPattern3() throws Exception {
		ExpressionEvaluator eval = new ExpressionEvaluator("hi AND ip:(*)");
		assertTrue(eval.evaluate("hi stuff ip:word1").isMatch());
		assertTrue(eval.evaluate("hi stuff ip:word 33333").isMatch());
		assertFalse(eval.evaluate("hello ip:word1").isMatch());
	}
	
	@Test
	public void matchesPatternWithWhiteSpaceUse() throws Exception {
		String line = "2012-10-04 07:29:57,333 INFO WriteBehindThread:BinaryEntryStoreWrapper(com.sportingbet.murdoch.cachestore.SpringAwareCacheStore):qa/web/fixture-part-service SpringAwareCacheStore TASK - ProcessMarketFixturePart missing-data time:0ms (cacheName:qa/web/fixture-part,key:FixturePartKey{fixtureId=1038562, partId=false},op:store)";
		
		assertTrue(new ExpressionEvaluator("TASK - (\\S+) (\\S+)\\s+time:(\\S+)ms").evaluate(line).isMatch());

		assertTrue(new ExpressionEvaluator("TASK - (\\S+) (\\S+) time:(\\S+)ms").evaluate(line).isMatch());
		
		assertTrue(new ExpressionEvaluator(".*TASK - (\\S+) (\\S+)\\s+time:(\\S+)ms.*").evaluate(line).isMatch());
	}
	
	@Test
	public void matchedORStringIndexWithCaseOff() throws Exception {
		ExpressionEvaluator.ignoreCase = true;
		ExpressionEvaluator eval = new ExpressionEvaluator("A OR b OR C");
		assertTrue(eval.evaluate("B").isMatch());
		assertTrue(eval.evaluate("b").isMatch());
		assertTrue(eval.evaluate("c").isMatch());
	}
	@Test
	public void matchedORStringIndex() throws Exception {
		ExpressionEvaluator.ignoreCase = true;
		ExpressionEvaluator eval = new ExpressionEvaluator("A OR B OR C");
		
		assertTrue(eval.evaluate("B A C").isMatch());
		assertTrue(eval.evaluate("A B Z").isMatch());
		assertTrue(eval.evaluate("Z B A").isMatch());
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			assertTrue(eval.evaluate("B A C").isMatch());
			assertTrue(eval.evaluate("A B Z").isMatch());
			assertTrue(eval.evaluate("Z B A").isMatch());
			assertFalse(eval.evaluate("Z D E ").isMatch());
		}
		long end = System.currentTimeMillis();
		System.out.println("Elapsed:" + (end -start));
	}
	
	@Test
	public void matchesANDStringIndex() throws Exception {
		ExpressionEvaluator eval = new ExpressionEvaluator("A AND B AND C");
		assertTrue(eval.evaluate("B A C").isMatch());
		assertFalse(eval.evaluate("A B Z").isMatch());
		assertFalse(eval.evaluate("Z B A").isMatch());
	}
	
	@Test
	public void shouldGetStringIndexPartsWithOR() throws Exception {
		List<String[]> mapped = new ExpressionEvaluator("").getStringIndexParts("A OR B OR C");
		assertEquals(3, mapped.size());
		assertEquals(1, mapped.get(0).length);
		
	}
	@Test
	public void shouldGetStringIndexPartsWithAND() throws Exception {
		List<String[]> mapped = new ExpressionEvaluator("").getStringIndexParts("A AND B AND C");
		assertEquals(1, mapped.size());
		assertEquals(3, mapped.get(0).length);
		
	}

	
	
	@Test
	public void shouldRecogniseRegExp() throws Exception {
		assertTrue(new ExpressionEvaluator("").isRegExp("A (*) B"));
		assertTrue(new ExpressionEvaluator("").isRegExp("A AND ip:(*)"));
		assertFalse(new ExpressionEvaluator("").isRegExp("A OR B"));
	}

}
