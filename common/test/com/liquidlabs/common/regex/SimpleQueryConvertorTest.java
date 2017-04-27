package com.liquidlabs.common.regex;

import static com.liquidlabs.common.regex.RegExpUtil.matches;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;


/**
 * Examples
 * 1  -this or *
 * 1.1-this or * or *
 * 2  -this or (*)
 * 2.1 (*,) - anything up until a comma == [^,]
 * 3  -this or * where number:(d)
 * 4  -* (A OR B OR C) Stuff ...
 * 5  -Check(.*)\\s+.*lastCheck(.*) => Check(*) ** lastCheck(*)
 * 
 * 6  -...(A OR B OR C) Stuff ...
 * 7 (A OR B) AND C

 * @author neil
 *
 */
public class SimpleQueryConvertorTest {
	
	
	private SimpleQueryConvertor convertor;

	@Before
	public void setup() {
		convertor = new SimpleQueryConvertor();
	}
	
	@Test
	public void shouldNotEscapeGroupWithORRegEx() throws Exception {
		String expr = "(A|B)";
		String regexp = new SimpleQueryConvertor().processExpression(expr);
		assertEquals(".*?(A|B).*", regexp);
		
	}
	
	@Test
	public void shouldAddCarrotToStartOfRegExp() throws Exception {
		String expr = "(* * *)";
		String regexp = convertor.convertSimpleToRegExp(expr);
		System.out.println("RegExp:" + regexp);
//		assertEquals("^(\\S+ \\S+ \\S+).*", regexp); if it is ^ it breaks links if it .* it breaks this. Don't no why it is hat anyway
		assertEquals(".*?(\\S+ \\S+ \\S+).*", regexp);
	}
	
	@Test
	public void shouldMakeGoodRegExp() throws Exception {
		String expr = "Denied ** from (*)";
		String regexp = convertor.convertSimpleToRegExp(expr);
		System.out.println("RegExp:" + regexp);
		assertEquals(".*?Denied .* from (\\S+).*", regexp);
		
	}
	
	@Test
	public void shouldConvertTABDelimit() throws Exception {
		String expression = "(*,),(*\\t)\t.*";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println(regexp);
		
	}
	
	@Test
	public void singleSkipFunctionWorks() throws Exception {
			String expression = "(s5).*";
			String regexp = convertor.convertSimpleToRegExp(expression);
			System.out.println(expression);
			System.out.println(regexp);
			String line = "1234567890";
			MatchResult match = matches(regexp, line);
			assertTrue(match.isMatch());
			assertEquals("12345", match.groups[1]);
			
	}
	
	
	@Test
	public void optionalQuestionIsGood() throws Exception {
		String expression = "(w),(?),(w)";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println(expression);
		System.out.println(regexp);
		assertTrue(matches(regexp, "one,two,three").isMatch());
		assertTrue(matches(regexp, "one,,three").isMatch());
	}
	@Test
	public void singleStarIsGood() throws Exception {
		String expression = "*";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println(expression);
		System.out.println(regexp);
		assertTrue(matches(regexp, "c:\\stuff\\agent.log").isMatch());
		assertTrue(matches(regexp, "c:\\stuff\\agent.log.24-10").isMatch());
	}
	
	@Test
	public void shouldAllowTrailingStar() throws Exception {
		String expression = "*.log*";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println(expression);
		System.out.println(regexp);
		assertTrue(matches(regexp, "c:\\stuff\\agent.log").isMatch());
		assertTrue(matches(regexp, "c:\\stuff\\agent.log.24-10").isMatch());
	}

	
	@Test
	public void shouldNotStuffUpXXString() throws Exception {
		String expression  = ".*XX.*";
		String regexp = convertor.convertSimpleToRegExp(expression);
		assertEquals(expression, regexp);
		
	}
	
	@Test
	public void shouldMatchDotStar() throws Exception {
		String expression  = ".*";
		String regexp = convertor.convertSimpleToRegExp(expression);
		assertEquals(expression, regexp);
		
	}
	
	@Test
	public void shouldMatchExplicitGroups() throws Exception {
		String expression  = "(stuff)";
		String regexp = convertor.convertSimpleToRegExp(expression);
		assertEquals(".*?(stuff).*", regexp);
	}
	

	@Test
	public void shouldWorkWithOldSchool() throws Exception {
		String expression = "WARN OR CRAP";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println("Exp:" + regexp);
		assertFalse("exp:" + regexp, regexp.contains(" OR "));
		assertTrue("exp:" + regexp, regexp.contains("__OR__"));
		
		
	}
	@Test
	public void shouldConvertStarStarProperly() throws Exception {
		String expression = "(WARN OR CRAP) **stcp(*)";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println("Exp:" + regexp);
		assertFalse("exp:" + regexp, regexp.contains("**"));
		
	}
	@Test
	public void shouldConvertStarStarWithGroup() throws Exception {
//		String expression = "(WARN OR CRAP) (**)";
		String expression = "(s23) (*) (*) (*) (**)";
		String regexp = convertor.convertSimpleToRegExp(expression);
		System.out.println("Exp:" + regexp);
		assertFalse("exp:" + regexp, regexp.contains("\\n"));
		
	}

	@Test
	public void shouldBeSimple() throws Exception {
		assertTrue(convertor.isSimpleLogFiler("ERROR"));
		
		
	}
	
	@Test
	public void testShouldDetectSimple() throws Exception {
		assertFalse(convertor.isSimpleLogFiler(".*stuff(*).*"));
		assertTrue(convertor.isSimpleLogFiler("stuff(*)"));
		assertTrue(!convertor.isSimpleLogFiler("[0-9]+\\-[0-9]+\\-[0-9]+\\s+[0-9]+\\:[0-9]+\\:[0-9]+\\,[0-9]+\\s+[A-Z\\_\\-]+\\s+ResourceAgent\\-2\\-1\\s+([A-Za-z\\.]+)\\s+\\w+\\s+\\w+\\s+\\w+\\s+\\w+\\s+[A-Z\\_\\-]+"));
	}
	
	@Test
	public void shouldToExampleOne() throws Exception {
		String actual = convertor.processExpression("this or *");
		assertEquals(".*?this or .*", actual);
	}
	public void shouldToExampleOneOne() throws Exception {
		String actual = convertor.processExpression("this or * or *");
		assertEquals("this or \\S+ or .*", actual);		
	}
	
	@Test
	public void shouldToExampleTwo() throws Exception {
		String actual = convertor.processExpression("this or (*)");
		assertEquals(".*?this or (\\S+).*", actual);
	}
	@Test
	public void shouldToExampleTwoB() throws Exception {
		String actual = convertor.processExpression("this or\\s+(*)");
		assertEquals(".*?this or\\s+(\\S+).*", actual);
	}
	
	@Test
	public void shouldToExampleThree_Numeric() throws Exception {
		String actual = convertor.processExpression("number:(d)");
		 //[-+]?[0-9]*\.?[0-9]+
		System.out.println(actual);
		assertTrue("number:10".matches(actual));
		assertTrue("number:0".matches(actual));
	}
	@Test
	public void shouldToExampleThree_Numeric2() throws Exception {
		String actual = convertor.processExpression("number:(d)|stuff");
		//[-+]?[0-9]*\.?[0-9]+
		System.out.println(actual);
		System.out.println("Match:" + "number:0.123|stuff".matches(".*?number:([-+]?\\d+\\.\\d*)\\|stuff.*"));
		assertTrue("number:10|stuff".matches(actual));
		assertTrue("number:0.123|stuff".matches(actual));
		
	}
	
	@Test
	public void shouldProcessGroupExampleFour() throws Exception {
		String actual = convertor.processExpression("this (ERROR OR Exception)");
		assertEquals(".*?this (ERROR|Exception).*", actual);
	}
	
	@Test
	public void shouldProcessGroupExampleFive() throws Exception {
		String actual = convertor.processExpression("Check(*) ** lastCheck(*)");
		assertEquals(".*?Check(\\S+) .* lastCheck(\\S+).*", actual);
	}
	@Test
	public void shouldProcessGroupALL() throws Exception {
		String actual = convertor.processExpression("(f*)");
		assertEquals(".*?(f\\S+).*", actual);
		assertTrue("fubber".matches(actual));
		assertTrue("a fubber".matches(actual));
	}
	
	
	@Test
	public void shouldhandleGroupedOR() throws Exception {
		assertEquals("(A|B|C)", convertor.handleGroupedOR("A OR B OR C"));
		
	}
	
	
	@Test
	public void shouldHandleSTAR() throws Exception {
		String convertSimpleToRegExp = convertor.convertSimpleToRegExp("*");
		assertTrue("some stuff".matches(convertSimpleToRegExp));
		
	}
	
	@Test
	public void shouldHandleNoGroups() throws Exception {
		List<Part> parts = convertor.getSections("some stuff");
		assertTrue(parts.size() == 1);
		assertTrue(parts.get(0).grouped == false);
		assertEquals("some stuff", parts.get(0).regexp());
		
	}
	@Test
	public void shouldBeakIntoPartsWithNoGroups() throws Exception {
		List<Part> parts = convertor.getSections("some (stuff)");
		assertTrue(parts.size() == 2);
		assertTrue(parts.get(0).grouped == false);
		assertEquals("some ", parts.get(0).regexp());
		
		assertTrue(parts.get(1).grouped);
		assertEquals("stuff", parts.get(1).regexp());
	}

    @Test
    public void shouldHandleGroupsAtStart() throws Exception {
        List<Part> parts = convertor.getSections("(some) (stuff)");
        assertThat(parts.size(), is(3));
        assertThat(parts.get(0).grouped, is(true));
    }
	
	@Test
	public void shouldEscapeGroup() throws Exception {
		//(A OR B OR C) => (A|B|C)
		
	}
	
	@Test
	public void shouldConvertANDORExpressionWithQuotes() throws Exception {
		String result = ".*?(A|B).*Stuff Things.*";
		String regex = convertor.convertSimpleToRegExp("(A OR B) AND 'Stuff Things''");
		assertThat(matches(regex, "A Stuff Things").match, is(true));
        assertEquals(result, regex);
		assertTrue(" this A and Stuff Things".matches(regex));
	}
	@Test
	public void shouldConvertANDExpressionWithQuotes() throws Exception {
		String result = ".*?A.*B C.*";
		String regex = convertor.convertSimpleToRegExp("A AND 'B C'");
        assertThat(matches(regex, "Hello A B C Hello").match, is(true));    
		assertEquals(result, regex);
	}
	@Test
	public void shouldConvertANDExpression() throws Exception {
		String result = ".*?A.*B.*C.*";
		String regex = convertor.convertSimpleToRegExp("A AND B AND C");
        assertThat(matches(regex, "Hello A B C Hello").match, is(true));
		assertEquals(result, regex);
	}
	public void shouldConvertSimpleAndToRegEx() throws Exception {
		String result = ".*?SLACONTAINER.*AddResource.*";
		String regex = convertor.convertSimpleToRegExp("SLACONTAINER AND AddResource");
        assertEquals(result, regex);
	}
	@Test
	public void shouldConvertSimpleOrToRegEx() throws Exception {
		String result = ".*?SLACONTAINER.*__OR__.*AddResource.*";
		String regex = convertor.convertSimpleToRegExp("SLACONTAINER OR AddResource");
		assertEquals(result, regex);
	}

	@Test
	public void shouldConvertToRegEx() throws Exception {
		String result = ".*?SLACONTAINER.*AddResource.*__OR__.*SLACONTAINER.*RemoveResource.*__OR__.*SLACONTAINER.*Free.*";
		String regex = convertor.convertSimpleToRegExp("SLACONTAINER AND AddResource OR SLACONTAINER AND RemoveResource OR SLACONTAINER AND Free");
		
		System.err.println(regex);
		assertEquals(result, regex);
	}
	@Test
	public void shouldLeaveVariablesAlone() throws Exception {
		String result = ".*?SLACONTAINER.*{0}.*__OR__.*SLACONTAINER.*{1}.*__OR__.*SLACONTAINER.*Free.*";
		String regex = convertor.convertSimpleToRegExp("SLACONTAINER AND {0} OR SLACONTAINER AND {1} OR SLACONTAINER AND Free");
		
		System.err.println(regex);
		assertEquals(result, regex);
	}
	@Test
	public void shouldEscapeChars() throws Exception {
		String regex = convertor.convertSimpleToRegExp("A AND some-host-name-1.0.com AND stuff");
		
		assertTrue("stuff A some-host-name-1.0.com stuff and things ".matches(regex));
		assertTrue("stuff A some-host-name stuff ".matches(".*A.*some.*host.*"));
	}
	
	@Test
	public void shouldNotEscapeExistingRegExpression() throws Exception {
		String expression = ".*Stuff.*";
		String regex = convertor.convertSimpleToRegExp(expression);
		assertEquals(expression, regex);
	}
	
	@Test
	public void shouldEscapeExistingRegExpression() throws Exception {
		String expression = "*Stuff*";
		String regex = convertor.convertSimpleToRegExp(expression);
		assertTrue("Stuff".matches(".*?Stuff.*"));
		assertTrue("HiStuff".matches(regex));
		assertEquals(".*?\\S+Stuff.*", regex);
	}
	
	@Test
	public void shouldEscapeBrackets() throws Exception {
		String replacement = null;
		replacement = convertor.replaceIfNotEscaped("[stuff", '[', false);
		assertEquals("\\[stuff", replacement);
		
		replacement = convertor.replaceIfNotEscaped("stuff]", ']', false);
		assertEquals("stuff\\]", replacement);
		
		replacement = convertor.replaceIfNotEscaped("stuff] a", ']', false);
		assertEquals("stuff\\] a", replacement);
		
		replacement = convertor.replaceIfNotEscaped("a [stuff", '[', false);
		assertEquals("a \\[stuff", replacement);
		
	}
	
	@Test
	public void shouldDetermineIfCharIsInGroup() throws Exception {
		String textString = "one ( t ) x";
		assertTrue(convertor.charIsInGroup(textString.indexOf("t"), textString));
		assertFalse(convertor.charIsInGroup(textString.indexOf("o"), textString));
		assertFalse(convertor.charIsInGroup(textString.indexOf("x"), textString));
	}
}
