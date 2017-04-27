package com.liquidlabs.common.regex;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;

import org.junit.Test;

public class JRegExTest  {
	
	@Test
	public void shouldJRegExpMultipleException() throws Exception {
		String expr = "^(.{23}) (\\S+) (\\S+) (.*) (\\w+)";
		String line = "2010-06-03 09:34:22,297 [ContainerBackgroundProcessor[StandardEngine[Catalina]]] ERROR org.apache.catalina.core.ContainerBase.[Catalina].[localhost].[/odc-dashboard-dev] - Error loading WebappClassLoader\r\n" + 
				"  delegate: false\r\n" + 
				"  repositories:\r\n LAST"; 
		
		MatchResult matches = RegExpUtil.matches(expr, line);
		for (String group : matches.groups) {
			System.out.println("JREG>" + group);
		}
		assertTrue(matches.isMatch());
		
	}
	
	@Test
	public void shouldMatchFalseCorrectlyOnMultiLine() throws Exception {
		String lineToMatch = "host\r\nValue:six";
		Pattern jregexpPattern = new jregex.Pattern(".*Value:(\\S+)", REFlags.MULTILINE | REFlags.DOTALL);
		Matcher matcher = jregexpPattern.matcher(lineToMatch);
		
		MatchResult matchResult = new MatchResult(matcher, true);
		System.out.println("Matches:" + matchResult);
		assertTrue(matchResult.isMatch());
		
		lineToMatch = "host\r\nValueXX:six";
		matcher = jregexpPattern.matcher(lineToMatch);
		MatchResult matchResult2 = new MatchResult(matcher, true);
		assertFalse(matchResult2.isMatch());
	}
	
	@Test
	public void shouldJREGEXPMatchMultilineWithSingleGroup() throws Exception {
		String lineToMatch = "isit one\r\ntwo\r\nthree\r\nfour\r\n six";
//		MatchResult matches = RegExpUtil.matches("(\\w+) ((.|\n|\r)+)", lineToMatch);
		MatchResult matches = RegExpUtil.matches("(\\w+) (.*) (\\S+)", lineToMatch);
		printMatch(matches);
	}
	
	private void printMatch(MatchResult matches) {
		if (!matches.match) System.out.println("BAD MATCH");
		String[] groups = matches.groups;
		int i = 0;
		for (String string : groups) {
			System.out.println(i++ + ") " + string);
		}
	}

	
	@Test
	public void sholdHaveGoodWebLogsPerformance() throws Exception {
		
		String text = "216.67.1.91 - leon [01/Jul/2002:12:11:52 +0000] \"GET /index.html HTTP/1.1\" 200 431 \"http://www.loganalyzer.net/\" \"Mozilla/4.05; [en] (WinNT I)\" \"USERID=CustomerA;IMPID=01234\"";
		String regExp = SimpleQueryConvertor.convertSimpleToRegExp("(*)\\s+(*)\\s+(*)\\s+\\[(c26)\\]\\s+\"(*)\\s+(*)\\s+(*)\"))\\s+(\\d+)\\s+(\\d+)\\s+\"(*)\"\\s+\"(*\")\"");
		
		System.out.println("RegExp:" + regExp);
		Pattern pattern = new Pattern(regExp);
		printResult(text, pattern, true);
		
		// do performance test
		// test performance
		long start = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			printResult(text, pattern, false);
		}
		long end = System.currentTimeMillis();
		System.out.println("elapsedMs:" + (end - start));

	}


	private void printResult(final String text, Pattern pattern, boolean sysoutIt) {
		Matcher matcher = pattern.matcher(text);
		MatchResult matchResult = new MatchResult(matcher, false);
		for (String group : matchResult.getGroups()) {
			if (sysoutIt) System.out.println("-) " + group);
		}
	}
	
	
	@Test
	public void testWithTabs() throws Exception {
		Pattern pattern = new Pattern("^(\\w+)\t([^\t]+)\t(.*)");
		Matcher matcher = pattern.matcher("one	twoA twoB	three");
		MatchResult matchResult = new MatchResult(matcher, false);
		System.out.println(">>>>>>>>>>" + Arrays.toString(matchResult.getGroups()));
		
	}
	@Test
	public void testStuff() throws Exception {
		Pattern pattern = new Pattern(".*(Exception).*");
		Matcher matcher = pattern.matcher("this is an Exception stuff");
		System.out.println(Arrays.toString(matcher.groups()));
		System.out.println("0:" + matcher.group(0));
	}

}
