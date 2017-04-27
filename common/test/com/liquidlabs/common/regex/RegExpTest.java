package com.liquidlabs.common.regex;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;

public class RegExpTest {
	String lineToMatch = "this\nis\r\nan\nERROR astuff\ndoing\nstuff";
	
	@Test
	public void shouldJavaMatchMultilineWithSingleGroup() throws Exception {
		String lineToMatch = "isit one\ntwo\nthree\r\nfour\rfive six:seven:eight";
//		MatchResult matches = RegExpUtil.matchesJava("(\\S+) ((.*)+) (six):(\\w+):(\\w+)", lineToMatch);
		MatchResult matches = RegExpUtil.matches("(\\S+) (.*)", lineToMatch);
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
	public void shouldJavaMatchDirectory() throws Exception {
		
		// escape backslashes!!
		java.util.regex.Pattern compile = java.util.regex.Pattern.compile("E:\\\\Files-stuff\\\\logs.*log");
		java.util.regex.Matcher matcher = compile.matcher("E:\\Files-stuff\\logs\\crap.log");
		assertTrue(matcher.matches());
		
		java.util.regex.Pattern compile1 = java.util.regex.Pattern.compile("/var/log/.*log");
		java.util.regex.Matcher matcher1 = compile1.matcher("/var/log/system/crap.log");
		assertTrue(matcher1.matches());
	}
	
	@Test
	public void shouldMultiLineMatchWithJDK() throws Exception {
		// The JDK Version
		java.util.regex.Pattern compile = java.util.regex.Pattern.compile(".*ERROR.*stuff");

		java.util.regex.Matcher matcher = compile.matcher(lineToMatch);
		boolean matches2 = matcher.matches();
		boolean find = matcher.find();
		assertTrue("Java didnt match multiline", find);	
	}
	
	@Test
	public void shouldHandleMultiLineMatching() throws Exception {
		MatchResult matches = RegExpUtil.matches("(.*)ERROR (\\w+)\\s+(\\w+)\\s+(.*)", lineToMatch);
		assertTrue(matches.match);
		System.out.println("groups:" + Arrays.toString(matches.groups));
	}

}
