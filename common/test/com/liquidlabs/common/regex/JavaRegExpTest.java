package com.liquidlabs.common.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;



public class JavaRegExpTest {

	
	@Test
	public void shouldHaveGoodPerformance() throws Exception {
		String text = "216.67.1.91 - leon [01/Jul/2002:12:11:52 +0000] \"GET /index.html HTTP/1.1\" 200 431 \"http://www.loganalyzer.net/\" \"Mozilla/4.05; [en] (WinNT I)\" \"USERID=CustomerA;IMPID=01234\"";
		String regExp = SimpleQueryConvertor.convertSimpleToRegExp("(*)\\s+(*)\\s+(*)\\s+\\[(c26)\\]\\s+\"(*)\\s+(*)\\s+(*)\"))\\s+(\\d+)\\s+(\\d+)\\s+\"(*)\"\\s+\"(*\")\"");
		
		System.out.println(regExp);
//		Pattern pattern = Pattern.compile(regExp, Pattern.DOTALL | Pattern.MULTILINE);
		Pattern pattern = Pattern.compile(regExp, Pattern.DOTALL | Pattern.MULTILINE);
		printResults(text, pattern, true);
		
		// test performance
		long start = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			printResults(text, pattern, false);
		}
		long end = System.currentTimeMillis();
		System.out.println("elapsedMs:" + (end - start));


		
	}

	private void printResults(String text, Pattern pattern, boolean printIt) {
		Matcher matcher = pattern.matcher(text);
		matcher.matches();
		int groupCount = matcher.groupCount();
		for (int i = 0; i < groupCount; i++) {
			String group = matcher.group(i);
			if (printIt) System.out.println(i + ") " + group);
		}
	}
}
