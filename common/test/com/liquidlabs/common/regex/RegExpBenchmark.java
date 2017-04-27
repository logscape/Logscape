package com.liquidlabs.common.regex;

import java.util.regex.Pattern;

import org.junit.Test;

public class RegExpBenchmark {
	
	
	@Test
	public void numericPerformance() throws Exception {
		
		int limit = 50 * 10000;
//		benchmarkExpression(".*?(\\d+\\.?\\d*)", limit);
//		benchmarkExpression(".*?(\\d+\\.?\\d*?)", limit);
		while (true) {
		benchmarkExpression(".*?(\\d+\\.?\\d*)", limit);
		benchmarkExpression(".*?(\\d+\\.?\\d*+)", limit);
		benchmarkExpression(".*?(\\d++\\.?\\d*+)", limit);
		}
//		benchmarkExpression(".*?([0-9]+\\.?[0-9]*)", limit);
//		benchmarkExpression(".*?([0-9]+\\.?[0-9]*?)", limit);
		
	}


	private void benchmarkExpression(String expr1, int limit) {
		long start = System.currentTimeMillis();
		for (int i = 0; i < limit; i++) {
			String line = i + " stuff:" + i + "." + i;
			MatchResult match = RegExpUtil.matchesJava(expr1, line);
			
//			Pattern pattern = Pattern.compile(expr1, Pattern.DOTALL | Pattern.MULTILINE);
//			java.util.regex.Matcher matcher = pattern.matcher(line);
//			if (!matcher.matches()) {
			if (!match.isMatch()) {
				System.out.println("ERROR Failed to match:" + expr1);
				return;
			}
			
		}
		long end = System.currentTimeMillis();
		System.out.println(expr1 + "\t elapsed:" + (end - start));
	}


}
