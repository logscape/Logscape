package com.liquidlabs.common.regex;


import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.junit.Assert;
import org.junit.Test;

public class OroUtilTest {

	Perl5Compiler compiler = new Perl5Compiler();
	Perl5Matcher matcher = new Perl5Matcher();
	private Pattern pattern;
	
	
	@Test
	public void testWebLogGroupPerformance() throws Exception {
		  // Create Perl5Compiler and Perl5Matcher instances.
		String text = "216.67.1.91 - leon [01/Jul/2002:12:11:52 +0000] \"GET /index.html HTTP/1.1\" 200 431 \"http://www.loganalyzer.net/\" \"Mozilla/4.05; [en] (WinNT I)\" \"USERID=CustomerA;IMPID=01234\"";
		String regExp = SimpleQueryConvertor.convertSimpleToRegExp("(*)\\s+(*)\\s+(*)\\s+\\[(c26)\\]\\s+\"(*)\\s+(*)\\s+(*)\"))\\s+(\\d+)\\s+(\\d+)\\s+\"(*)\"\\s+\"(*\")\"");
		
		printMatches(text, regExp);
		
		// test performance
		long start = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			boolean matches = matcher.matches(text, pattern);
			if (!matches) {
				System.out.println("doh!");
			} else {
				MatchResult match = matcher.getMatch();
				int groups = match.groups();
				for (int ii = 0; ii < groups; ii++) {
					match.group(ii);
				}
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("elapsedMs:" + (end - start));
	}

	@Test
	public void testMultiLineMatch() throws Exception {
		
//		String text = "Node Compression:\n Removal and compression of internal btree nodes.\n second line\n";
		String text = "Checkpoints: Frequency and extent of checkpointing activity\r\n" + 
		"	lastCheckpointEnd=0x1/0x8b9e61\r\n" + 
		"	lastCheckpointId=26\r\n";// + 
//				"	lastCheckpointStart=0x1/0x88a36a\r\n" + 
//				"	nCheckpoints=26\r\n" + 
//				"	nDeltaINFlush=4,125\r\n";
		
//		String regExp = "Checkpoints: (.*)";
//		String regExp = "(.*)\\n^(.*)\\n^(.*)";
		String regExp = "(.*)\\n^(.*)";
//		String regExp = "(.*)";
		printMatches(text, regExp);
	}
	
	
	private void printMatches(String text, String regExp) throws MalformedPatternException {
		
		System.out.println("--------------- Results");
		System.out.println(" RegExp:" + regExp);
//		pattern = compiler.compile(regExp, Perl5Compiler.DEFAULT_MASK | Perl5Compiler.SINGLELINE_MASK | Perl5Compiler.MULTILINE_MASK);
		pattern = compiler.compile(regExp, Perl5Compiler.DEFAULT_MASK | Perl5Compiler.SINGLELINE_MASK | Perl5Compiler.MULTILINE_MASK);
		boolean matches = matcher.matches(text,  pattern);
		System.out.println("matcher mline:" + matcher.isMultiline());
		if (!matches) {
			System.out.println("DIDNT MATCH:" + text);
			return;
		}
		MatchResult match = matcher.getMatch();
		int groups = match.groups();
		for (int i = 0; i < groups; i++) {
			System.out.println(i + ") " + match.group(i));
		}
	}
	

	
	@Test
	public void testIt01() throws Exception {
		
		String regExp = "(.*)\t(.*)\t.*\t(.*)\t.*\t.*\t.*\t.*\t.*\t.*";
		String text = "103	Thu Jun 11 12:00:15 BST 2009	6216111	3	PS Scavenge	179	3.0	1119	14.0	6216233	5.0	1070923776	1073741824	1070923776	305975376";
		String testRegExp = RegExpUtil.testJRegExp(regExp, text);
		System.out.println(testRegExp);
		Assert.assertTrue("Should have got groups", !testRegExp.contains("No match"));
	}

	
	@Test
	public void testItJRegExcp() throws Exception {
		
		String regExp = ".*\t(.*)\t.*\t.*\t.*\t.*\t(.*)\t.*\t.*\t.*\t.*\t.*\t.*";
		String text = "103	Thu Jun 11 12:00:15 BST 2009	6216111	3	PS Scavenge	179	3.0	1119	14.0	6216233	5.0	1070923776	1073741824	1070923776	305975376";
		String testRegExp = RegExpUtil.testJRegExp(regExp, text);
		System.out.println(testRegExp);
		Assert.assertTrue("Should have got groups", !testRegExp.contains("No match"));
	}
	
}
