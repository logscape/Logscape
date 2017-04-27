package com.liquidlabs.common;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;

import jregex.Matcher;
import jregex.Pattern;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.common.regex.MatchResult;

public class StringExtractorTest {
	
//	String filename = "/Volumes/Media/logs/AccuWeather/www_100709.esw3c_U.201011210000-2400-28";
	String filename = "test-data/lineCount.txt";
	private Pattern jregexpPattern;
	private java.util.regex.Pattern javaFieldPattern;
	private Perl5Compiler oFCompiler;
	private org.apache.oro.text.regex.Pattern oFPattern;
	private Perl5Matcher oFMatcher;
	
	@Test
	public void shouldExtractSubstringLinesFromFile() throws Exception {
		RAF raf = RafFactory.getRaf(filename, "");
		String line = "";
		
		long start = DateTimeUtils.currentTimeMillis();
		
		int hits = 0;
		int lines = 0;
		while ((line = raf.readLine()) != null) {
//			String result = com.liquidlabs.common.StringUtil.substring(line, "strAppID","&");
			String result = com.liquidlabs.common.StringUtil.substring(line, "grid","log");
//			//System.out.println("Result:" + result);
			if (result != null) hits++;
			lines++;
		}
		raf.close();
		long end = DateTimeUtils.currentTimeMillis();
		System.out.println("Elasped:" + (end - start) + " hits:" + hits + " lines:" + lines);
		assertEquals(20950, hits);
	}
	
	@Test
	public void shouldExtractUsingORO() throws Exception {
		RAF raf = RafFactory.getRaf(filename, "");
		String line = "";
		
		long start = DateTimeUtils.currentTimeMillis();
		
		int hits = 0;
		int lines = 0;
		while ((line = raf.readLine()) != null) {
//			MatchResult mr = getValueUsingORORegEx(".*?strAppID=(\\w+).*", line);
//			MatchResult mr = getValueUsingJAVARegEx(".*?strAppID=(\\w+).*", line);
//			MatchResult mr = getValueUsingJRegEx(".*?strAppID=(\\w+).*", line);
			
			MatchResult mr = getValueUsingORORegEx(".*?grid(\\w+).*", line);
//			MatchResult mr = getValueUsingJRegEx(".*?grid(\\w+).*", line);
//			System.out.println(" mr:" + mr.isMatch() + " line:" + line);
			if (mr.isMatch()) hits++;
			lines++;
		}
		raf.close();
		long end = DateTimeUtils.currentTimeMillis();
		System.out.println("Elasped:" + (end - start) + " hits:" + hits + " lines:" + lines);
		assertEquals(20950, hits);
	}
	private MatchResult getValueUsingJRegEx(String expr, String srcFieldData) {
		if (jregexpPattern == null) jregexpPattern = new jregex.Pattern(expr);
		Matcher matcher = jregexpPattern.matcher(srcFieldData);
		return new MatchResult(matcher, false);
	}
	private MatchResult getValueUsingJAVARegEx(String expr, String srcFieldData) {
		if (javaFieldPattern == null) javaFieldPattern = java.util.regex.Pattern.compile(expr);

		java.util.regex.Matcher matcher = javaFieldPattern.matcher(srcFieldData);
		return new MatchResult(matcher,false);
	}
	private MatchResult getValueUsingORORegEx(String expr, String srcFieldData) {
			if (oFCompiler == null) {

				oFCompiler = new Perl5Compiler();
				try {
					oFPattern = oFCompiler.compile(expr);//, Perl5Compiler.DEFAULT_MASK | Perl5Compiler.SINGLELINE_MASK);
//							| Perl5Compiler.MULTILINE_MASK | Perl5Compiler.READ_ONLY_MASK);
					oFMatcher = new Perl5Matcher();
				} catch (MalformedPatternException e) {
					e.printStackTrace();
				}
			}

			boolean matches = oFMatcher.matches(srcFieldData, oFPattern);
			return  new MatchResult(oFMatcher, matches,false);
	}


}
