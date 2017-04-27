package com.liquidlabs.common.regex;

import com.liquidlabs.common.collection.Arrays;

import java.util.ArrayList;
import java.util.List;

public class SimpleQueryConvertor {

//public static final String MATCH_NUMBER = "([-+]?\\d++\\.?\\d*+)";
public static final String MATCH_NUMBER = "([-+]?\\d+\\.?\\d*)";
private static final String RIGHT_PAR = ")";
private static final String LEFT_PAR = "(";
private static final String SPACE = " ";
private static final String NO_WHITESPACE = "\\S+";
private static final String STAR = "*";
	//	private static final String MATCH_ALL_INCLUDING_EOL = "(.|\\n|\\r)+";
	private static final String MATCH_ALL_INCLUDING_EOL = ".*";
	private static final String DOT_STAR = ".*";
	private static final char BACKSLASH = '\\';
	
	static public boolean isSimpleLogFiler(String logFilter) {
		if (logFilter.startsWith(DOT_STAR)) return false;
		// found a nested wildcard
		int wcardPos = logFilter.indexOf(STAR);
		if (wcardPos > -1 && (wcardPos == 0 || logFilter.getBytes()[wcardPos-1] != '.')) return true;
		
		// found our own stuff
		boolean foundOurOwnExpressions = logFilter.contains("(*") || logFilter.contains("(d)") || logFilter.contains("**") || logFilter.contains(" OR ") ||
		logFilter.contains(" AND ") || logFilter.contains("**") || logFilter.contains("(w)") || logFilter.contains("(s") || logFilter.contains("(c");
		if (foundOurOwnExpressions) return true;
		
		// return false if we didnt get regexp;
		boolean containsRegexp = logFilter.contains(DOT_STAR) || logFilter.contains("\\s+") || logFilter.contains("\\w+");
		return !containsRegexp;
	}
	

	public String[] convertArray(String[] includes) {
		String[] results = new String[includes.length];
		int pos = 0;
		for (String include : includes) {
			results[pos++] = convertSimpleToRegExp(include);
		}
		return results;
	}

	static public String convertSimpleToRegExp(String expression) {
		return new SimpleQueryConvertor().processExpression(expression);
	}

	public List<Part> getSections(String content) {
		ArrayList<Part> result = new ArrayList<Part>();
		if (content.trim().length() == 0) return result;
		char[] chars = content.toCharArray();
		Character previousChar = null;
		Part currentPart = new Part();
        for (int i = 0; i<chars.length; i++) {
            if (chars[i] == '(' && previousChar == null) {
                previousChar = chars[i];
                currentPart.grouped = true;
            } else if (chars[i] == ')' && previousChar != BACKSLASH) {
				result.add(currentPart);
				currentPart = new Part();
				previousChar = chars[i];
			} else if (chars[i] == '(' && previousChar != BACKSLASH) {
				result.add(currentPart);				
				currentPart = new Part();
				currentPart.grouped = true;
				previousChar = chars[i];
			} else {
			    currentPart.regexp = currentPart.regexp.append(chars[i]);
			    previousChar = chars[i];
            }
		}
		if (currentPart != null && currentPart.regexp.toString().length() > 0) {
			result.add(currentPart);
		}
		
		result.get(result.size()-1).isLast = true;
		
		return result;
	}
	
	public String processExpression(String userExpression) {
		
		if (!isSimpleLogFiler(userExpression)) {
			if (!userExpression.startsWith(DOT_STAR) && !userExpression.endsWith(DOT_STAR)) return DOT_STAR +"?" + userExpression + DOT_STAR;
			else return userExpression;
		}
		
		if (!(userExpression.contains("\\s+") || userExpression.contains(".*"))) {
			userExpression = replaceCorrectly(userExpression, "\\.");
			userExpression = replaceIfNotEscaped(userExpression, '[', userExpression.contains("(["));
			userExpression = replaceIfNotEscaped(userExpression, ']', userExpression.contains("(["));
			userExpression = replaceIfNotEscaped(userExpression, '|', false);
		}
		
		userExpression = fixTrailingStarToMatchAnything(userExpression);

		List<Part> sections = getSections(userExpression);
		StringBuilder result = new StringBuilder();
		for (Part part : sections) {
			result.append( getProcessedPart(part.isLast, part.grouped, part.regexp()));
		}
		String resultString = result.toString();
		if (!resultString.startsWith(DOT_STAR) && !resultString.startsWith("(.{")) 	{
			if (resultString.startsWith("^")) resultString = resultString.substring(1);
			else if (resultString.startsWith("(.*)")) resultString = "^" + resultString;
			else resultString = DOT_STAR + "?" + resultString;
		}
		if (!resultString.endsWith(DOT_STAR) && !resultString.endsWith("(.*)") )	resultString = resultString + DOT_STAR;
		return resultString.trim();
	}

	private String fixTrailingStarToMatchAnything(String userExpression) {
		// hack to allow a, non-grouped trailing star mean - anything from here on...
		// i.e. *.log* 
		if (userExpression.endsWith(STAR) && !userExpression.endsWith(".*")) {
			userExpression = userExpression.substring(0, userExpression.length()-1) + ".*";
		}
		return userExpression;
	}
	
	

	private String replaceCorrectly(String userExpression, String token) {
		StringBuilder result = new StringBuilder();
		byte[] bytes = userExpression.getBytes();
		
		for (int i = 0; i < bytes.length; i++) {
			byte b = bytes[i];
			if (i < bytes.length-1 && b == '.' && bytes[i+1] != '*' && (i > 0 && bytes[i-1] != '\\')) {
				result.append(token);
			} else {
				result.append((char) b);
			}
		}
		return result.toString();
	}
	String replaceIfNotEscaped(String userExpression, char token, boolean ignoreMe) {
		if (ignoreMe) return userExpression;
		StringBuilder result = new StringBuilder();
		byte[] bytes = userExpression.getBytes();
		
		for (int i = 0; i < bytes.length; i++) {
			char b = (char) bytes[i];
			if (charIsInGroup(i, userExpression)) {
				result.append(b);
				continue;
			}
			if (i > 0 && b == token && bytes[i-1] != '\\') {
				result.append("\\" + token);
			} else if (i == 0 && b == token) {
				result.append("\\" + token);
			} else {
				result.append((char) b);
			}
		}
		return result.toString();
	}

	public boolean charIsInGroup(int i, String userExpression) {
		String head = userExpression.substring(0, i);
		int lastLeftParen = head.lastIndexOf("(");
		int lastRightParen = head.lastIndexOf(")");
		
		return lastLeftParen > lastRightParen;
	}


	String getProcessedPart(boolean isLast, boolean isGrouped, String string) {
		// do me first
		boolean handled = false;
		if (string.contains("**")) {
			handled = true;
			string = handleSectionSplit(isGrouped, string);			
		} 
		if (string.contains(" *") || string.equals(STAR)) {
			handled = true;
			string = handleWildcard(isLast, isGrouped, string);
			
		} else if (string.contains(STAR) && !string.contains("(.*)")) {
			handled = true;
			string = handleWildcardSpecial(isLast, isGrouped, string);
			
		} else if (isGrouped && string.equals("w")) {
			handled = true;
			string = "(\\w+)";
		} else if (isGrouped && string.equals("?")) {
			handled = true;
			string = "(.*?)";
		} else if (isGrouped && string.startsWith("s")) {
			handled = true;
			string = handleSkipExpression(isGrouped, string);
		} else if (isGrouped && string.startsWith("c")) {
			handled = true;
			string = handleSkipExpression(isGrouped, string);
		} else if (isGrouped && string.startsWith("^")) {
			handled = true;
			string = "([" + string + "]+)";
		}
		if (string.contains(" AND ")) {
			handled = true;
			string = handleReplaceAll(" AND ", DOT_STAR,string);			
			string = string.replaceAll("'","");			
			
		}
		if (string.contains("\n") && !string.contains(MATCH_ALL_INCLUDING_EOL)) {
			handled = true;
			string = handleReplaceAll("\n", ".*?\\\\s+.*?",string);			
		}
		if (!string.equals("([^\\n]+)") && string.contains("\\n") && !string.contains(MATCH_ALL_INCLUDING_EOL)) {
			handled = true;
			string = handleReplaceAll("\\\\n", ".*?\\\\s+.*?",string);			
		}
		
		if (isGrouped && string.equals("d")) {
			handled = true;
			string = handleNumber(isGrouped, string);
		} else if (isGrouped && string.contains("d\\.")) {
			handled = true;
			string = handleNumber2(isGrouped, string);
		}
		// do me last = conflict with '*'s
		if (string.contains(" OR ")) {
			handled = true;
			if (isGrouped) 
			string = handleGroupedOR(string);
			else string = handleReplaceAll(" OR ", ".*__OR__.*", string);
		}
		// explicit group
		if (!handled && isGrouped) {
			string = LEFT_PAR + string + RIGHT_PAR;
		}

		return string;
	}


	private String handleSkipExpression(boolean isGrouped, String expression) {
		try {
			byte[] bytes = expression.getBytes();
			StringBuilder number = new StringBuilder();
			
			for (int i = 1; i < bytes.length; i++) {
				char b = (char) bytes[i];
				number.append(b);
			}
			int amount = Integer.parseInt(number.toString());
			return "(.{" + amount + "})";
		} catch (Throwable t) {
			if (isGrouped) return LEFT_PAR + expression + RIGHT_PAR;
			else return expression;
		}
	}


	private String handleWildcardSpecial(boolean isLast, boolean isGrouped, String expression) {
		StringBuilder result = new StringBuilder();
		byte[] bytes = expression.getBytes();
		
		for (int i = 0; i < bytes.length; i++) {
			char b = (char)bytes[i];
			//char b1 = (char)bytes[i+1];
			
			if (b == '*' && (i == 0 || (i > 0) && bytes[i-1] != '.') && i < bytes.length-1) {
				// handle the case where we got (*), - i.e. anything until the comma! or (*;)
				if (expression.length() == 2) {
					result.append("[^").append((char)bytes[i+1]).append("]+");
					i++;
					
				} else if (expression.length() == 3 && ((char)bytes[i+1]) == '\\') {
						result.append("[^\\").append((char)bytes[i+2]).append("]+");
						i++;
						i++;
						
					} else {
						// doesnt make sense to do white space here.....
					result.append(NO_WHITESPACE);
				}
			} else {
				if (b == '*' && bytes[i-1] != '.') result.append("\\S+");
				else result.append((char) b);
			}
		}
		String rString = result.toString();
		return isGrouped ? LEFT_PAR + rString + RIGHT_PAR : rString;

	}

	private String handleReplaceAll(String token, String replaceWith, String string) {
		return string.replaceAll(token, replaceWith);
	}

	private String handleNumber(boolean isGrouped, String string) {
		return MATCH_NUMBER;
	}
	private String handleNumber2(boolean isGrouped, String string) {
		return string.replaceAll("d", "\\\\d+");
	}

	final private String handleWildcard(boolean isLast, boolean isGrouped, String string) {
		String[] split = Arrays.split(SPACE, string);
		StringBuilder result = new StringBuilder();
		int pos = 0;
		for (String part : split) {
			if (part.equals(STAR)) {
					result.append(NO_WHITESPACE);
			} else {
				result.append(part);
			}
			if (pos < split.length-1) result.append(SPACE);
			pos++;
		}
		String rString = result.toString();
		String aresult = isGrouped ? LEFT_PAR + rString + RIGHT_PAR : rString;
		if (string.endsWith(" ")) aresult += " ";
		return aresult;
	}
	
	private String handleSectionSplit(boolean isGrouped, String string) {
		String[] split = Arrays.split(SPACE, string);
		StringBuilder result = new StringBuilder();
		int pos = 0;
		for (String part : split) {
			if (part.equals("**")) {
				if (!isGrouped) result.append(DOT_STAR);
				else result.append(MATCH_ALL_INCLUDING_EOL);
			} else if (part.contains("**")) {
				if (!isGrouped) result.append(part.replaceAll("\\*\\*", DOT_STAR));
				else result.append(part.replaceAll("\\*\\*", MATCH_ALL_INCLUDING_EOL));
			} else {
				result.append(part);
			}
			if (pos < split.length-1) result.append(SPACE);
			pos++;
		}
		String rString = result.toString();
		// preserve any trailing space char
		if (string.endsWith(" ")) rString += " ";
		return isGrouped ? LEFT_PAR + rString + RIGHT_PAR : rString;
	}

	String handleGroupedOR(String string) {
		String[] split = string.split(" OR ");
		StringBuilder result = new StringBuilder(LEFT_PAR);
		int pos = 0;
		for (String string2 : split) {
			result.append(string2);
			if (pos++ < split.length-1) result.append("|");
			
		}
		result.append(RIGHT_PAR);
		return result.toString();
	}



}
