package com.liquidlabs.transport.serialization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.liquidlabs.common.collection.Arrays;

public class Matchers {

	public Matcher[] rules = new Matcher[] {
			new MatchAll("all"),
			new StringContains("contains"),
			new StringNotContains("notContains"),
			new StringContainsAny("containsAny"),
			new StringNotContainsAny("notContainsAny"),
			new StringEquals("equals"),
            new StringStartsWith("startsWith"),
			new StringNotEquals("notEquals"),
			new StringEqualsAny("equalsAny"),
			new NumberEquals("=="),
			new NumberGreaterEquals(">="),
			new NumberGreaterEquals(">>"),
			new NumberGreaterThan(">"),
			new NumberLessEqualThan("<="),
			new NumberLessThan("<")
	};
	Map<String, Matcher> matchers = new HashMap<String, Matcher>();
	
	public Matchers() {
		for (Matcher matcher : rules) {
			matchers.put(matcher.getKey(), matcher);
		}
	}
	public boolean isMatcherColumnBased(String[] templates) {
		for (String template : templates) {
			String[] templateName = template.split(":");
			Matcher matcher = matchers.get(templateName[0]);
			if (matcher != null) if (matcher.isColumnBased()) return true;
		}
		return false;
	}
	public int getMatcherColumnCount(String[] templates) {
		int columns = 1;
		for (String template : templates) {
			String[] templateName = template.split(":");
			Matcher matcher = matchers.get(templateName[0]);
			if (matcher != null) {
				if (matcher.isColumnBased())  columns = Math.max(columns, matcher.columnCount(templateName[1]));
			}
		}
		return columns;
	}


	public String[][] trimSearchTemplate(String[] searchTemplate) {
		String[][] result = new String[2][searchTemplate.length];
		
		for (int i = 0; i < searchTemplate.length; i++) {
				String templateItem = searchTemplate[i];
				if (templateItem == null || templateItem.length() == 0) {
					result[0][i] = null;
					result[0][i] = null;
				} else {
					templateItem = templateItem.trim();
					int indexOf = templateItem.indexOf(":");
					if (templateItem.startsWith("AND:") || templateItem.startsWith("OR:")) {
						indexOf = templateItem.indexOf(":", indexOf+1);
					}
					if (indexOf == -1) {
						result[0][i] = null;
						result[1][i] = templateItem;
					} else {
						result[0][i] = templateItem.substring(0, indexOf).trim();
						result[1][i] = templateItem.substring(indexOf+1, templateItem.length()).trim();
					}
				}
		}
		return result;
	}


	public boolean isMatch(String[] itemLookup, String[] searchTemplate, int column) {
		String[][] trimSearchTemplate = trimSearchTemplate(searchTemplate);
		return isMatch(itemLookup, trimSearchTemplate[0], trimSearchTemplate[1], true, column);
	}
	public boolean isMatch(String[] itemLookup, String[] operators, String[] searchTemplate, boolean trimTemplate, int column) {
		if (searchTemplate.length == 0) return false;
		if (itemLookup == null) return false;
		int currentTemplateMatchCount = 0;
		int expectedTemplateMatchCount = 0;
		
		int sourcePos = 0;
		int andCount = 0;
		
		if (operators.length != searchTemplate.length) throw new RuntimeException("Given bad operator arg!");
		
		for (int templatePos = 0; templatePos < searchTemplate.length; templatePos++) {
			String templateItem = searchTemplate[templatePos];
			String operator = operators[templatePos];
			
			// valid items and valid length
			if (operator != null && operator.equals("all")) {
			} else if (canBailOnTemplate(itemLookup, templatePos, templateItem)){
				sourcePos++;
				continue;
			}
			
			String sourceItem = itemLookup[sourcePos];
			if (trimTemplate) {
				templateItem = templateItem.trim();
				sourceItem = sourceItem.trim();
			}
			
			expectedTemplateMatchCount++;
			
			// weirdness for "OR" handling - its done in-line
			if (isOrOperatorNext(operators, searchTemplate, templatePos)) {
				boolean matched = isMatch(sourceItem, operator, templateItem, column);
				while (templatePos+1 < searchTemplate.length && operators[templatePos+1].startsWith("OR:")){
					String orTemplate[] = operators[templatePos+1].trim().split(":");
					matched = matched || isMatch(sourceItem, orTemplate[1], searchTemplate[templatePos], column);
					templatePos++;
				}
				if (matched) currentTemplateMatchCount++;
				sourcePos++;
				continue;
			}
			
			
			// Increment EXPECTATION for AND arguments - they also need to evaluate to TRUE
			if (operator!= null && operator.startsWith("AND:")) {
				// DO NOT increment sourcePos - as it still matching on previous argument
				andCount++;
				continue;
			}
			
			if (isMatch(sourceItem, operator, templateItem, column)) {
				currentTemplateMatchCount++;
				sourcePos++;
			} else {
				// short circuit evaluation
				return false;
			}
		}
		if (andCount > 0) {
			currentTemplateMatchCount += processANDs(itemLookup, operators, searchTemplate, column);
		}
		return expectedTemplateMatchCount == currentTemplateMatchCount && expectedTemplateMatchCount > 0;
	}
	private boolean isOrOperatorNext(String[] operators, String[] searchTemplate, int templatePos) {
		return templatePos+1 < searchTemplate.length && searchTemplate[templatePos+1] != null && operators[templatePos+1] != null && operators[templatePos+1].startsWith("OR:");
	}
	private boolean canBailOnTemplate(String[] itemLookup, int templatePos, String templateItem) {
		return templatePos >= itemLookup.length || itemLookup[templatePos] == null || templateItem == null || templateItem.length() == 0;
	}

	private boolean isMatch(String source, String operator, String templateItem, int column){
		if (operator != null && operator.length() > 0){
			Matcher matcher = matchers.get(operator);
			if (matcher != null) return matcher.match(source, templateItem, column);
		} 
		// allow fall back to simple - rules - i.e. ,A will match ,A
		// String.contains is SLOW
		if (source.contains(templateItem)) {
			return true;
		}
		return false;

	}
	int processANDs(String[] itemLookup, String[] operators, String[] searchTemplate, int column) {
		List<AND> array = getANDArray(operators);
		int result = 0;
		for (AND and : array) {
			if (itemLookup.length >= and.fieldPos) {
				String string = itemLookup[and.fieldPos].trim();
				if (isMatch(string, and.template, searchTemplate[and.andIndex], column)) result++;
			}
		}
		return result;
	}

	public static class MatchAll extends MatcherImpl {
		public MatchAll(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			return true;
		}
	}

	public static class StringNotContains extends MatcherImpl {
		public StringNotContains(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			return arg2.length() > 0 && !arg1.contains(arg2);
		}
	}
	public static class StringContainsAny extends MatcherImpl {
		public StringContainsAny(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			String[] rhsSplit = Arrays.split(",", arg2);
			if (rhsSplit.length > column) {
				String trim = rhsSplit[column].trim();
				return trim.length() > 0 && arg1.contains(trim);
			}
			
//			for (String rhsItem : rhsSplit) {
//				if (arg1.contains(rhsItem.trim())) return true;
//			}
			return false;
		}
		public boolean isColumnBased() {
			return true;
		}
		public int columnCount(String arg0) {
			return arg0.split(",").length;
		}
	}
	public static class StringNotContainsAny extends MatcherImpl {
		public StringNotContainsAny(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			String[] rhsSplit = Arrays.split(",", arg2);
			for (String rhsItem : rhsSplit) {
				rhsItem = rhsItem.trim();
				if (rhsItem.length() > 0 && arg1.contains(rhsItem)) return false;
			}
			return true;
		}
	}
	public static class StringContains extends MatcherImpl {
		public StringContains(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			String[] rhsSplit = Arrays.split(",", arg2);
			for (String rhsItem : rhsSplit) {
				rhsItem = rhsItem.trim();
				if (rhsItem.length() > 0 && !arg1.contains(rhsItem)) return false;
			}
			return true;
		}
	}
	
	public static class StringEquals extends MatcherImpl {
		public StringEquals(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			return arg1.equals(arg2);
		}
	}
    public static class StringStartsWith extends MatcherImpl {
        public StringStartsWith(String key) {
            super(key);
        }
        public boolean match(String arg1, String arg2, int column) {
            return arg1.startsWith(arg2);
        }
    }
	
	public static class StringNotEquals extends MatcherImpl {
		public StringNotEquals(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			return !arg1.equals(arg2);
		}
	}
	
	public static class StringEqualsAny extends MatcherImpl {
		public StringEqualsAny(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			
			String[] rhsSplit = Arrays.split(",", arg2);
			String rhsValue = rhsSplit[column].trim();
			String[] lhsSplit = arg1.split(",");
			for (String lhsItem : lhsSplit) {
				lhsItem = lhsItem.trim();
				if (rhsValue.startsWith("\"")) rhsValue = rhsValue.substring(1);
				if (rhsValue.endsWith("\"")) rhsValue = rhsValue.substring(0,rhsValue.length()-1);
				if (lhsItem.equals(rhsValue.trim())) return true;
			}
			return false;
		}
		public boolean isColumnBased() {
			return true;
		}
		public int columnCount(String arg0) {
			return arg0.split(",").length;
		}
	}
	public static class NumberEquals extends MatcherImpl {
		public NumberEquals(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			try {
				double arg1Int = Double.parseDouble(arg1);
				double arg2Int = Double.parseDouble(arg2);
				return arg1Int == arg2Int;
			} catch (Throwable t) {
				return false;
			}
		}
	}
	public static class NumberGreaterEquals  extends MatcherImpl {
		public NumberGreaterEquals(String key) {
			super(key);
		}
		
		public boolean match(String arg1, String arg2, int column) {
			try {
				double arg1Int = Double.parseDouble(arg1);
				double arg2Int = Double.parseDouble(arg2);
				return arg1Int >= arg2Int;
			} catch (Throwable t) {
				return false;
			}
		}
	}
	public static class NumberGreaterThan extends MatcherImpl {
		public NumberGreaterThan(String key) {
			super(key);
		}
		
		public boolean match(String arg1, String arg2, int column) {
			try {
				double arg1Int = Double.parseDouble(arg1);
				double arg2Int = Double.parseDouble(arg2);
				return arg1Int > arg2Int;
			} catch (Throwable t) {
				return false;
			}
		}
	}
	public static class NumberLessEqualThan extends MatcherImpl {
		public NumberLessEqualThan(String key) {
			super(key);
		}

		public boolean match(String arg1, String arg2, int column) {
			try {
				double arg1Int = Double.parseDouble(arg1);
				double arg2Int = Double.parseDouble(arg2);
				return arg1Int <= arg2Int;
			} catch (Throwable t) {
				return false;
			}
		}
	}
	public static class NumberLessThan extends MatcherImpl {
		public NumberLessThan(String key) {
			super(key);
		}
		public boolean match(String arg1, String arg2, int column) {
			try {
				double arg1Int = Double.parseDouble(arg1);
				double arg2Int = Double.parseDouble(arg2);
				return arg1Int < arg2Int;
			} catch (Throwable t) {
				return false;
			}
		}
	}
	public static abstract class MatcherImpl implements Matcher {
		final String key;

		public MatcherImpl(String key){
			this.key = key;
		}
		public String getKey() {
			return key;
		}
		public boolean isApplicable(String type) {
			return type.contains(key);
		}
		String getArgSplit(String token, String itemToSubString) {
			return itemToSubString.substring(itemToSubString.indexOf(token, key.length())+1, itemToSubString.length());
		}
		public boolean isColumnBased() {
			return false;
		}
		public int columnCount(String string) {
			return 0;
		}

	}
	public Map<String, Matcher> getMatchers() {
		return matchers;
	}
	public List<AND> getANDArray(String[] strings) {
		List<AND> result = new ArrayList<AND>();
		int fieldPos = -1;
		int andIndex = 0;
		for (String string : strings) {
			if (string != null && string.startsWith("AND:")) {
				result.add(new AND(string, fieldPos, andIndex));
			} else {
				fieldPos++;
			}
			andIndex++;
		}
		return result;
	}
	public static class AND {
		final String template;
		final int fieldPos;
		final int andIndex;

		public AND(String template, int fieldPos, int andIndex) {
			this.template = template.substring("AND:".length(), template.length());
			this.fieldPos = fieldPos;
			this.andIndex = andIndex;
		}
	}
}
