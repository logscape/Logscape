package com.liquidlabs.log.fields;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldSetGuesser2 implements FieldGenerator{

	private static final String SPACE_DELIM = "\\s+";

	public FieldSet guessFieldSet(String[] srcLines, List<String> fieldNames, String delimited) {
		String[] lines = getReducedLines(srcLines);
		String token = SPACE_DELIM;
		if (delimited != null)
			token = delimited;
		
		String[][] splitLines = getSplitLines(srcLines, token);

		int numberOfItems = getNumberOfItems(splitLines);
		boolean allTheSameLength = isAllTheSameLength(splitLines);
		FieldSet fieldSet = new FieldSet("test", srcLines, "(**)", "*", 100);
		StringBuilder exprs = new StringBuilder();
		boolean stop = false;
		String year = Integer.valueOf(new DateTime().getYear()).toString();
//		System.out.println("ExpectedItemCount:" + numberOfItems
//				+ " AllSameLength:" + allTheSameLength);

		boolean isLastGivenFieldName = false;

		for (int position = 0; position < numberOfItems; position++) {
			if (stop || lines.length < 2)
				continue;
			String sample = splitLines[0][position];

			String possibleName = null;
			if (fieldNames != null && position < fieldNames.size())
				possibleName = fieldNames.get(position);

			if (fieldNames != null && position == fieldNames.size() - 1) {
				isLastGivenFieldName = true;
				stop = true;
			}

			String[] fieldNameTypeFunction = getFieldType(position, splitLines,
					token, sample, year, possibleName, isLastGivenFieldName);
			if (fieldNameTypeFunction != null) {
				// System.out.println(" Processing:" + position + "\t" +
				// possibleName + "\t" + " - " + sample + "\t" +
				// Arrays.toString(fieldNameTypeFunction));
			}

			// try and peek ahead - if the next item is good then grab this as a
			// chunk
			if (((allTheSameLength && fieldNameTypeFunction == null) || fieldNameTypeFunction == null)
					&& position + 1 < numberOfItems) {

				String sample2 = splitLines[0][position + 1];
				String[] fieldNameTypeFunction2 = getFieldType(position + 1, splitLines, token, sample2, year, possibleName,
						isLastGivenFieldName);
				if (allTheSameLength || fieldNameTypeFunction2 != null) {
					String fieldTag = "maybe" + position;
					String selection = "(*)";
					if (sample.startsWith("/"))
						fieldTag = "path" + position;
					if (sample.startsWith("http"))
						fieldTag = "url" + position;
					if (sample.split(":").length == 2) {
						fieldTag = sample.split(":")[0];
						selection = fieldTag + ":(*)";
					}
					if (fieldTag == null && sample.split("=").length == 2) {
						fieldTag = sample.split("=")[0];
						selection = fieldTag + "=(*)";
					}
					fieldNameTypeFunction = new String[] {
							possibleName != null ? possibleName : fieldTag,
							selection, "count()", "true" };
				}
			}

			if (fieldNameTypeFunction != null) {
				boolean summary = Boolean.valueOf(fieldNameTypeFunction[3]);
				fieldSet.addField(fieldNameTypeFunction[0], fieldNameTypeFunction[2], true, summary);
				if (exprs.length() > 0)
					exprs.append(token);
				exprs.append(fieldNameTypeFunction[1]);

			} else {
				System.out.println("Stopping");
				stop = true;
			}

		}
		int maxItems = getMaxItems(splitLines);
		if (!isLastGivenFieldName && maxItems != fieldSet.fields().size()) {
			fieldSet.addField("data", "count(*)", false, false);
			exprs.append(token + "(**)");
		}
		fieldSet.expression = exprs.toString();
		return fieldSet;
	}

	String[][] getSplitLines(String[] srcLines, String token) {
		boolean skipFirst = srcLines[0].startsWith("#");
		if(skipFirst){
			srcLines = Arrays.subArray(srcLines, 1, srcLines.length-1);
		}
		
		int arrLen = srcLines.length;
		if (!token.equals(" ") && !token.equals("\\s+")) {
			String[][] results = new String[arrLen][];
			for (int i = 0; i < srcLines.length; i++) {
				if (srcLines[i].length() == 0) continue;
				results[i] = srcLines[i].split(token);
			}
			return results;
		}
		
		
		Pattern pattern = Pattern.compile("(\"[^\"]+\"|\\S+)+");
		String[][] results = new String[arrLen][];
		for (int i = 0; i < srcLines.length; i++) {
			if (srcLines[i].length() == 0) continue;
			Matcher matcher = pattern.matcher(srcLines[i]);
			List<String> strings = new ArrayList<String>();
			while(matcher.find()){
				strings.add(matcher.group(1));
			}
			results[i] = strings.toArray(new String[0]);
		}
		return results;
		
	}

	public int getNumberOfItems(String[][] lines) {
		int length = Integer.MAX_VALUE;
		for (String[] string : lines) {
			length = Math.min(length, string.length);
		}
		return length;
	}

	public boolean isSameLength(int i, String[][] lines) {
		try {
			int length = lines[0][i].length();
			for (String[] string : lines) {
				if (length != string[i].length())
					return false;
			}
			return true;
		} catch (Throwable t) {
			return false;
		}
	}

	public boolean isAllDigit(int i, String[][] lines, String token) {
		String regexp = "(\\d+)";
		return matches(i, lines, token, regexp);
	}

	public boolean isAllDecimal(int i, String[][] lines, String token) {
		String regexp = SimpleQueryConvertor.MATCH_NUMBER;
		return matches(i, lines, token, regexp);
	}

	public boolean isAllWords(int i, String[][] lines, String token) {
		String regexp = "(\\w+)";
		return matches(i, lines, token, regexp);
	}
	public boolean isTime(int i, String[][] lines, String token) {
		String regexp = "(\\d\\d:\\d\\d:\\d\\d)";
		return matches(i, lines, token, regexp);
	}
	public boolean isDate(int i, String[][] lines, String token) {
		String string = lines[0][i];
		if (string.contains(new DateTime().getYear() + "")) {
			if (matches(i, lines, token, "(\\d\\d-\\d\\d-\\d\\d)")) return true;			
			if (matches(i, lines, token, "(\\d\\d\\d\\d-\\d\\d-\\d\\d)")) return true;
		} 
		return false;
	}
	
	/**
	 * Bit of a guess - it starts with a number, has 2x : i.e. 18:30:12.222 or 18:12:01
	 * @param i
	 * @param lines
	 * @param token
	 * @return
	 */
	public boolean isTime2(int i, String[][] lines, String token) {
		String string = lines[0][i];
		return string.length() > 0 && (StringUtil.isIntegerFast(string.charAt(0) + "") && string.split(":").length == 3);
	}



	public boolean isLCWords(int i, String[][] lines, String token) {
		String regexp = "([a-z]+)";
		return matches(i, lines, token, regexp);
	}

	public boolean isUCWords(int i, String[][] lines, String token) {
		String regexp = "([A-Z]+)";
		return matches(i, lines, token, regexp);
	}

	public boolean isInitCapsWords(int i, String[][] lines, String token) {
		String regexp = "([A-Z][a-z]+)";
		return matches(i, lines, token, regexp);
	}

	public boolean isIPAddress(int i, String[][] lines, String token) {
		String regexp = "(\\d+\\.\\d+\\.\\d+\\.\\d+)";
		return matches(i, lines, token, regexp);
	}

	public boolean isHostAddress(int i, String[][] lines, String token) {
		String regexp = "(\\w+\\.\\w+\\.\\w+)";
		return matches(i, lines, token, regexp);
	}

	public boolean matches(int i, String[][] lines, String token, String regexp) {
		try {
			for (String[] string : lines) {
				String wordMaybe = string[i];
				if (!wordMaybe.matches(regexp))
					return false;
			}
			return true;
		} catch (Throwable t) {
			return false;
		}
	}

	public boolean isSpecialChars(int pos, String[][] lines) {
		try {
			for (String[] line : lines) {
				if (isSpecialChar(line[pos]))
					return true;
			}

			return false;
		} catch (Throwable t) {
			// worst case
			return true;
		}
	}

	List<String> specialChars = Arrays.asList(".", "-", "&", "_", ",", "@",
			"/", "+", ":", ";");

	private boolean isSpecialChar(String string) {
		for (String charItem : specialChars) {
			if (string.indexOf(charItem) > -1)
				return true;
		}
		return false;
	}

	// strip out first line if it has #
	private String[] getReducedLines(String[] lines) {
		List<String> result = new ArrayList<String>();
		int pos = 0;
		for (String line : lines) {
			if (pos++ == 0 && line.startsWith("#"))
				continue;
			if (line.trim().length() > 0)
				result.add(line);
		}
		return Arrays.toStringArray(result);
	}

	private boolean isAllTheSameLength(String[][] lines) {

		int length = lines[0].length;
		for (String[] string : lines) {
			if (string.length != length)
				return false;
		}
		return true;
	}

	private String[] getFieldType(int i, String[][] lines, String token,
			String sample, String year, String possibleName, boolean isLastGiven) {
		if (isLastGiven == true) {
			return new String[] { possibleName, "(**)", "count()", "false" };
		}
		// 0 field spacer
		if (possibleName != null && possibleName.trim().length() == 0) {
			return new String[] { "spacer" + i, "(\\s+)", "count()", "false" };

		}
		
		if (isAllDigit(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "number" + i,
					"(\\d+)", "avg()", Boolean.valueOf(sample.length() <= 2).toString() };

		} else if (isAllDecimal(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "number" + i, "(d)",
					"avg()", "false" };

		} else if (isLCWords(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "word" + i, "(w)",
					"count()", "true" };

		} else if (isUCWords(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "WORD" + i, "(w)",
					"count()", "true" };

		} else if (isInitCapsWords(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "Word" + i, "(w)",
					"count()", "true" };
			
		} else if (isTime(i, lines, token) || isTime2(i,lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "Time" + i, "(*)",
							"count()", "false" };

		} else if (isDate(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "Date" + i, "(*)",
					"count()", "false" };

			// } else if (isAllWords(i, lines, token)) {
			// return new String[] { possibleName != null ? possibleName :
			// "word" + i, "(w)", "count()", "true" };

		} else if (isIPAddress(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "addr" + i,
					"(\\d+\\.\\d+\\.\\d+\\.\\d+)", "count()", "true" };

		} else if (!isSpecialChars(i, lines)) {
			if (sample.trim().length() > 0) {
				String matchExpr = "(*" + token + ")";
				if (token.equals(SPACE_DELIM))
					matchExpr = "(*)";

				return new String[] {
						possibleName != null ? possibleName : "item" + i,
						matchExpr, "count()", "true" };
			} else {
				// empty/optional sample value (i.e. 2 delimis next to each
				// other i.e. delimi is | and we get ||
				String matchExpr = "(?)";// + token + ")";

				return new String[] {
						possibleName != null ? possibleName : "item" + i,
						matchExpr, "count()", "true" };

			}

		} else if (isHostAddress(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "host" + i,
					"(\\w+\\.\\w+\\.\\w+)", "count()", "true" };

		} else if (isQuotedText(i, lines, token)) {
			return new String[] {
					possibleName != null ? possibleName : "text" + i, "\"([^\"]+)\"",
					"count()", "true" };

		} else if (isSameLength(i, lines)) {
			Boolean summary = true;
			String fieldName = "field";
			if (sample.contains(year)) {
				fieldName = "date";
				summary = false;
			}
			String matchExpr = "(*" + token + ")";
			if (token.equals(SPACE_DELIM))
				matchExpr = "(*)";
			return new String[] {
					possibleName != null ? possibleName : fieldName + i,
					matchExpr, "count()", summary.toString() };
		}
		if (possibleName != null) {
			String matchExpr = "(*" + token + ")";
			if (token.equals(SPACE_DELIM))
				matchExpr = "(*)";
			return new String[] { possibleName, matchExpr, "count()", "true" };

		}
		return null;
	}

	private boolean isQuotedText(int i, String[][] lines, String token) {
		try {
			for (String[] string : lines) {
				String string2 = string[i];
				boolean quoted = string2.startsWith("\"")
						&& string2.endsWith("\"");
				if (!quoted)
					return false;
			}
			return true;
		} catch (Throwable t) {
			return false;
		}
	}

	private int getMaxItems(String[][] lines) {
		int max = lines[0].length;
		for (String[] string : lines) {
			max = Math.max(string.length, max);
		}
		return max;
	}

	public List<String> getFieldNames(String firstLine) {
		if (!firstLine.startsWith("#"))
			return null;

		String delimiter = getDelimiterFromString(firstLine);
		firstLine = firstLine.replace("#", "");
		if (delimiter.equals("|"))
			delimiter = "\\|";
		//String[] split = firstLine.replace(" ","_").split(delimiter);
        String[] split = firstLine.split(delimiter);
        List<String> strings = Arrays.asList(split);
        List<String> results = new ArrayList<String>();
        for (int i = 0; i < strings.size(); i++) {
            String string = strings.get(i).replace(" ","-");
            if (results.contains(string)) string = string + "-" + i;
            results.add(string);

        }
        return results;
	}

	public String getDelimiterFromString(String firstLine) {
		if (firstLine.charAt(1) == ' ') {
			MatchResult matchResult = RegExpUtil.matchesJava(
					"#\\s+(\\w+)\\s+.*", firstLine);
			if (matchResult.isMatch())
				return SPACE_DELIM;
		}
		MatchResult matchResult = RegExpUtil.matchesJava(
				"#(\\w+)(\\W+)(\\w+).*", firstLine);
		if (matchResult.isMatch() && !firstLine.contains("\\t")) {
			String part2 = matchResult.group(2);
			if (!part2.equals("\t") && part2.trim().length() == 0)
				part2 = SPACE_DELIM;
			return part2;
		}

		String[] spaceParts = firstLine.split(" ");
		
		String[] commaSpaceParts = firstLine.split(", ");
		String[] commaParts = firstLine.split(",");
		String[] tabParts = firstLine.split("\t");
		String[] tabParts2 = firstLine.split("\\\\t");
		String[] pipeParts = firstLine.split("\\|");
		String[] semiParts = firstLine.split("\\|");
		
		
		if (commaSpaceParts.length > spaceParts.length)
			return ",";
		if (commaParts.length > spaceParts.length)
			return ",";
		if (tabParts.length > spaceParts.length)
			return "\t";
		if (tabParts2.length > spaceParts.length)
			return "\t";
		if (pipeParts.length > spaceParts.length) return "|";
		if (semiParts.length > spaceParts.length) return "|";
		
		return SPACE_DELIM;

	}

	public FieldSet guess(String[] srcText) {
		String[] text = getCleanedData(srcText);
		List<String> fieldNames = getFieldNames(text[0]);
		String delimiter = getDelimiterFromString(text[0]);
		FieldSet fieldSet = guessFieldSet(text, fieldNames, delimiter);
		fieldSet.example = srcText;
		if (fieldNames!= null && !delimiter.equals(SPACE_DELIM)) {
			fieldSet.expression = "split(" + delimiter + "," + fieldNames.size() + ")";
		}
		return fieldSet;

	}

	private String[] getCleanedData(String[] text) {
		return getCleanedFieldHeaders(text, shouldClean(text));
	}

	private String[] getCleanedFieldHeaders(String[] text, boolean shouldClean) {
		ArrayList<String> results = new ArrayList<String>();
		if (!shouldClean) {
			for (String line : text) {
				if (line.trim().length() > 0) {
					results.add(line);
				}
			}
			return Arrays.toStringArray(results);
		}

		boolean foundFieldsHashLine = false;
		for (String line : text) {
			if (line.length() == 0)
				continue;
			if (!foundFieldsHashLine) {
				if (line.contains("#Fields:")) {
					results.add(line.replace("#Fields: ", "#"));
					foundFieldsHashLine = true;
				}
				continue;
			} else {
				results.add(line);
			}
		}

		return Arrays.toStringArray(results);
	}

	private boolean shouldClean(String[] text) {
		return Arrays.toString(text).contains("#Fields:");
	}

}
