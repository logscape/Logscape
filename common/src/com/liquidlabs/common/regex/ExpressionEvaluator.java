package com.liquidlabs.common.regex;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;
import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Takes in A AND B OR C and uses Stirng.indexOF - or if there are groups or *s falls back
 * to regex
 *
 */
public class ExpressionEvaluator {
	private final static Logger LOGGER = Logger.getLogger(ExpressionEvaluator.class);
	
	private List<String[]> stringIndexParts;

	private Pattern regPattern;

	private Matcher matcher;

	private MatchResult matchResult;

	final public MatchResult FALSE_matchResult;
	
	private boolean matchAll = false;
	private boolean evalError = false;

	final private MatchResult TRUE_matchResult;

	private String expressionStringMatchHead;
    private Pattern splitPattern;
    private boolean isSplitting;

    public ExpressionEvaluator(String expression){
		
		if (expression.equals("") || expression.equals("*") || expression.equals(".*")) matchAll = true;
		
		
		this.FALSE_matchResult = new MatchResult();
		this.FALSE_matchResult.setMatch(false);
		this.FALSE_matchResult.setGroups(new String[0]);
		
		this.TRUE_matchResult = new MatchResult();
		this.TRUE_matchResult.setMatch(true);
		this.TRUE_matchResult.setGroups(new String[0]);

        if (isSplitFunction(expression)) {
            createSplitConfig(expression);
            return;
        }

		
		boolean useStringIndex = !isRegExp(expression);
		if (useStringIndex) {
			this.stringIndexParts = getStringIndexParts(expression);
		} else {
			String regExp = SimpleQueryConvertor.convertSimpleToRegExp(expression);
			compilePattern(regExp);
			if (expression.indexOf("(") > 0 && !expression.startsWith(".")) {
				int startIndex = expression.indexOf("(");
				int indexOfStar = expression.indexOf("*");
				if (indexOfStar > 0 && indexOfStar < startIndex) {
					startIndex = indexOfStar;
				}
				int indexOfAND = expression.indexOf("AND");
				if (indexOfAND > 0 && indexOfAND < startIndex) {
					startIndex = indexOfAND;
				}
				expressionStringMatchHead = expression.substring(0, startIndex).trim();
				if (expressionStringMatchHead.indexOf("\\") != -1) expressionStringMatchHead = expressionStringMatchHead.substring(0, expressionStringMatchHead.indexOf("\\"));
			}
		}
	}

    private boolean isSplitFunction(String expression) {
        return expression.startsWith("split");
    }

    private void createSplitConfig(String expression) {
        String splitExpression = expression.substring("split(".length(), expression.length()-1);
        if (splitExpression.startsWith("\"")) {
            splitExpression = splitExpression.substring(1, splitExpression.length()-1);
        }
        splitPattern = new Pattern(splitExpression);
        this.isSplitting = true;

    }

    private MatchResult handleSplitLine(String line) {
        try {
            String[] split = splitPattern.tokenizer(line).split();
            if (split == null || split.length == 0) return FALSE_matchResult;
            return new MatchResult(Arrays.append(new String[] { "" } , split));
        } catch (Throwable t) {
            return FALSE_matchResult;
        }
    }


    private void compilePattern(String pattern){
		try {
			int flags = REFlags.MULTILINE | REFlags.DOTALL;
			if (ignoreCase) flags = flags | REFlags.IGNORE_CASE;
			String regExp = SimpleQueryConvertor.convertSimpleToRegExp(pattern);
			regPattern =  new jregex.Pattern(regExp, flags);
			matcher = regPattern.matcher();
		} catch (Throwable t) {
			LOGGER.warn("Cannot evalute:" + pattern + " EX:" + t.getMessage());
			evalError = true;
		}
	}


	List<String[]> getStringIndexParts(String expression) {
		List<String[]> results = new ArrayList<String[]>();
		String[] lines = expression.split(" OR ");
		for (String line : lines) {
			line = line.replaceAll("'", "");
			String[] linePart = line.split(" AND ");
			results.add(linePart);
		}
		return results;
	}


	boolean isRegExp(String expression) {
		return expression.indexOf("*") > -1 || expression.indexOf("(") > -1;
	}
	
	/**
	 * Return true if it hits
	 * @param line
	 * @return
	 */
	public MatchResult evaluate(String line) {
		if (matchAll) {
			TRUE_matchResult.groups = new String[] { line };
			return TRUE_matchResult;
		}

        if (isSplitting) {
            return handleSplitLine(line);
        }


		if (stringIndexParts !=  null) {
			if (evaluateStringIndex(line)) {
				TRUE_matchResult.groups = new String[] { line };
				this.matchResult = TRUE_matchResult;
			} else {
				TRUE_matchResult.groups = new String[0];
				this.matchResult = FALSE_matchResult;
			}
		} else {
			// short circuit so we dont have to regex every line
			if (expressionStringMatchHead != null && !line.contains(expressionStringMatchHead)) {
				this.matchResult = FALSE_matchResult;
			} else {
				boolean matches = matcher.matches(line);
				if (matches) {
					this.matchResult = new MatchResult(matcher, RegExpUtil.isMultiline(line));
				}
				else {
					this.matchResult = FALSE_matchResult;
				}
			}
		}
		return matchResult;
		
	}


    /**
	 * Case is 2x slower - 800Ms for 4million versus 1560Ms with case-off
	 */
	static boolean ignoreCase = System.getProperty("search.case.ignore", "true").equals("true");
	private boolean evaluateStringIndex(String line) {
		List<String[]> stringIndexParts2 = this.stringIndexParts;

		for (String[] strings : stringIndexParts2) {
			int matchCount = 0;
			for (String string : strings) {
				if (ignoreCase) {
					if (StringUtil.containsIgnoreCase(line, string)) matchCount++;
				}
                else if (line.indexOf(string) != -1) matchCount++;
			}
			if (matchCount == strings.length) return true;
			
		}
		
		return false;
	}
	/**
	 * Failed to construct something useful and cannot process with any match
	 * @return
	 */
	public boolean isEvalError() {
		return evalError;
	}

}
