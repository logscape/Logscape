package com.liquidlabs.common.regex;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;

public class StringSplitEval {

	int length = 0;
	private char splitChar;

	public StringSplitEval(String expression) {
		expression = expression.replace("\\t", "\t");
		String params = expression.substring("split(".length());
		int split = params.lastIndexOf(",");
        if (split == 0) split =1;
		splitChar = params.substring(0, split).charAt(0);
        try {
		    length = Integer.parseInt(params.substring(split+1, params.length()-1));
        } catch (Throwable t) {
        }
	}

	public MatchResult evaluate(String line) {
        if (length == 0) {
            String[] splits = StringUtil.splitFast(line, splitChar);
            return new MatchResult(Arrays.append(new String[] { line } , splits));

        } else {
            String[] splits = StringUtil.splitFast(line, splitChar, length, true);
            return new MatchResult(splits);
        }
	}
	public String[] evaluateGroups(String line) {
        if (length == 0) return StringUtil.splitFast(line, splitChar);
		return StringUtil.splitFast(line, splitChar, length, false);
		
	}

}
