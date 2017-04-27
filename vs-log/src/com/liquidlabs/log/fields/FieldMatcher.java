package com.liquidlabs.log.fields;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

class FieldMatcher implements ExpressionMatcher {
    private List<String> expressions = new ArrayList<String>();
    private int matchCount;
    private String autoFieldName;

    public FieldMatcher(String autoFieldName, List<String> matchingExpressions) {
        this.autoFieldName = autoFieldName;
        expressions.addAll(matchingExpressions);

    }

    public boolean isMatch(String part) {
        return match(part) != null;
    }

    private String match(String part) {
        for (String expression : expressions) {
            if (part.matches(expression)) {
                return expression;
            }
        }
        return null;
    }

    public FieldMatch createFieldMatch(String part) {
        String match = match(part);
        if (match != null) {
            matchCount++;
            return new FieldMatch(matchCount > 1 ? format("%s%d", autoFieldName, matchCount - 1) : autoFieldName, match);
        }
        throw new RuntimeException("No match found for part - call isMatch first");
    }

    public void resetCount() {
        matchCount = 0;
    }
}
