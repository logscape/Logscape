package com.liquidlabs.log.fields;

public interface ExpressionMatcher {
    boolean isMatch(String part);

    FieldMatch createFieldMatch(String part);

    void resetCount();
}
