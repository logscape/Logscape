package com.liquidlabs.log.fields;

import static java.lang.String.format;

class DelegatingMatcher implements ExpressionMatcher {

    private final FieldMatcher delegate;
    private final String regexp;

    public DelegatingMatcher(FieldMatcher delegate, String regexp) {
        this.delegate = delegate;
        this.regexp = regexp;
    }

    @Override
    public boolean isMatch(String part) {
        return delegate.isMatch(part);
    }

    @Override
    public FieldMatch createFieldMatch(String part) {
        if (isMatch(part)) {
            return new FieldMatch(delegate.createFieldMatch(part).guessedName(), regexp);
        }
        throw new RuntimeException(format("No match found for part %s - call isMatch first", part));
    }

    @Override
    public void resetCount() {
        delegate.resetCount();
    }
}
