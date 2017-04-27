package com.liquidlabs.common.expr;

/**
 * Does (A+)
 */
public class AlphaNumericBlock extends BaseBlock {
    static String TOKEN = "(A1+)";

    public Block isMe(String sample) {
        if (sample.startsWith(TOKEN)) {
            return new AlphaNumericBlock(sample);
        } else return null;
    }
    public AlphaNumericBlock(String chunk) {
        super(TOKEN, chunk);
    }

    protected boolean isGood(char c) {
        return (c >= smallFrom && c <= smallTo ||
                c >= bigFrom && c <= bigTo ||
                c >= numFrom && c <= numTo)
                ;
    }
}
