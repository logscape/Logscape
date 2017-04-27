package com.liquidlabs.common.expr;

/**
 * Does (1+)
 */
public class NumericBlock extends BaseBlock {
    static String TOKEN = "(1+)";

    public Block isMe(String sample) {
        if (sample.startsWith(TOKEN)) {
            return new NumericBlock(sample);
        } else return null;
    }
    public NumericBlock(String chunk) {
        super(TOKEN, chunk);
    }

    protected boolean isGood(char c) {
        return  c >= numFrom && c <= numTo || c == dot;
    }
}
