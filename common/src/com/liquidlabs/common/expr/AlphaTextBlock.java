package com.liquidlabs.common.expr;

/**
 * Does (A+)
 */
public class AlphaTextBlock extends BaseBlock {
    static String TOKEN = "(Aa+)";

    public Block isMe(String sample) {
        if (sample.startsWith(TOKEN)) {
            return new AlphaTextBlock(sample);
        } else return null;
    }
    public AlphaTextBlock(String chunk) {
        super(TOKEN, chunk);
    }

    protected boolean isGood(char c) {
        return c >= bigFrom && c <= bigTo ||
                c >= smallFrom && c <= smallTo;
    }
}
