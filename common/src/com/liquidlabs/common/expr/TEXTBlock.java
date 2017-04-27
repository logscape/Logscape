package com.liquidlabs.common.expr;

/**
 * Does (A+)
 */
public class TEXTBlock extends BaseBlock {
    static String TOKEN = "(A+)";

    public Block isMe(String sample) {
        if (sample.startsWith(TOKEN)) {
            return new TEXTBlock(sample);
        } else return null;
    }
    public TEXTBlock(String chunk) {
        super(TOKEN, chunk);
    }

    protected boolean isGood(char c) {
        return c >= bigFrom && c <= bigTo;
    }
}
