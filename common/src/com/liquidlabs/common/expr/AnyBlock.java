package com.liquidlabs.common.expr;

/**
 * Does (*)
 */
public class AnyBlock extends BaseBlock {
    static String TOKEN = "(*)";
    private final char stopChar;

    public Block isMe(String sample) {
        if (sample.startsWith(TOKEN)) {
            return new AnyBlock(sample);
        } else return null;
    }
    public AnyBlock(String chunk) {
       super(TOKEN,chunk);
        if (remainder.length() > 0) stopChar = remainder.charAt(0);
        else stopChar = ' ';
    }
    protected boolean isGood(char c) {
        return c != stopChar;
    }
}
