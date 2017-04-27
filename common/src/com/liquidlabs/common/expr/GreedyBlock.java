package com.liquidlabs.common.expr;

/**
 * Does (**) i.e. to the end of line
 */
public class GreedyBlock extends BaseBlock {
    static String TOKEN = "(**)";

    public Block isMe(String sample) {
        if (sample.startsWith(TOKEN)) {
            return new GreedyBlock(sample);
        } else return null;
    }
    public GreedyBlock(String chunk) {
        super(TOKEN, chunk);
    }
    public int skip(){
        return -1;
    }

    public String get(int pos, String string) {
        return string.substring(pos,string.length());
    }

    boolean isGood(char c) {
        return true;
    }
}
