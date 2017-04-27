package com.liquidlabs.common.expr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds a BlockChain which is used to
 * a) determine match
 * b) extract groups
 * Blocks like
 * (*) (a+) (A+) (aA+) (1+) (* * *) (**)
 */
public class BlockBuilder {
    static List<Block> blocks = Arrays.asList(  new Block[]{new AnyBlock(""),
                                                            new TEXTBlock(""),
                                                            new AlphaNumericBlock(""),
                                                            new AlphaTextBlock(""),
                                                            new NumericBlock(""),
                                                            new GreedyBlock("")});

    public static List<Block> build(String pattern) {
        String[] segments = pattern.split("\\(");
        List<Block> results = new ArrayList<Block>();
        for (String segment : segments) {
            if (segment.length() == 0) continue;
            else segment = "(" + segment;
            Block found = null;
            for (Block block : blocks) {
                found = block.isMe(segment);
                if (found != null) {
                    results.add(found);
                    break;
                }
            }
            if (found == null) throw new RuntimeException("Cannot Parse:" + pattern + " Got;" + results);
        }
        return results;
    }

    public static List<String> match(List<Block> build, String string) {
        List<String> result = null;
        int pos = 0;
        for (Block block : build) {
            String text = block.get(pos, string);
            if (text == null) return null;
            if (result == null) result = new ArrayList<String>();
            result.add(text);
            pos += block.skip();
        }
        return result;
    }
}
