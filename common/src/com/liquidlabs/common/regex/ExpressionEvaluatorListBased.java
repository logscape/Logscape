package com.liquidlabs.common.regex;

import com.liquidlabs.common.collection.Arrays;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ExpressionEvaluatorListBased {
    private List<Item> orListWithANDs;
    public static class Item {
        String[] matching;

        public Item(String[] matching) {
            this.matching = matching;
        }

    }


    public ExpressionEvaluatorListBased(List<Item> orListWithANDs) {
        this.orListWithANDs = cleanIt(orListWithANDs);
    }

    private List<Item> cleanIt(List<Item> andListWithORS) {
        List<Item> results = new ArrayList<Item>();
        for (int i = 0; i < andListWithORS.size(); i++) {
            String[] strings = andListWithORS.get(i).matching;
            List<String> cleaned = new ArrayList<String>();
            for (String s : strings) {
                if (!s.contains("*")) cleaned.add(s);
            }
            if (cleaned.size() > 0) results.add(new Item(Arrays.toStringArray(cleaned)));
        }
        return results;
    }

    public boolean evaluate(String nextLine) {
        if (orListWithANDs.isEmpty()) return true;
        int hits = 0;
        for (Item ors : orListWithANDs) {
            for (int i = 0; i < ors.matching.length; i++) {
                String orItem = ors.matching[i];

                    if (StringUtils.containsIgnoreCase(nextLine, orItem)) {
                        hits++;
                        i = ors.matching.length;
                }
            }
        }
        return hits == orListWithANDs.size();
    }
}
