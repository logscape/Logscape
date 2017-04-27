package com.liquidlabs.common.regex;

import java.util.ArrayList;
import java.util.List;

public class CsvParser {
    private String toParse;


    public CsvParser(String toParse) {
        this.toParse = toParse;
    }

    public List<String> parse() {
        ArrayList<String> strings = new ArrayList<String>();
        char[] chars = toParse.toCharArray();
        StringBuilder builder = new StringBuilder();
        boolean quoted = false;
        for(int i = 0; i < chars.length; i++) {
            if (chars[i] == '"') {
                builder.append(chars[i]);
                quoted = !quoted;
            } else if(quoted || chars[i] != ',') {
                builder.append(chars[i]);
            } else if (chars[i] == ','){
                strings.add(builder.toString());
                strings.add(",");
                builder = new StringBuilder();
            }
        }
        if (builder.length() > 0) {
            strings.add(builder.toString());
        } 
        return strings;
    }
}
