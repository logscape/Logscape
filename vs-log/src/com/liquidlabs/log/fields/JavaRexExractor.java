package com.liquidlabs.log.fields;

import com.liquidlabs.common.regex.SimpleQueryConvertor;
import jregex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 08:49
 * To change this template use File | Settings | File Templates.
 */
public class JavaRexExractor implements Extractor {

    private final String regExp;
    private final java.util.regex.Pattern pattern;

    public JavaRexExractor(String expression) {
        regExp = SimpleQueryConvertor.convertSimpleToRegExp(expression);
        pattern = java.util.regex.Pattern.compile(regExp, Pattern.DOTALL | Pattern.MULTILINE);
    }

    public String[] extract(String nextLine) {
        java.util.regex.Matcher matcher = pattern.matcher(nextLine);
        if (!matcher.matches()) return com.liquidlabs.common.collection.Arrays.EMPTY_ARRAY;

        int groupCount2 = matcher.groupCount();
        String[] groups = new String[groupCount2];
        for (int i = 0; i < groupCount2; i++) {
            groups[i] = matcher.group(i+1);
        }
        return groups;
    }

    @Override
    public void reset() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

}
