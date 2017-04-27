package com.liquidlabs.log.fields;

import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import jregex.Matcher;
import jregex.MatcherUtil;
import jregex.Pattern;
import jregex.REFlags;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 08:42
 * To change this template use File | Settings | File Templates.
 */
public class JRexExtractor implements Extractor {
    String expression;
    private final String regExp;
    private final Pattern jrPattern;
    private final Matcher jrMatcher;
    private final char[] aa = "".toCharArray();

    public JRexExtractor(String expression) {
        regExp = SimpleQueryConvertor.convertSimpleToRegExp(expression);
        jrPattern = new Pattern(regExp, REFlags.DOTALL | REFlags.MULTILINE);
        jrMatcher = jrPattern.matcher();


    }
    public String[] extract(final String nextLine) {

        boolean multiline = RegExpUtil.isMultiline(nextLine);
        if (multiline) {
            if (jrMatcher.matches(nextLine)) {
                return MatcherUtil.groups(jrMatcher, nextLine);
            } else {
                return com.liquidlabs.common.collection.Arrays.EMPTY_ARRAY;
            }
        }
        final boolean matches = jrMatcher.matches(nextLine);
        if (!matches) return com.liquidlabs.common.collection.Arrays.EMPTY_ARRAY;
        return MatcherUtil.groups(jrMatcher, nextLine);
    }
    @Override
    public void reset() {
        jrMatcher.setTarget(aa,0,1);
    }

}
