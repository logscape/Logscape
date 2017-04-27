package com.liquidlabs.log.fields;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 08:45
 * To change this template use File | Settings | File Templates.
 */
public class OroExtractor implements Extractor {
    private final String regExp;
    private final Perl5Compiler oCompiler;
    private Pattern oPattern;
    private Perl5Matcher oMatcher;

    public OroExtractor(String expression) {
        regExp = SimpleQueryConvertor.convertSimpleToRegExp(expression);
        oCompiler = new Perl5Compiler();
        try {
            oPattern = oCompiler.compile(regExp, Perl5Compiler.DEFAULT_MASK	 | Perl5Compiler.SINGLELINE_MASK | Perl5Compiler.MULTILINE_MASK);
            oMatcher = new Perl5Matcher();
        } catch (MalformedPatternException e) {
            System.err.println("Bad Expression:" + regExp);
            e.printStackTrace();
        }
    }

    public String[] extract(String nextLine) {
        boolean matches = oMatcher.matches(nextLine, oPattern);
        if (!matches) return com.liquidlabs.common.collection.Arrays.EMPTY_ARRAY;

        MatchResult matchResult = new MatchResult(oMatcher, matches, true);
        matchResult.groups();
        if (matchResult.isMatch())
            return matchResult.getGroups();
        return com.liquidlabs.common.collection.Arrays.EMPTY_ARRAY;
    }

    @Override
    public void reset() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

}


