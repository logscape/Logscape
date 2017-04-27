package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.log.fields.FieldSet;
import jregex.Matcher;
import jregex.MatcherUtil;
import jregex.Pattern;
import jregex.REFlags;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:23
 * To change this template use File | Settings | File Templates.
 */
public class JRegExField extends FieldBase{
    private String expression;
    private String synthSrcField;
    private String synthRegExp;

    transient Matcher jregexpMatcher;
    transient String stringHeader;
    transient Pattern jregexpPattern;



    public JRegExField(){}

    public JRegExField(String name, int groupId, boolean visible, boolean summary, String expression, String synthSrcField, String function, boolean indexed) {
        super(name, groupId, visible, summary, function, indexed);
        this.expression = expression;
        this.synthSrcField = synthSrcField;
        synthRegExp = SimpleQueryConvertor.convertSimpleToRegExp(expression);
    }

    @Override
    public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
        if (jregexpPattern == null) {
            jregexpPattern = new jregex.Pattern(synthRegExp, REFlags.MULTILINE | REFlags.DOTALL);
            jregexpMatcher = jregexpPattern.matcher();
            if (!expression.contains(".") && expression.split("\\(").length == 2 && expression.contains("(*)")) {
                stringHeader = expression.substring(0, expression.indexOf("("));
            }
        }

        String sourceValue = fieldSet.getFieldValue(synthSrcField, events);
        if (sourceValue == null) return null;

        // PERFORMANCE this is much faster than messing around with patterns so do it first!
        if (stringHeader != null && !sourceValue.contains(stringHeader)) return null;



        boolean multiline = RegExpUtil.isMultiline(sourceValue);
        if (multiline) {
            if (jregexpMatcher.matches(sourceValue)) {
                return MatcherUtil.groups(jregexpMatcher, sourceValue)[0];//groupId()-1];
            } else {
                return null;
            }
        } else {
            if (!jregexpMatcher.matches(sourceValue)) return null;
            return MatcherUtil.groups(jregexpMatcher, sourceValue)[0];//groupId()-1];
        }
    }
    public String synthExpression() {
        return expression;
    }

    public String synthSource() {
        return synthSrcField == null ? "" : synthSrcField;
    }


}
