package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.fields.FieldSet;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:22
 * To change this template use File | Settings | File Templates.
 */
public class SplitField extends FieldBase {
    static final String SPLIT = "split,";
    private String expression;
    private String synthSrcField;

    public SplitField(){}

    public SplitField(String name, int groupId, boolean visible, boolean summary, String expression, String synthSrcField, String function, boolean indexed) {
        super(name, groupId, visible, summary, function, indexed);
        this.expression = expression;
        this.synthSrcField = synthSrcField;
    }

    /**
     * split,\n,10 or split,\s,5 or split,\|
     * @return
     */
    @Override
    public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
        String cleaned = expression.substring(SPLIT.length());
        String[] parts = cleaned.split(",");
        int itemToReturn = Integer.parseInt(parts[parts.length-1]);
        String token = parts[0];
        if (token.equals("\\s")) token = " ";
        if (token.equals("\\|")) token = "|";
        if (token.equals("\\n")) token = new String(new char[] { 10 });
        if (token.equals("\\t")) token = new String(new char[] { 9 });
        String value = fieldSet.getFieldValue(synthSrcField,events);
        return StringUtil.splitFastSCAN(value, token.charAt(0), itemToReturn);
    }

    public String synthExpression() {
        return expression;
    }

    public String synthSource() {
        return synthSrcField == null ? "" : synthSrcField;
    }


}
