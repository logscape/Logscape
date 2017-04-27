package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.fields.FieldSet;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:21
 * To change this template use File | Settings | File Templates.
 */
public class SubstringField extends FieldBase {
    protected static final String SUBSTRING_TOKEN = "substring,";
    protected static final String LASTSUBSTRING_TOKEN = "lastsubstring,";

    private String expressionControl;
    transient String sstringFrom;
    transient String sstringTo;
    private String synthSrcField;

    public SubstringField(){}

    public SubstringField(String name, int groupId, boolean visible, boolean summary, String expression, String synthSrcField, String function, boolean indexed) {
        super(name, groupId, visible, summary, function, indexed);
        this.synthSrcField = synthSrcField;
        this.expressionControl = expression;
    }

    @Override
    public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
//        private String handleSubStringExtraction(String expressionControl, String srcFieldData) {
        String srcFieldData = fieldSet.getFieldValue(synthSrcField, events);
        if (srcFieldData == null) return null;
        if (sstringFrom == null) {
            String[] split = StringUtil.splitFast(expressionControl, ',', 3, false);
//				String[] split2 = expressionControl.split(",");
            if (split.length == 2) {
                sstringFrom = split[1];
            } else {
                sstringFrom = split[1];
                sstringTo = split[2];

            }
        }
        if (sstringFrom == null) return null;
        if (sstringTo == null) {
            if (expressionControl.contains(LASTSUBSTRING_TOKEN))return safeTrim(StringUtil.lastSubstring(srcFieldData, sstringFrom, " "));
            else  return StringUtil.substring(srcFieldData, sstringFrom, " ");
        } else {
            if (expressionControl.contains(LASTSUBSTRING_TOKEN)) return safeTrim(StringUtil.lastSubstring(srcFieldData, sstringFrom, sstringTo));
            else {
                if (sstringTo.equals("\\n") || sstringTo.equals("\n")) {
                    if (srcFieldData.contains("\r\n")) sstringTo = "\r\n";
                    else sstringTo = "\n";
                    return safeTrim(StringUtil.substring(srcFieldData, sstringFrom, sstringTo)).trim();
                } else {
                    return safeTrim(StringUtil.substring(srcFieldData, sstringFrom, sstringTo)).trim();
                }
            }
        }
    }

    private String safeTrim(String resultSting) {
        if (resultSting == null) resultSting = "";
        return resultSting.trim();
    }
    public String synthExpression() {
        return expressionControl;
    }

    public String synthSource() {
        return synthSrcField == null ? "" : synthSrcField;
    }


}
