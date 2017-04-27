package com.liquidlabs.log.fields.field;

import com.liquidlabs.log.fields.FieldSet;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:46
 * To change this template use File | Settings | File Templates.
 */
public class AppendingField extends FieldBase {
    private String synthSrcField;

    public AppendingField(){}

    public AppendingField(String name, int groupId, boolean visible, boolean summary, String synthSrcField, String function, boolean indexed) {
        super(name, groupId,visible,summary, function, indexed);
        this.synthSrcField = synthSrcField;
    }

    @Override
    public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
        String[] splitSources = synthSrcField.split(",");
        StringBuilder result = new StringBuilder();
        for (String sourceField : splitSources) {
            sourceField = sourceField.trim();
            String fieldValue = fieldSet.getFieldValue(sourceField, events);
            if (fieldValue != null) {
                if (result.length() > 0) result.append(",");
                result.append(fieldValue);
            } else return null;
        }
        return result.toString();
    }
    public String synthSource() {
        return synthSrcField == null ? "" : synthSrcField;
    }



}
