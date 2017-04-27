package com.liquidlabs.log.fields.field;

import com.liquidlabs.log.fields.FieldSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 04/04/2013
 * Time: 16:27
 * To change this template use File | Settings | File Templates.
 */
public class FieldDTO implements FieldI {
    private boolean indexed;
    public String name;
    public String funct;
    public boolean visible;
    public boolean summary;
    public boolean index;
    public String description;
    public int groupId = -1;
    public String synthSrcField = "";
    public String synthExpression = "";

    public FieldDTO(){}

    public FieldDTO(String name, String funct, boolean visible, boolean summary, String description, int groupId, String synthSrcField, String synthExpression, boolean indexed) {
        this.name = name;
        this.funct = funct;
        this.visible = visible;
        this.summary = summary;
        this.description = description;
        this.groupId = groupId;
        this.synthSrcField = synthSrcField;
        this.synthExpression = synthExpression;
        this.indexed = indexed;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isSummary() {
        return this.summary;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String synthSource() {
        return this.synthSrcField;
    }

    @Override
    public void setSynthSource(String source) {
        this.synthSrcField = source;
    }

    @Override
    public int groupId() {
        return this.groupId;
    }

    @Override
    public boolean isVisible() {
        return this.visible;
    }

    @Override
    public void setIndexed(boolean b) {
    }

    @Override
    public void setVisible(boolean b) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setDescription(String s) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(String literal, String s) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setSummary(boolean b) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String description() {
        return this.description;
    }

    @Override
    public String funct() {
        return this.funct;
    }

    @Override
    public Number mapToView(Number rawValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object transform(Number thisValue, Map<String, Object> valueMap, FieldSet fieldSet) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<FieldI> buildSynthScriptVars(String name, List<FieldI> fields, String script, FieldSet fieldSet, String[] events, Set<String> evalContext) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isNumeric() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public FieldSet.Field toBasicField() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isIndexed() {
        return this.indexed;
    }
}
