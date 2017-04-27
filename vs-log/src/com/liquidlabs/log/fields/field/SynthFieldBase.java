package com.liquidlabs.log.fields.field;

import com.liquidlabs.log.fields.FieldSet;

import java.util.Set;

/**
 * Created by neil on 19/01/16.
 */
public abstract class SynthFieldBase extends FieldBase {

    private String synthSource = "";

    public String expression = "";

    public SynthFieldBase(){

    }
    public SynthFieldBase(String name, int groupId, boolean visible, boolean summary, String function, boolean indexed) {
        super(name, groupId, visible, summary, function, indexed);
    }
    public String synthSource() {
        return this.synthSource;
    }
    public void setSynthSource(String source) {
        this.synthSource = source;
    }

    public String synthExpression() {
        return expression == null ? "" : expression;
    }

    public void setSynthExpression(String synthExpression) {
        this.expression = synthExpression;
    }
}
