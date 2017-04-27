package com.liquidlabs.log.fields.field;

import com.liquidlabs.log.fields.FieldSet;

import java.lang.Override;
import java.lang.String;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:26
 * To change this template use File | Settings | File Templates.
 */
public class MvelField extends SynthFieldBase {

    enum SCHEMA { description, expr, funct, groupId, index, literal, name, summary, visible, synthSource };

    private String expr;

    transient MvelRunner mvelRunner;

    public MvelField(){}

    public MvelField(String name, int groupId, boolean visible, boolean summary, String expr, String function, boolean indexed) {
        super(name, groupId, visible, summary, function, indexed);
        this.expr = expr;
    }

    @Override
    public String get(final String[] events, Set<String> evalContext, FieldSet fieldSet) {
        if (mvelRunner == null) mvelRunner = new MvelRunner(name());
        return mvelRunner.evalMVELScript(events, fieldSet, expr, fieldSet.getId(), evalContext, this);
    }
    public String synthExpression() {
        return expr;
    }



}
