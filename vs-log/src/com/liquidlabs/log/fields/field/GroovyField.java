package com.liquidlabs.log.fields.field;

import com.liquidlabs.log.fields.FieldSet;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:24
 * To change this template use File | Settings | File Templates.
 */
public class GroovyField extends SynthFieldBase {


    enum SCHEMA { description, expression, funct, groupId, index, literal, name, summary, visible, synthSource };

    transient GroovyScriptRunner groovyRunner;

    public GroovyField(){
    }

    public GroovyField(String name, int groupId, boolean visible, boolean summary, String expression, String function, boolean indexed) {
        super(name, groupId, visible, summary, function, indexed);
        this.expression = expression;
    }

    public String get(final String[] events, Set<String> evalContext, FieldSet fieldSet) {
        if (groovyRunner == null) groovyRunner = new GroovyScriptRunner(name());
        return groovyRunner.evalGroovyScript(events, fieldSet, expression, evalContext, this);
    }
    public String synthExpression() {
        return expression;
    }

}
