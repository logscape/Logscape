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
public class JepField extends SynthFieldBase {


    enum SCHEMA { description, expression, funct, groupId, index, literal, name, summary, visible, synthSource };


    transient JepScriptRunner jepRunner;

    public JepField(){}

    public JepField(String name, int groupId, boolean visible, boolean summary, String expression, String function, boolean indexed) {
        super(name, groupId, visible, summary, function, indexed);
        this.expression = expression;
    }

    @Override
    public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
        if (jepRunner == null) jepRunner = new JepScriptRunner(name());
//        try {
            return jepRunner.evalJEPScript(events, fieldSet, expression, evalContext, this);
//        } catch (Throwable t) {
//            //System.err.println("Failed to JEP:" + super.name() + " expr:" + expression + " ex:" + expression.toString());
//            return "";
//        }
    }
    public String synthExpression() {
        return expression;
    }

}
