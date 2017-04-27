package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.regex.SimpleQueryConvertor;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:29
 * To change this template use File | Settings | File Templates.
 */
public class FieldFactory {


    static boolean stringEmpty(String val) {
        return val == null || val.isEmpty();
    }
    static public FieldI getField(String name, String expression, String synthSrcField, int groupId, boolean visible, boolean summary, String function, boolean indexed) {

        FieldI result = null;
        if (stringEmpty(expression)) result =  new GroupField(name,groupId,visible,summary,function, indexed);
        else if (expression.startsWith("groovy-script:")) result = new GroovyField(name,groupId, visible, summary, expression, function, indexed);
        else if (expression.startsWith("mvel:")) result = new MvelField(name,groupId, visible, summary, expression, function, indexed);
        else if (expression.startsWith("jep:")) result = new JepField(name,groupId, visible, summary, expression, function, indexed);
        else if (expression.startsWith(SubstringField.SUBSTRING_TOKEN) || expression.startsWith(SubstringField.LASTSUBSTRING_TOKEN)) result = new SubstringField(name,groupId, visible, summary, expression, synthSrcField, function, indexed);
        else if (expression.startsWith(SplitField.SPLIT)) result = new SplitField(name,groupId, visible, summary, expression, synthSrcField, function, indexed);
        else  if (expression != null && SimpleQueryConvertor.isSimpleLogFiler(expression)) result = new JRegExField(name,groupId, visible, summary, expression, synthSrcField, function, indexed);
        // we werent given any groups so think its a literal reference i.e. 'level'
        else  if (!expression.contains("(")) result = new LiteralField(name,groupId, visible, summary,synthSrcField, function);
        else result = new JRegExField(name,groupId, visible, summary, expression, synthSrcField, function, indexed);
        result.setSynthSource(synthSrcField);
        return result;

    }


}
