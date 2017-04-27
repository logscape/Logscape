package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.fields.FieldSet;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 26/03/2013
 * Time: 17:40
 * To change this template use File | Settings | File Templates.
 */
public class MvelRunner {

    private String name;

    public MvelRunner(String name) {

        this.name = name;
    }
    transient Map<String,Serializable> mvelScriptCache = new HashMap<String,Serializable>();
    public String evalMVELScript(String[] events, FieldSet fieldSet, String script, String fieldSetId, Set<String> evalContext, FieldI callField) {

        Map variables = new HashMap();
        List<FieldI> synthScriptVars = callField.buildSynthScriptVars(this.name, fieldSet.getFields(), script, fieldSet, events, evalContext);

        if (synthScriptVars.size() > 0 && evalContext.contains(this.name)) {
            throw new RuntimeException("Recursive reference to [" + this.name + "] detected, Context:" + evalContext);
        }
        evalContext.add(this.name);

        int varsPut = 0;
        for (FieldI field : synthScriptVars) {

            String value = field.get(events, evalContext, fieldSet);
            if (value == null || value.length() == 0) continue;
            if (value.endsWith("\n")) value = value.substring(0, value.length()-1);
            varsPut++;
            if (StringUtil.isIntegerFast(value)) {
                variables.put(field.name(), Long.valueOf(value));
                continue;
            } else {
                Double isDouble = StringUtil.isDouble(value);
                if (isDouble != null) variables.put(field.name(), new Double(value));
                else variables.put(field.name(), value);
            }
        }
        if (synthScriptVars.size() > 0 && varsPut == 0) return null;
        if (mvelScriptCache == null) mvelScriptCache = new HashMap<String, Serializable>();
        Serializable script2 = mvelScriptCache.get(script);
        if (script2 == null) {
            String sscript = script.substring("mvel:".length());
            script2 = MVEL.compileExpression(sscript);
            mvelScriptCache.put(script,  script2);
        }
        Object result = MVEL.executeExpression(script2, variables);
        return result == null ? null : result.toString();
    }

}
