package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.fields.FieldSet;
import org.apache.log4j.Logger;
import org.cheffo.jeplite.JEP;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 26/03/2013
 * Time: 17:18
 * To change this template use File | Settings | File Templates.
 */
public class JepScriptRunner {
    private static final Logger LOGGER = Logger.getLogger(JepScriptRunner.class);
    private String name;
    public JepScriptRunner(String name) {

        this.name = name;
    }
    transient boolean printedEX = false;
    transient Map<String,JEP> jepScriptCache = new HashMap<String,JEP>();

    public String evalJEPScript(String[] events, FieldSet fieldSet, String script, Set<String> evalContext, FieldI callField) {

        if (evalContext.contains(this.name)) {
            throw new RuntimeException("Recursive reference to [" + this.name + "] detected, Context:" + evalContext);
        }
        evalContext.add(this.name);
        if (jepScriptCache == null) jepScriptCache = new HashMap<String, JEP>();

        JEP jepScript = jepScriptCache.get(script);

        Map<String,Double> variables = null;
        if (jepScript == null) variables = new HashMap<String,Double>();
        boolean varsPut = buildJEPVars(events, fieldSet, script, fieldSet.getId(), evalContext, variables, jepScript, callField);
        if (!varsPut) return null;


        if (jepScriptCache == null) jepScriptCache = new HashMap<String, JEP>();
        if (jepScript == null) {
            printedEX = false;
            String sscript = script.substring("jep:".length());
            if (sscript.contains(" return ")) {
                sscript = sscript.replace("return", "");
            }
            if (sscript.contains("\n")) sscript = sscript.replaceAll("\n", " ");
            sscript = sscript.replaceAll("\\$", " ");
            jepScript = new JEP();
            jepScript.addStandardConstants();
            jepScript.addStandardFunctions();
            jepScriptCache.put(script,  jepScript);
            try {
                // need to add vars before calling parse
                for (String key : variables.keySet()) {
                    jepScript.addVariable(key, variables.get(key));
                }
                jepScript.parseExpression(sscript);
            } catch (Exception e) {
            }
        }
        if (variables != null) {
            for (String key : variables.keySet()) {
                jepScript.addVariable(key, variables.get(key));
            }
        }
        try {
            double value = jepScript.getValue();
            //double result = (double)Math.round(value * 10000) / 10000;
//					return Double.valueOf(value).toString();
            return StringUtil.doubleToString(value);

        } catch (Exception e) {
            // clear out the jep cache.Jep.Parser can get into an unrecoverable error state if it was built at a time when the referred vars didnt exist
            jepScriptCache.clear();

            if (!printedEX) {
                printedEX = true;
                throw new RuntimeException("Field:" + this.name + " Cannot evaluate", e);
            }
            return "";
        }
    }


    private boolean buildJEPVars(String[] events, FieldSet fieldSet, String script, String fieldSetId, Set<String> evalContext, Map<String, Double> variables, JEP jepScript, FieldI callField) {
        List<FieldI> synthScriptVars = callField.buildSynthScriptVars(name, fieldSet.getFields(), script, fieldSet, events, evalContext);
        int varsPut = 0;

        for (FieldI field : synthScriptVars) {
            String value = fieldSet.getFieldValue(field.name(), events);
            if (value == null || value.length() == 0) continue;
            try {
                if (value.endsWith("\n")) value = value.substring(0, value.length()-1);
                Double double1 = StringUtil.isDouble(value);
                varsPut++;
                if (double1 == null && value.indexOf("E-") == -1) continue;
                if (double1 == null) double1 = Double.parseDouble(value);
                if (variables == null && jepScript != null) {

                    jepScript.addVariable(field.name(), double1);
                } else {
                    variables.put(field.name(), double1);
                }
            } catch (Throwable t){
                LOGGER.warn("FieldEvalFailed:" + this.name + " ex:" + t.toString(),t);
            }
        }
        return synthScriptVars.size() == 0  || varsPut > 0;
    }
}
