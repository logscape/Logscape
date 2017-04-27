package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.fields.FieldSet;
import com.maxmind.geoip.LookupService;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 26/03/2013
 * Time: 17:37
 * To change this template use File | Settings | File Templates.
 */
public class GroovyScriptRunner {
    private static final Logger LOGGER = Logger.getLogger(GroovyScriptRunner.class);


    private String name;
    transient int depth = 0;

    static LookupService geoip = null;
    static {
        try {
            /**
             * This product includes GeoLite data created by MaxMind, available from
             <a href="http://www.maxmind.com">http://www.maxmind.com</a>.

             API Notes here:
             https://github.com/molindo/maxmind-geoip

             Example
             https://github.com/maxmind/geoip-api-java/blob/master/examples/CityLookup.java
             */
            String def = "system-bundles/lib-1.0/thirdparty/GeoLiteCity.dat";
            String ff = System.getProperty("geoip.db", def);
            if (!new java.io.File(ff).exists()) {
                ff ="../lib/lib/GeoLiteCity.dat";
            }
            geoip = new LookupService(new java.io.File(ff),LookupService.GEOIP_INDEX_CACHE);
        } catch (Throwable t) {
            LOGGER.warn(t.getMessage());
        }
    }

    public GroovyScriptRunner(String name) {

        this.name = name;
    }

    private static final ThreadLocal<Map<String,ScriptPair>> scriptCache = new ThreadLocal<Map<String,ScriptPair>>(){
        @Override
        protected Map<String,ScriptPair> initialValue()
        {
            return new HashMap<String,ScriptPair>();
        }
    };

    public static class ScriptPair {
        public ScriptPair(GroovyShell shell, Script script) {
            this.shell = shell;
            this.script = script;
        }
        private GroovyShell shell;
        private Script script;
        public void release() {
            shell.getClassLoader().clearCache();
        }
    }

    public String evalGroovyScript(String[] events, FieldSet fieldSet, String script, Set<String> evalContext, FieldI callField) {


        try {
            depth++;
            if (depth > 25) {
                System.err.println("Recursion Detected: Field:" + this.name);
                return null;
            }


            Binding binding = new Binding();
            if (evalContext.contains(this.name)) {
                LOGGER.warn("Recursive reference to [" + this.name + "] detected, Context:" + evalContext);
                throw new RuntimeException("Recursive reference to [" + this.name + "] detected, Context:" + evalContext);
            }
            evalContext.add(this.name);

            List<FieldI> synthScriptVars = callField.buildSynthScriptVars(this.name, fieldSet.getFields(), script, fieldSet, events, evalContext);

            int varsPut = 0;

            for (FieldI field : synthScriptVars) {
                String value = fieldSet.getFieldValue(field.name(), events);
                if (value == null || value.length() == 0) continue;
                if (value.endsWith("\n")) value = value.substring(0, value.length()-1);
                varsPut++;
                if (StringUtil.isIntegerFast(value)) {
                    binding.setVariable(field.name(), Long.valueOf(value));
                    continue;
                } else {
                    Double isDouble = StringUtil.isDouble(value);
                    if (isDouble != null) binding.setVariable(field.name(), new Double(value));
                    else binding.setVariable(field.name(), value);
                }
            }
            binding.setVariable("geoipLookup",geoip);

            if (varsPut == 0 && synthScriptVars.size() > 0) return null;

            script = script.replaceAll("\\$", " ");
            Map<String, ScriptPair> localMap = scriptCache.get();
            ScriptPair scriptPair = localMap.get(script);

            if (scriptPair == null) {
                GroovyShell shell = new GroovyShell(binding);
                String sscript = script.substring("groovy-script:".length());
                Script gscript = shell.parse(sscript);
                scriptPair = new ScriptPair(shell, gscript);
                localMap.put(script, scriptPair);
            }
            scriptPair.script.setBinding(binding);
            Object value = scriptPair.script.run();
            return value == null ? null : value.toString();

        } finally {
            depth--;
        }

    }
}
