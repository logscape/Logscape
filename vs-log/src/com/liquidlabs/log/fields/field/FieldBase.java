package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.fields.FieldSet;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 12:53
 * To change this template use File | Settings | File Templates.
 */
public abstract class FieldBase implements FieldI {

    private String name = "";
    private String funct;
    private boolean visible;
    private boolean summary;
    boolean index;
    private String literal; // not used but here for bw compatability serialization
    private String description;
    private int groupId;


    public FieldBase(){
    }
    public FieldBase(String name, int groupId, boolean visible, boolean summary, String function, boolean indexed) {
        this.name = name;
        this.groupId = groupId;
        this.visible = visible;
        this.summary = summary;
        this.funct = function;
        this.index = indexed;
    }
    public boolean isIndexed() {
        return index;
    }

    @Override
    public boolean isSummary() {
        return summary;
    }

    @Override
    public String name() {
        return name;
    }

    public int groupId() {
        return groupId;
    }

    public void setValue(String literal, String s) {
    }
    @Override
    public boolean isVisible() {
        return visible;
    }

    @Override
    public void setVisible(boolean b) {
        this.visible = b;
    }

    @Override
    public void setDescription(String s) {
        this.description = s;
    }

    @Override
    public void setSummary(boolean b) {
        this.summary = b;
    }

    @Override
    public void setIndexed(boolean b) {
        this.index = b;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public String funct() {
        return funct;
    }
    @Override
    public Number mapToView(Number rawValue) {
        if (description != null && description.trim().length() > 0 && description.startsWith("groovy-script:")) {
            String script = description;
            script = script.substring("groovy-script:".length());
            // bind this value
            Binding binding = new Binding();
            binding.setVariable(this.name, rawValue);

            GroovyShell shell = new GroovyShell(binding);
            Object value = shell.evaluate(script);
            if (value instanceof Number) return (Number) value;
            else {
                //LOGGER.warn("Dont know how to handle field:" + name + " the script returned value:" + value + ", it is not a Double or Integer value");
                return rawValue;
            }
        }
        return rawValue;


    }
    @Override
    public Object transform(Number thisValue, Map<String, Object> valueMap, FieldSet fieldSet) {
        if (description == null || description.length() == 0 && !this.description.contains("transform")) return thisValue;
        String str2 = "transform(groovy-script:";
        int index = description.indexOf(str2);
        if (index == -1) {
            str2 = "transform(\rgroovy-script:";
            index = description.indexOf(str2);
        }
        if (index == -1) return thisValue;

        int nextEnd = description.indexOf("\n)",index);
        if (nextEnd == -1) nextEnd = description.indexOf("\r)",index);
        if (nextEnd == -1) nextEnd = description.indexOf(")", index);
        if (nextEnd == -1) return "";

        String script = description.substring(str2.length() + index, nextEnd);
        Binding binding = new Binding();
        for (String key : valueMap.keySet()) {
            binding.setVariable(key, valueMap.get(key));
        }

        if (fieldSet != null) {
            // now bind the literal fields
            List<FieldI> literalFields = fieldSet.getFieldsByType(LiteralField.class);
            for (FieldI field : literalFields) {
                try {
                    String value = field.get(null, null, fieldSet);
                    if (value == null) continue;
                    if (StringUtil.isIntegerFast(value)) {
                        binding.setVariable(field.name(), StringUtil.isInteger(value));
                    } else if (StringUtil.isDouble(value) != null) {
                        binding.setVariable(field.name(), StringUtil.isDouble(value));
                    } else {
                        binding.setVariable(field.name(), value);
                    }
                } catch (Throwable t) {
                }
            }
        }

        binding.setVariable(this.name, thisValue);

        GroovyShell shell = new GroovyShell(binding);
        Object value = shell.evaluate(script);
        if (value instanceof Number) return (Number) value;
        else {
            return value;
        }
    }

    public boolean isTransformable() {
        return description.contains("transform");
    }

    transient List<FieldI> synthScriptVars;
    transient String discoKeySetString = "";
    @Override
    public List<FieldI> buildSynthScriptVars(String name, List<FieldI> fields, String script, FieldSet fieldSet, String[] events, Set<String> evalContext) {


        Map<String, String> discoFields = fieldSet.getDynamicFields();
        if (synthScriptVars == null ||  !discoKeySetString.equals(discoFields.keySet().toString())) {
            // heap chewer   - need to track if the set of discovered fields has changed......
            discoKeySetString = discoFields.keySet().toString();

            synthScriptVars = new ArrayList<FieldI>();
            // do discovered first so they can be overridden by explicit fields
            Map<String, String> discoveredFields = discoFields;
            for (String field : discoveredFields.keySet()) {
                if (field.equalsIgnoreCase(name)) continue;
                // only eval fields being reference in the script
                if (!script.contains(field)) continue;
                synthScriptVars.add(new LiteralField(field, 0, true, true, discoveredFields.get(field), "count()"));
            }

            for (FieldI field : fields) {
                if (field.name().equalsIgnoreCase(name)) continue;
                // only eval fields being reference in the script
                if (!script.contains(field.name())) continue;
                synthScriptVars.add(field);
            }


            // now parse out anything with '$xxx'
            String[] decls = script.split("\\$");
            for (String decl : decls) {
                decl = decl.trim();
                if (decl.contains(" ")) decl = decl.substring(0, decl.indexOf(" "));
                if (evalContext.contains(decl)) continue;
                String v = fieldSet.getFieldValue(decl, events);
                evalContext.add(decl);
                if (v != null) {
                    synthScriptVars.add(new LiteralField("$" +decl, 0, true, true, v, ""));
                    synthScriptVars.add(new LiteralField(decl, 0, true, true, v, ""));
                }
            }
        }
        return synthScriptVars;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        FieldBase fieldBase = (FieldBase) o;

        if (!name().equals(fieldBase.name())) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name().hashCode();
    }
    @Override
    public boolean isNumeric() {
        return this.funct().contains("avg") ||
                funct().contains("sum") ||
                funct().contains("min") ||
                funct().contains("max");

    }

    public FieldSet.Field toBasicField() {
        return new FieldSet.Field(this.groupId, this.name, this.funct, this.visible, this.summary, this.description, synthSource(), synthExpression(), index);

    }

    public String synthSource() { return ""; }
    public void setSynthSource(String source) {  }

    public String synthExpression() {
        return "";
    }

    public long longValue() {
        return 0;
    }
}
