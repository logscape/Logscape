package com.liquidlabs.log.fields.field;

import com.liquidlabs.log.fields.FieldSet;

import java.io.Serializable;
import java.lang.String;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:26
 * To change this template use File | Settings | File Templates.
 */
public interface FieldI extends Serializable {
    String name();

    String get(String[] events, Set<String> evalContext, FieldSet fieldSet);

    boolean isSummary();

    String synthSource();

    void setSynthSource(String source);

    int groupId();

    boolean isVisible();

    void setVisible(boolean b);

    void setDescription(String s);

    void setValue(String literal, String s);

    void setSummary(boolean b);

    String description();

    String funct();

    Number mapToView(Number rawValue);

    Object transform(Number thisValue, Map<String, Object> valueMap, FieldSet fieldSet);

    List<FieldI> buildSynthScriptVars(String name, List<FieldI> fields, String script, FieldSet fieldSet, String[] events, Set<String> evalContext);

    boolean isNumeric();

    FieldSet.Field toBasicField();

    boolean isIndexed();

    void setIndexed(boolean b);
}
