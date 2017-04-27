package com.liquidlabs.log.fields.field;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.fields.FieldSet;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 11:43
 * To change this template use File | Settings | File Templates.
 */
public class LiteralField extends FieldBase {
    private String value;

    public LiteralField(){}

    public LiteralField(String name, int groupId, boolean visible, boolean summary, String value, String function) {
        super(name, groupId, visible, summary, function, false);
        this.value = value;
    }

    @Override
    public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
        return value;
    }

    public void setValue(String literal, String s) {
        this.value = literal;
    }
    public String getValue(){
        return this.value;
    }

    public String toString() {
        return "LiteralField - " + name() + ":" + value;
    }
    public String synthExpression() {
        return value;
    }
    public long longValue() {
        return StringUtil.longValueOf(value);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LiteralField that = (LiteralField) o;

        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
