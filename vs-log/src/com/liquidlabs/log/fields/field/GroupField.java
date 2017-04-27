package com.liquidlabs.log.fields.field;

import com.liquidlabs.log.fields.FieldSet;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 27/03/2013
 * Time: 14:52
 * To change this template use File | Settings | File Templates.
 */
public class GroupField extends FieldBase {
    public GroupField(){
    }
    public GroupField(String name, int groupId, boolean visible, boolean summary, String function, boolean indexed) {
        super(name,groupId,visible,summary,function, indexed);
    }

    @Override
    public String get(String[] events, Set<String> evalContext, FieldSet fieldSet) {
        if(groupId() > events.length) {
            return "";
        }
        return events[groupId()-1];
    }
}
