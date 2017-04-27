package com.liquidlabs.log.search.filters;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.functions.FunctionBase;

import java.util.List;


public class GreaterThan extends FunctionBase implements Filter  {

	private static final long serialVersionUID = 1L;
	private String tag;
	private String group;
	public Number greaterThanThis;
	
	public GreaterThan() {}
	
	public GreaterThan(String tag, String group, Number greaterThanThis) {
		this.tag = tag;
		this.group = group;
		this.greaterThanThis = greaterThanThis;
	}
	public String toStringId() {
		return String.format("%s %s %s %s", getClass().getSimpleName(), tag, group, greaterThanThis.toString());
	}
	
	public String group() {
		return group;
	}
	public boolean isAppledAtFinalAgg() {
		return true;
	}
	public boolean isNumeric() {
		return true;
	}
	public Object value() {
		return greaterThanThis;
	}
	transient Integer usingGroupId;
	public boolean isPassed(FieldSet fieldSet, String[] events, String lineData, MatchResult matchResult, int lineNumber) {
		if (usingGroupId == null) {
			usingGroupId = super.groupIsInt(group) ? Integer.parseInt(group) : -1;
		}
		if (usingGroupId == 0) return true;
		if (usingGroupId > -1) {
			String groupValue = matchResult.getGroup(usingGroupId);
			return execute(groupValue);
		} else {

			if (fieldSet.isMultiField(group)) {
				List<String> applyFields = fieldSet.getMultiFields(group);
				for (String applyField : applyFields) {
					final String value = fieldSet.getFieldValue(applyField, events);
					if (execute(value)) return true;
				}
				return false;

			} else {
				String val = fieldSet.getFieldValue(group, events);

				// if the field returned null - see if its a real field....
				if (val == null) {
					// no value - no passing
					if (fieldSet.getField(group) == null) return false;
				}
				return execute(val);
			}
		}
	}

	final public boolean execute(String val) {
		try {
			if (val == null) return false;
			Double value = StringUtil.isDouble(val);
			if (value == null) return false;
			return value > greaterThanThis.doubleValue();
		} catch(NumberFormatException nfe) {
		}
		// be optimistic - as agg-level process may operation on - count() values
		return false;

	}

	public String getTag() {
		return tag;
	}
    public String[] values() {
        return new String[0];
    }


}
