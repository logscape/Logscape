package com.liquidlabs.log.search.filters;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.functions.FunctionBase;

import java.util.List;


public class LessThan extends FunctionBase implements Filter {

	private static final long serialVersionUID = 1L;
	
	private String tag;
	private String group;
	private Number lessThanThis;
	
	public LessThan() {}
	
	public LessThan(String tag, String group, Number lessThanThis) {
		this.tag = tag;
		this.group = group;
		this.lessThanThis = lessThanThis;
	}
	public String toStringId() {
		return String.format("%s %s %s %s", getClass().getSimpleName(), tag, group, lessThanThis);
	}
	
	public boolean isAppledAtFinalAgg() {
		return true;
	}
	public boolean isNumeric() {
		return true;
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

				String field = fieldSet.getFieldValue(group, events);

				// if the field returned null - see if its a real field....
				if (field == null) {
					// no value - no passing
					if (fieldSet.getField(group) == null) return false;
				}
				return execute(field);
			}
		}
	}

	final public boolean execute(final String val) {
		try {
			if (val == null) return false;
			Double value = Double.valueOf(val);
			return value < lessThanThis.doubleValue();
		}catch (NumberFormatException nfe) {
		}
		return false;
	}

	public String getTag() {
		return tag;
	}
	public String group() {
		return group;
	}
	@Override
	public Object value() {
		return this.lessThanThis;
	}

    @Override
    public String[] values() {
        return new String[0];
    }
}
