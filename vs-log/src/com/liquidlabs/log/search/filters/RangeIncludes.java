package com.liquidlabs.log.search.filters;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;

import java.util.List;


public class RangeIncludes implements Filter {

	private static final long serialVersionUID = 1L;
	
	private String tag;
	private String group;
	protected Number lowerRange;
	protected Number upperRange;
	
	public RangeIncludes() {}
	
	public RangeIncludes(String tag, String group, Number lowerRange, Number upperRange) {
		this.tag = tag;
		this.group = group;
		this.lowerRange = lowerRange;
		this.upperRange = upperRange;
	}
	public String toStringId() {
		return String.format("%s %s %s %s %s", getClass().getSimpleName(), tag, group, lowerRange, upperRange);
	}
	
	public boolean isAppledAtFinalAgg() {
		return true;
	}
	public boolean isNumeric() {
		return true;
	}

	public boolean isPassed(FieldSet fieldSet, String[] events, String lineData, MatchResult matchResult, int lineNumber) {
		if (fieldSet.isMultiField(group)) {
			List<String> applyFields = fieldSet.getMultiFields(group);
			for (String applyField : applyFields) {
				final String value = fieldSet.getFieldValue(applyField, events);
				if (execute(value)) return true;
			}
			return false;

		} else {
			String field = fieldSet.getFieldValue(group, events);
			if (field == null) return false;
			return execute(field);
		}
	}

	public boolean execute(String val) {
		try {
			if (val == null) return false;
			Double value = Double.valueOf(val);
			return value >= lowerRange.doubleValue() && value <= upperRange.doubleValue();
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
	public Object value() {
		return this.lowerRange;
	}
    public String[] values() {
        return new String[0];
    }

}
