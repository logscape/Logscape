package com.liquidlabs.log.search.filters;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Splitter;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;

public class Equals implements Filter {
	private static final long serialVersionUID = 1L;
	private String tag;
	private String[] values;
	public String group;

	public Equals() {
	}
	public Equals(String tag, String group, String singleFilter) {
		this(tag,group,Arrays.asList(singleFilter));
	}
	public Equals(String tag, String group, List<String> list) {
		this(tag,list);
		this.group = group;
	}
	
	public Equals(String tag, List<String> value) {
		this.tag = tag;
		values = new String[value.size()];
		int i = 0;
		for (String string : value) {
			values[i++] = string.trim();
		}
	}
	public boolean isAppledAtFinalAgg() {
		return false;
	}
	public boolean isNumeric() {
		return false;
	}

	public boolean isPassed(FieldSet fieldSet, String[] events, String lineData, MatchResult matchResult, int lineNumber) {
		try {
			if (group.equalsIgnoreCase(STAR)){
				return execute(lineData);
			} else { 
//		if (group > 0) {
				if (fieldSet.isMultiField(group)) {
					List<String> applyFields = fieldSet.getMultiFields(group);
					for (String applyField : applyFields) {
						final String value= fieldSet.getFieldValue(applyField, events);
						if (execute(value)) return true;
					}
					return false;

				} else {
					String value = fieldSet.getFieldValue(group, events);
					return execute(value);
				}
			}
		} catch (Throwable t) {
			return false;
		}
	}
    transient Splitter splitComma = StringUtil.getCommaSplitter();
	public boolean execute(String val) {


        // checking for a '' length field
		if (values.length == 1 && values[0].length() == 0 && val != null) {
			return val.length() == 0;
		}
		if (val == null) return false;

        if (val.indexOf(',', 0) != -1) {
            Iterable<String> splits = splitComma.split(val);
            for (String split : splits) {
                for (String value : values) {
                    if (split.length() > 0 && split.trim().equalsIgnoreCase(value)) return true;
                }
            }
        } else {
            for (String value : values) {
                if (val.equalsIgnoreCase(value)) return true;
            }
        }
		return false;
	}

	public String getTag() {
		return tag;
	}
	public String group() {
		return group;
	}
	public String toStringId() {
		return String.format("%s %s %s", getClass().getSimpleName(), tag, Arrays.toString(values));
	}
	
	public Object value() {
		return this.values;
	}
    public String[] values() {
        return this.values;
    }


    public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[Equals:");
		buffer.append(" tag:");
		buffer.append(tag);
		buffer.append(" g:").append(group);
		buffer.append(" { ");
		for (int i0 = 0; values != null && i0 < values.length; i0++) {
			buffer.append(" v[" + i0 + "]:");
			buffer.append(values[i0]);
		}
		buffer.append(" } ");
		buffer.append("]");
		return buffer.toString();
	}


}
