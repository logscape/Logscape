package com.liquidlabs.log.search.filters;

import java.util.Arrays;
import java.util.List;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.functions.FunctionBase;

public class Contains extends FunctionBase implements Filter {
	private static final long serialVersionUID = 1L;
	private String tag;
	private String[] values;
	public String group;

	public Contains() {
	}
	public Contains(String tag, String group, String singleFilter) {
		this(tag,group,Arrays.asList(singleFilter));
	}
	public Contains(String tag, String group, List<String> list) {
		this(tag,list);
		this.group = group;
	}

	
	public Contains(String tag, List<String> value) {
		this.tag = tag;
		values = new String[value.size()];
		int i = 0;
		for (String string : value) {
			values[i++] = string;
		}
	}
	public boolean isAppledAtFinalAgg() {
		return false;
	}
	public boolean isNumeric() {
		return false;
	}

	transient Integer usingGroupId;
	public boolean isPassed(FieldSet fieldSet, String[] events, String lineData, MatchResult matchResult, int lineNumber) {
		try {
            if (usingGroupId == null) {
				usingGroupId = super.groupIsInt(group) ? Integer.parseInt(group) : -1;
			}
			
			if (group.equals(STAR) || group.equals(ZERO)){
				return execute(lineData);
			} else if (usingGroupId > -1) {
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
					String value = fieldSet.getFieldValue(group, events);
					return execute(value);
				}
			}
		} catch (Throwable t) {
			return false;
		}
	}
	public boolean execute(String val) {
		if (val == null || val.length() == 0) return false;
        // nothing specified - let it through
        if (values.length == 0) return true;

		for (String value : values) {
			if (value.contains(WCARD) || value.contains("*")) {
				String exp = SimpleQueryConvertor.convertSimpleToRegExp(value);
				if (val.matches(exp)) return true;
			} else {
				if (StringUtil.containsIgnoreCase(val, value)) return true;
			}
		}
		return false;
	}

    public String[] values() {
        return this.values;
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

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[Contains:");
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
