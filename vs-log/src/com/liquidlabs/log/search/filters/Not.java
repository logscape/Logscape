package com.liquidlabs.log.search.filters;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Splitter;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.functions.FunctionBase;


public class Not extends FunctionBase implements Filter {

	private static final long serialVersionUID = 1L;
	
	private String[] not;
	private String tag;
	public String group;

	public Not(){}
	public Not(String tag, String group, String not) {
        this(tag, group, Arrays.asList(not.split(",")));
        if (not.trim().length() == 0) this.not = new String[] { "" };
	}
	
	public Not(String tag, String group, List<String> not) {
		this(tag, not);
		if (not.size() == 0) this.not = new String[] { "" };
		this.group = group;
	}
	public Not(String tag, List<String> not) {
		this.tag = tag;
		int pos = 0;
		this.not = new String[not.size()];
		for (String nota : not) {
			this.not[pos++] = nota; 
		}
	}
	public boolean isAppledAtFinalAgg() {
		return false;
	}
	public boolean isNumeric() {
		return false;
	}
	
	public String toStringId() {
		return String.format("%s %s %s", getClass().getSimpleName(), tag, Arrays.toString(not));
	}

	transient Integer usingGroupId;
	final public boolean isPassed(FieldSet fieldSet, String[] events, String lineData, MatchResult matchResult, int lineNumber) {
		
		if (usingGroupId == null) {
			usingGroupId = super.groupIsInt(group) ? Integer.parseInt(group) : -1;
		}

		if (group.equals(STAR) || group.equals(ZERO)) {
			return execute(lineData);
		} else if (usingGroupId > -1) {
			String groupValue = matchResult.getGroup(usingGroupId);
			return execute(groupValue);
		} else {

			if (fieldSet.isMultiField(group)) {
				List<String> applyFields = fieldSet.getMultiFields(group);
				for (String applyField : applyFields) {
					final String value = fieldSet.getFieldValue(applyField, events);
					if (execute(value)) return false;
				}
                return true;

			} else {
				String field = fieldSet.getFieldValue(group, events);
				return execute(field);
			}
		}
	}


    transient Splitter splitComma = StringUtil.getCommaSplitter();
	final public boolean execute(String value) {
		if (value == null) return true;
        // allow exclusion of empty values!
        if (not.length == 1 && not[0].length() == 0 && value == null) return false;


        if (group.startsWith("_") && value.indexOf(',',0) != -1) {
            Iterable<String> values = splitComma.split(value);
            for (String aValue : values) {
                for (String aNot : not) {
                    if (aNot.contains(WCARD) || aNot.contains(STAR)) {
                        String exp = SimpleQueryConvertor.convertSimpleToRegExp(aNot);
                        if (aValue.matches(exp)) return false;
                    } else {
                        // need to check in case filtering against empty values - if we dont want empty values - and this is a value - return true to let it pass.
                        if (aNot.length() == 0 && aValue.length() != 0) return true;
                        if (StringUtil.containsIgnoreCase(aValue, aNot)) return false;
                    }
                }
            }

        } else {
            for (String aNot : not) {
                if (aNot.contains(WCARD) || aNot.contains(STAR)) {
                    String exp = SimpleQueryConvertor.convertSimpleToRegExp(aNot);
                    if (value.matches(exp)) return false;
                } else {
                    // need to check in case filtering against empty values - if we dont want empty values - and this is a value - return true to let it pass.
                    if (aNot.length() == 0 && value.length() != 0) return true;
                    if (StringUtil.containsIgnoreCase(value, aNot)) return false;
                }
            }
        }
		return true;
	}

	public String getTag() {
		return tag;
	}

	public String[] getNot() {
		return not;
	}
	
	public String group() {
		return group;
	}
	
	@Override
	public Object value() {
		return this.not;
	}
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[Not:");
		buffer.append(" { ");
		for (int i0 = 0; not != null && i0 < not.length; i0++) {
			buffer.append(" [" + i0 + "]:");
			buffer.append(not[i0]);
		}
		buffer.append(" } ");
		buffer.append(" tag:");
		buffer.append(tag);
		buffer.append(" group:");
		buffer.append(group);
		buffer.append("]");
		return buffer.toString();
	}
    public String[] values() {
        return this.not;
    }


}
