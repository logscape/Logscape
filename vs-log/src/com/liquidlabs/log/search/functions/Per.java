package com.liquidlabs.log.search.functions;

import com.google.common.base.Splitter;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.space.LogRequest;
import javolution.util.FastMap;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * 
 * 3 use cases,
 * 1. count(tag, 0) = counts all lines with a match
 * 2. count(tag, 2) = counts all group(2) items
 * 3. count(tag, 1,2) = counts using a groupBy() - i.e. user/action
 * 
 * 1. Count 
 * a b
 * a c
 * 2. count(1,2) into 
 * a_b=1
 * a_c=2
 * count(1) into
 * a=2
 * 
 * @author Neil
 *
 */
public class Per extends FunctionBase implements Function, FunctionFactory {

    private String function;
    private String groupByGroup;
    private String applyToGroup;
    public String tag = "";

		private final static Logger LOGGER = Logger.getLogger(Per.class);
        private final static String SIMPLENAME = Per.class.getSimpleName();

		Map<String, IntValue> groups;

		public Per(){
		}
		public Per(String function, String groupByGroup, String applyToGroup) {
            super(SIMPLENAME);
            this.function = function;
            this.groupByGroup = groupByGroup;
            this.applyToGroup = applyToGroup;
        }

		public String group() {
			return "";
		}
		public String groupByGroup() {
			return "";
		}
		public String getTag() {
			return tag;
		}

		public boolean execute(final FieldSet fieldSet, final String[] fields, final String pattern, final long time, final MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {

            return true;

		}
        public void execute(String apply, String group) {
        }
		
    public Function create() {
			return new Per(tag, groupByGroup, applyToGroup);
		}
		
		/**
		 * Aggregate other results into this map
		 */
		public void updateResult(String groupName, Map<String, Object> otherCountGroupsMap) {
		}
		
		final public Map<String, IntValue> getGroups() {
			if (groups == null) {
				groups = new FastMap<String, IntValue>();
				((FastMap) groups).shared();
				}
			return groups;
		}
		public void updateResult(String groupBy, Number value) {
			getGroups().put(groupBy, new IntValue(value.intValue()));
		}
		
		public String getApplyToField() {
			return applyToGroup;
		}
		public void reset() {
			this.groups = null;
			getGroups();
		}
}
