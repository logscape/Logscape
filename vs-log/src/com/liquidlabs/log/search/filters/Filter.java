package com.liquidlabs.log.search.filters;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;

import java.io.Serializable;

public interface Filter extends Serializable {
	public static final String ZERO = "0";
	public static final String STAR = "*";
	public static final String WCARD = ".*";

    /**
     * Post Agg filters filter function output - rather that souce analytic values i.e. CPU.avg(,AVG) AVG.gt(10)
     */
    public class FilterUtil {
        public static boolean isPostAggregate(String fieldName, String rawLine) {
            return rawLine.contains( "," + fieldName + ")");
        }
    }

	/**
	 *
     *
     *
     * @param fieldSet TODO
     * @param events
     * @param lineData TODO
     * @param matchResult TODO
     * @param lineNumber
     * @return true when the line 'passes' the filter
	 */
	boolean isPassed(FieldSet fieldSet, String[] events, String lineData, MatchResult matchResult, int lineNumber);
	/**
	 * 
	 * @param val - return TRUE when it passes the filter
	 * @return
	 */
	boolean execute(String val);
	
	String getTag();

	String toStringId();
	
	String group();
	
	Object value();


    String[] values();
	
	
	/**
	 * True =- when can be applied at the final aggregation stage.
	 * For example 
	 *  o contains() is only applied on the source
	 *  o gt() - can be applied at both stages - and when used with count() - will only work at the final stage
	 * @return
	 */
	boolean isAppledAtFinalAgg();
	boolean isNumeric();

}
