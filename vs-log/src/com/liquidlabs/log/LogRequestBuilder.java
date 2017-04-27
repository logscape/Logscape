package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.filters.*;
import com.liquidlabs.log.search.functions.*;
import com.liquidlabs.log.search.functions.txn.SyntheticTransAccumulate;
import com.liquidlabs.log.search.functions.txn.SyntheticTransTrace;
import com.liquidlabs.log.search.summaryindex.PersistingSummaryIndex;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.Replay;
import com.liquidlabs.log.space.ReplayType;
import com.liquidlabs.log.space.Search;
import com.liquidlabs.log.space.agg.HistoManager;
import org.apache.log4j.Logger;
import org.joda.time.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class LogRequestBuilder {
	private static final String OLD_SCHOOL = "old_school";

	private final static Logger LOGGER = Logger.getLogger(LogRequestBuilder.class);

	private Map<String, Class<? extends Function>> functions = new HashMap<String, Class<? extends Function>>();
	private Map<String, Class<? extends Filter>> filters = new HashMap<String, Class<? extends Filter>>();
    private final ArrayList<RequestParamAppliers.RequestParamApplier> requestParamAppliers;
    private final Set<String> ignoreSet = new HashSet<String>(Arrays.asList("sort"));




	public LogRequestBuilder() {
		functions.put("count", Count.class);
        functions.put("values", Values.class);

		functions.put("countSingle", CountSingle.class);

		functions.put("countMembers", CountSingle.class);

		functions.put("countDistinct", CountSingle.class);

		functions.put("countUnique", CountUnique.class);
		functions.put("countDelta", CountDelta.class);

		functions.put("countSingleDelta", CountSingleDelta.class);
		functions.put("countMembersDelta", CountSingleDelta.class);

		functions.put("average", Average.class);
		functions.put("avg", Average.class);
        functions.put("trend", AverageMoving.class);
		functions.put("avgDelta", AverageDelta.class);
        functions.put("avgDeltaPc", AverageDeltaPercent.class);
		functions.put("max", Max.class);
		functions.put("min", Min.class);
		functions.put("sum", Sum.class);
		functions.put("eval", EvaluateExpr.class);
		functions.put("percentile", Percentile.class);

		filters.put("lessThan", LessThan.class);
		filters.put("lt", LessThan.class);
		filters.put("greaterThan", GreaterThan.class);
		filters.put("gt", GreaterThan.class);
        requestParamAppliers = new ArrayList<RequestParamAppliers.RequestParamApplier>();
        requestParamAppliers.add(new FunctionApply());
        requestParamAppliers.add(new FilterApply());
        requestParamAppliers.add(new RequestParamAppliers.ApplyVerbose());
        requestParamAppliers.add(new RequestParamAppliers.ApplyAutoCancel());
        requestParamAppliers.add(new RequestParamAppliers.ApplyHitLimit());
        requestParamAppliers.add(new RequestParamAppliers.ApplySynthTrans());
        requestParamAppliers.add(new RequestParamAppliers.ApplySynthTransTrace());
        requestParamAppliers.add(new RequestParamAppliers.ApplyElapsed());
        requestParamAppliers.add(new RequestParamAppliers.ApplyByFirstOrLast());
        requestParamAppliers.add(new RequestParamAppliers.ThingsToIgnore());

    }

	class FunctionApply implements RequestParamAppliers.RequestParamApplier {
		public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
			return applyFunction(query, paramParts, pos);
		}
	}

	class FilterApply implements RequestParamAppliers.RequestParamApplier {

		public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
			final Filter filter = applyFilterFunction(null, paramParts, pos);
			if(filter == null) {
				return false;
			}
			query.addFilter(filter);
			return true;
		}
	}
	public LogRequest getLogRequest(String subscriberName, Search report, String variables) {
		Integer replayPeriod = report.replayPeriod;
		long now = System.currentTimeMillis();
		long fromTime = now - (replayPeriod * 60 * 1000);
		return getLogRequest(subscriberName, report.patternFilter, variables, fromTime, now);

	}

	public LogRequest getLogRequest(String subscriberName, List<String> logFilters, String variables, long fromTime, long toTime) {
		return getLogRequestRegExp(subscriberName, convertFromSimple(logFilters, variables), fromTime, toTime, false);
	}

	private List<Query> convertFromSimple(List<String> logFilters, String variables) {
		String groups = "[^(]*\\([^)]+\\).*";
        List<Query> result = new ArrayList<Query>();
		int sourcePos = 0;
		int pos = 0;
		if (logFilters.size() == 1 && logFilters.get(0).length() == 0) logFilters.set(0, "*");
		for (String logFilter : logFilters) {
            if (logFilter.length() == 0) logFilter = "*";
			if (logFilter.trim().length() == 0) continue;

			String fieldSetId = getFieldSetId(logFilter);

			if (!SimpleQueryConvertor.isSimpleLogFiler(logFilter)) {
				String[] searchExpression = splitLast(logFilter,"|");
				String functions = "";
				boolean isReplay = false;
				if (searchExpression.length == 2) {
					isReplay = !searchExpression[1].contains("replay(false)");
					functions = searchExpression[1];
				}
				String queryPart = searchExpression[0];
				if (queryPart.startsWith("type=")) queryPart = queryPart.substring(queryPart.indexOf("'", queryPart.indexOf("'")+1)+1).trim();
				Query query = getQueryFromString(variables, fieldSetId, pos++, sourcePos, queryPart, functions, isReplay);
				result.add(query);
				sourcePos++;
				continue;
			}

			String[] bits = splitLast(logFilter, "|");
			String queryPart = bits[0].trim();

			// remove any type stuff from the expression i.e. type='log4j' CPU -> CPU
			if (queryPart.startsWith("type=")) queryPart = queryPart.substring(queryPart.indexOf("'", queryPart.indexOf("'")+1)+1).trim();
			if (queryPart.length() == 0) queryPart = "*";
			String functionPart =  bits.length == 2 ?  bits[1].trim() : "";

            if(queryPart.matches(groups)) {
                boolean isReplay = ! functionPart.contains("replay(false)");
                Query query = getQueryFromString(variables, fieldSetId, pos++, sourcePos, queryPart,  functionPart, isReplay);
                result.add(query);
            } else {
                queryPart = queryPart.replaceAll("\\|", " OR ");
                if(!queryPart.contains(" OR ")) {

// DAMIANS Stuff to make it work with ORRRR - but breaks * | _filename.count()
//                final String functions = replaceZeroArgCountFunctionWithGroupNumber(functionPart);
//                if(!functions.equals(functionPart)) {
//                    queryPart = groupItUp(queryPart);
//                }
//                result.add(getQueryFromString(variables, fieldSetId, pos++, sourcePos, queryPart, functions, ! functionPart.contains("replay(false)")));


                    // NEIL - Reverted to make it work with basic stuff
                    boolean isReplay = ! functionPart.contains("replay(false)");
                    Query query = getQueryFromString(variables, fieldSetId, pos++, sourcePos, queryPart,  functionPart, isReplay);
                    result.add(query);
                } else {
                    String[] queries = queryPart.split(" OR ");
                    for(String thisQueryPart : queries) {
                        boolean isReplay = ! functionPart.contains("replay(false)");
                        if (thisQueryPart.contains("(")) {
                            String functions = replaceZeroArgCountFunctionWithValue(functionPart, "1", "");
                            Query query = getQueryFromString(variables, fieldSetId, pos++, sourcePos, groupItUp(thisQueryPart),  functions, isReplay);
                            result.add(query);
                        } else {
                            String functions = replaceZeroArgCountFunctionWithValue(functionPart,"", thisQueryPart);
                            Query query = getQueryFromString(variables, fieldSetId, pos++, sourcePos, thisQueryPart, functions, isReplay);
                            result.add(query);


                        }
                    }
                }
            }

			sourcePos++;
		}
		for (Query query : result) {
			query.setGroupId(getGroupIndex(query.getSourcePos(), query.position(), result));
		}
		return result;
	}

    private String replaceZeroArgCountFunctionWithValue(String functionPart, String value, String tag) {
        value = value.replace(" ", "_");
        int index = -1;
        int currentPos = 0;
        StringBuilder builder = new StringBuilder();
        while((index = functionPart.indexOf("count()",currentPos)) != -1) {
            builder.append(functionPart.substring(currentPos, index));
            currentPos = index + "count()".length();
            builder.append("count("+value + "," + tag + ")");
        }
        builder.append(functionPart.substring(currentPos));
        return builder.toString();
    }

    private String groupItUp(String thisQueryPart) {
        if(thisQueryPart.startsWith("(") && thisQueryPart.endsWith(")")) return thisQueryPart;
        if(thisQueryPart.startsWith("(")) return thisQueryPart + ")";
        if(thisQueryPart.endsWith(")")) return "(" + thisQueryPart;
        return "(" + thisQueryPart + ")";
    }

    private String getFieldSetId(String logFilter) {
		try {
			if (!logFilter.contains("type=")) return OLD_SCHOOL;
			String[] split = logFilter.split("type=");
			String[] fieldSetName = split[1].split(" ");
			return fieldSetName[0].replaceAll("'", "");
		} catch (Throwable t) {
			return "*";
		}
	}
	private String[] splitLast(String source, String split) {
		if (!source.contains(split)) return new String[] {source };
		int sourceSplit = source.lastIndexOf(split);
		return new String[] {
				source.substring(0, sourceSplit),
			source.substring(sourceSplit +1, source.length())
		};
	}
	public int getGroupIndex(int querySource, int queryNumber, List<Query> queries) {
		if (querySource == 0) return queryNumber;
		int groupId = 0;
		for (Query query : queries) {
			if (query.getSourcePos() < querySource) {
				groupId = 0;
				continue;
			}
			if (query.position() == queryNumber) return groupId;
			groupId++;
		}
		return 0;
	}


	public LogRequest getLogRequestRegExp(String subscriberName, List<Query> queries, long fromTime, long toTime, boolean simple) {

		LogRequest request = new LogRequest(subscriberName, fromTime, toTime);
		int pos = 0;
		for (Query query : queries) {
			request.addQuery(query);
			extractAndApplyRequestParams(query.sourceQuery(), query, request);
			pos++;
		}


		request.setStreaming(getStreamingFlag(queries));
		request.applyOffset(getOffsetValue(queries));

		request.setIgnoreCase(getIgnoreCase(queries));
        int buckets = getBuckets(queries, request.getStartTimeMs(), request.getEndTimeMs(), getBucketMultiplier(queries));
        request.setBucketCount(buckets);

        List<Long> times = new HistoManager().getBucketTimes(new DateTime(request.getStartTimeMs()), new DateTime(request.getEndTimeMs()), buckets, false);
        long width = (request.getEndTimeMs() - request.getStartTimeMs());
        if (times.size() > 1) width = times.get(1) - times.get(0);


		request.setBucketWidth((int) (width / DateUtil.SECOND));
        request.setBucketCount(times.size());

        // System.err.println("SUBMIT FROM:" + new DateTime(request.getStartTimeMs()) + " BUCKETS:" + request.getBucketCount() + "  INTERVAL:" +  request.getBucketSizeMs());


        request.setStartTimeMs(times.get(0));
        long bucketWidth = request.getEndTimeMs() - request.getStartTimeMs();//
        if (times.size() > 1) bucketWidth = times.get(1) - times.get(0);
        request.setEndTimeMs(times.get(0)  + (width * times.size()));

		request.setReplay(new Replay(ReplayType.START, 100));

		if (PersistingSummaryIndex.isWrite(queries.get(0).sourceQuery())) {
			request.makeSummaryIndex();
		}

		request.cacheKey();

        //System.err.println("SSSS STR TIME WAS:" + new DateTime(request.getStartTimeMs()) );
        //System.err.println("EEEE END TIME WAS:" + new DateTime(request.getEndTimeMs()));

        return request;
	}
	private int getBucketWidth(List<Query> logFilters) {
		try {
			for (Query query : logFilters) {
				String pattern = query.sourceQuery();
				String string = "bucketWidth(";
				if (pattern.contains(string)) {
					int index1 = pattern.indexOf(string) + string.length();
					if (pattern.contains("m)"))
						return Integer.parseInt(pattern.substring(index1, pattern.indexOf("m)", index1)));
					else if (pattern.contains("h)"))
						return Integer.parseInt(pattern.substring(index1, pattern.indexOf("h)", index1))) * 60;
				}
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return -1;
	}
	private String getOffsetValue(List<Query> logFilters) {
		for (Query query : logFilters) {
			try {
				String offset = "offset(";
				if (query.sourceQuery().contains(offset)) {
					String fullPattern = query.sourceQuery();
					int from = fullPattern.indexOf(offset) + offset.length();
					int to = fullPattern.indexOf(")", from);
					return fullPattern.substring(from, to);
				}
			} catch (Throwable t) {
				LOGGER.warn(query.toString(), t);
			}
		}
		return "";
	}
	/**
	 * Return 2 when a line chart is being used - otherwise return 1
	 * @param logFilters
	 * @return
	 */
	public int getBucketMultiplier(List<Query> logFilters) {
		boolean wasChartSpecified = false;
		for (Query query : logFilters) {
			String userExpression = query.sourceQuery();
			if (!userExpression.contains("bucketWidth") &&	 (userExpression.contains("chart(line") ||
																userExpression.contains("chart(area)") ||
																userExpression.contains("chart(stacked)"

			))) return LogProperties.getDefaultBucketMultiplier();
			if (!wasChartSpecified) wasChartSpecified = userExpression.contains("chart(");
		}
		if (!wasChartSpecified) return LogProperties.getDefaultBucketMultiplier();
		return 1;
	}
	private boolean getIgnoreCase(List<Query> logFilters) {
		for (Query query : logFilters) {
			if (query.sourceQuery().contains("ignoreCase(true)")) return true;
		}
		return false;
	}

	private boolean getStreamingFlag(List<Query> logFilters) {
		for (Query query : logFilters) {
			if (query.sourceQuery().contains("live(true)")) return true;
		}
		return false;
	}
	private Query getQueryFromString(String variables, String fieldSetId, int pos, int sourcePos, String pattern, String functions, boolean isReplaying) {
		String logFilterWVars = replaceVariables(pattern, variables);
		String functionsWVars = replaceVariables(functions, variables);

		Query query = new Query(pos, sourcePos, logFilterWVars, logFilterWVars.trim() + " | " + functionsWVars.trim(), isReplaying);
		if (functions.contains("top(")) {
			try {
				int topIndex = functions.indexOf("top(") + "top(".length();
				String topValue = functions.substring(topIndex, functions.indexOf(")", topIndex));
				// allow top to contains a fieldName
				if (topValue.contains(",")) {
					topValue=topValue.split(",")[0];
				}
				if (topValue.endsWith("+")) {
					topValue = topValue.replace("+", "");
					query.setTopOther(true);
				}
				int topLimit = Integer.parseInt(topValue);
				query.setTopLimit((short) topLimit);
			} catch (Throwable t) {
				LOGGER.warn("top", t);
			}

		} else if (functions.contains("bottom(")) {
			try {
				int topIndex = functions.indexOf("bottom(") + "bottom(".length();
				String topValue = functions.substring(topIndex, functions.indexOf(")", topIndex));
				// allow top to contains a fieldName
				if (topValue.contains(",")) topValue=topValue.split(",")[0];
				int topLimit = Integer.parseInt(topValue);
				query.setTopLimit((short) (topLimit * -1));
			} catch (Throwable t) {
				LOGGER.warn("bottom", t);
			}

		}
		return query;
	}

    private boolean tryApplyStuff(String queryString, Query query, List<String> paramParts, LogRequest request, int pos) {
        int i = 0;
        boolean applied = false;
        while(i < requestParamAppliers.size() && !applied) {
            applied = requestParamAppliers.get(i).apply(request, query, paramParts, pos, queryString);
            i++;
        }
        return applied;
    }

	private void extractAndApplyRequestParams(String queryString, Query query, LogRequest request) {
		int splitter = queryString.lastIndexOf("|");
		if (splitter == -1) return;
		queryString = queryString.substring(splitter+1);
		String[] splitArgs = parseArgs(queryString);
		int pos = 0;
		for (String possibleFunction : splitArgs) {

			List<String> paramParts = getParamParts(possibleFunction);
			if (paramParts.size() == 0) continue;

            if (paramParts.size() > 1 && ignoreSet.contains(paramParts.get(1))) continue;

            if(!tryApplyStuff(queryString, query, paramParts, request, pos)) {
                request.addError("Unknown function application: " + possibleFunction);
            }

            pos++;
		}

	}

	/**
	 * Support var args - i.e. count("myCount", 1, 2) OR count(1, 2) OR count("stuff", 2) OR count(1)
	 */
	private boolean applyFunction(Query query, List<String> paramParts, int pos) {
		if (paramParts == null || paramParts.size() == 0) return false;

		String fieldName = paramParts.get(0);
		String function = paramParts.get(1);
		if (!this.functions.containsKey(function)) {
			return false;
		}


		Class <? extends FunctionFactory> aggregateFunction = this.functions.get(function);

		if (aggregateFunction == null){
			return false;
		}

		RequestParamAppliers.ParamSet params = null;
		if (paramParts.size() == 2) {
			params = new RequestParamAppliers.ParamSet(function, fieldName, "", fieldName);
		}
		// count(tag,0)
		if (paramParts.size() == 3) {
			if (paramParts.get(2).contains("(")) {
				List<String> filterParamParts = getParamParts(paramParts.get(2));
				Filter filter = applyFilterFunction(fieldName, filterParamParts,pos);

				params = new RequestParamAppliers.ParamSet(function, fieldName, fieldName, fieldName, filter);
			} else {
				params = new RequestParamAppliers.ParamSet(function, fieldName, paramParts.get(2), fieldName);
			}
		}
		if (paramParts.size() == 4) {
			// data.countWFilter(Pages, contains(html),not(stuff))";
			if (paramParts.get(3).contains("(")) {
				List<String> filterParamParts = getParamParts(paramParts.get(3));
				Filter filter = applyFilterFunction(filterParamParts.get(0), filterParamParts,pos);
				params = new RequestParamAppliers.ParamSet(function, fieldName, paramParts.get(2), fieldName, filter);

			} else {
				if (paramParts.get(2).contains("(")) {
					List<String> filterParamParts = getParamParts(paramParts.get(2));
					Filter filter = applyFilterFunction(filterParamParts.get(0), filterParamParts,pos);
					params = new RequestParamAppliers.ParamSet(function, paramParts.get(3), fieldName, fieldName, filter);

				} else {
					params = new RequestParamAppliers.ParamSet(function, paramParts.get(3), paramParts.get(2), fieldName);
				}
			}
		}
		if (paramParts.size() == 5) {

			// data.countWFilter(Pages, contains(html))";
			// data.countWFilter(Pages, contains(html) ,not(stuff))";
			boolean param3isFilter = paramParts.get(3).contains("(") && paramParts.get(3).contains(")");
			boolean param4isFilter = paramParts.get(4).contains("(") && paramParts.get(4).contains(")");
			if (!param3isFilter && param4isFilter) {
				List<String> filterParamParts1 = getParamParts(paramParts.get(4));
				Filter filter1 = applyFilterFunction(filterParamParts1.get(0), filterParamParts1,pos);
				params = new RequestParamAppliers.ParamSet(function, paramParts.get(3), paramParts.get(2),  fieldName,filter1);

			} else if (param3isFilter && param4isFilter) {
				List<String> filterParamParts1 = getParamParts(paramParts.get(3));
				Filter filter1 = applyFilterFunction(filterParamParts1.get(0), filterParamParts1,pos);
				List<String> filterParamParts2 = getParamParts(paramParts.get(4));
				Filter filter2 = applyFilterFunction(filterParamParts2.get(0), filterParamParts2,pos);
				params = new RequestParamAppliers.ParamSet(function, fieldName, paramParts.get(2),  fieldName, filter1, filter2);
			} else {
				params = new RequestParamAppliers.ParamSet(function, paramParts.get(4), paramParts.get(3), fieldName);
			}
		}

		try {
			Function aggFunction = (Function) getFunctionInstance(aggregateFunction, params.tag, params.groupBy, params.applyTo, params.filter1, params.filter2, null, params.params);
			query.addFunction(aggFunction);
		} catch (Throwable e) {
			if (params != null) LOGGER.error("Failed to add function:" + params.tag);
			LOGGER.warn(e);
			return false;
		}
		return true;
	}
	Filter applyFilterFunction(String groupOverride, List<String> paramParts, int pos) {
		if (paramParts == null || paramParts.size() < 1) return null;
//		String[] fieldNameAndFilterName = paramParts.get(0).split("\\.");

		// OLD SCHOOL old_school
//		if (fieldNameAndFilterName.length != 2) {
//			else fieldNameAndFilterName = new String[] {  groupOverride , paramParts.get(0) };
//		}
		String fieldName = paramParts.get(0);
		String filterName = paramParts.get(1);

		if (groupOverride != null) {
			fieldName = groupOverride;// = new String[] {  "*" , paramParts.get(0) };
		}


		Class <? extends Filter> filterClass = this.filters.get(filterName);

		if (filterClass == null){
			Filter result = null;
			result = RequestParamAppliers.applyNotFunction(fieldName, paramParts, pos);
			if (result != null) return result;
			result = RequestParamAppliers.applyGroupNotFunction(fieldName, paramParts, pos);
			if (result != null) return result;
			result = RequestParamAppliers.applyContainsFunction(fieldName, paramParts, pos);
			if (result != null) return result;
			result = RequestParamAppliers.applyEqualsFunction(fieldName, paramParts, pos);
			if (result != null) return result;
			result = RequestParamAppliers.applyGroupContainsFunction(fieldName, paramParts, pos);
			if (result != null) return result;
			result = RequestParamAppliers.applyRangeIncludes(fieldName, paramParts, pos);
			if (result != null) return result;

			result = RequestParamAppliers.applyRangeExcludes(fieldName, paramParts, pos);
			if (result != null) return result;
		}
		if (filterClass == null) return null;

		RequestParamAppliers.ParamSet params = null;

		try {

			if (paramParts.size() == 4) {
				params = new RequestParamAppliers.ParamSet(filterName, paramParts.get(1), fieldName, paramParts.get(2));
			}
			if (paramParts.size() == 3) {
				params = new RequestParamAppliers.ParamSet(filterName, fieldName, Double.parseDouble(paramParts.get(2)));
			}
			if (paramParts.size() == 2) {
				params = new RequestParamAppliers.ParamSet(filterName, fieldName, Integer.parseInt(paramParts.get(1)));
			}

			return  (Filter) getFunctionInstance(filterClass, "", fieldName, fieldName, null, null, params.number, params.params);
		} catch (Throwable e) {
			LOGGER.warn("Field:" + fieldName, e);
		}
		return null;
	}

	List<String> getParamParts(String possibleParam) {
		ArrayList<String> result = new ArrayList<String>();
		if (possibleParam.contains("(")) {
			int indexOf = possibleParam.indexOf("(");
			if (indexOf == -1) return result;
			String trim = possibleParam.substring(0, indexOf).trim();
			if (trim.contains(".")) {
				int offSet = trim.lastIndexOf(".");
				result.add(trim.substring(0, offSet));
				result.add(trim.substring(offSet+1, trim.length()));
			} else {
				result.add("0");
				result.add(trim);
			}

			String value = possibleParam.substring(indexOf+1, possibleParam.length());
			List<String> parts = getParts(value, true);
			if (parts != null) {
				result.addAll(parts);
				return result;
			}

		}
		return new ArrayList<String>();
	}


	List<String> getParts(String stringToParse, boolean allResults) {
		List<String> results = new ArrayList<String>();

		if (stringToParse.equals(")")) return results;

		int openBrackets = 0;
		int closedBrackets = 0;
		String currentWord = "";
		for (int i = 0; i < stringToParse.length(); i++) {
			char charAt = stringToParse.charAt(i);
//			String sChar = ""+charAt;


			if (charAt == '(') {
				//if (i > 0)
				currentWord += charAt;
				openBrackets++;
			} else if (charAt == ')') {
				closedBrackets++;
				if (closedBrackets > openBrackets) {
					results.add(currentWord.trim());
					return results;
				}
				currentWord += charAt;
			}
			else if (charAt == ',') {
				if (openBrackets > 0 && openBrackets != closedBrackets) {
					currentWord += charAt;
				} else {
					results.add(currentWord.trim());
					currentWord = "";
				}
			}
			else {
				currentWord += charAt;
			}
		}
		if (openBrackets == closedBrackets) {
			results.add(currentWord.trim());
		} else if (allResults) {
			results.add(currentWord);
		}
		if (results.size() > 0 || allResults) return results;
		return  null;
	}

	@SuppressWarnings("unchecked")
	Object getFunctionInstance(Class aggregateFunction, String tag, String groupBy, String applyTo, Filter filter1, Filter filter2, Number filterValue, String[] args)
			throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {


		Constructor[] constructors = aggregateFunction.getConstructors();
		for (Constructor constructor : constructors) {
			Class[] params = constructor.getParameterTypes();
			if (params.length == 5 && filter1 != null && filter2 != null) {
				return constructor.newInstance(tag, groupBy, applyTo, filter1, filter2);
			}
			if (params.length == 4 && filter1 != null) {
				return constructor.newInstance(tag, groupBy, applyTo, filter1);
			}
			if (params.length == 3) {
				if (params[0].equals(String.class) && params[1].equals(String.class) && params[2].equals(String.class)) {
					return constructor.newInstance(tag, groupBy, applyTo);
				}
				if (params[0].equals(String.class) && params[1].equals(String.class) && params[2].equals(Number.class)) {
					return constructor.newInstance(tag, groupBy, filterValue);
				}
			}
			if (params.length == 2) {
//					if (args[0].equals(String.class) && args[1].equals(String.class)) {
//						return constructor.newInstance(tag, group2.trim());
//					}
				if (params[0].equals(String.class) && params[1].equals(int.class)) {
					return constructor.newInstance(tag, Integer.parseInt(applyTo));
				}
			}
		}

		throw new RuntimeException("Failed to find constructor for Function:" + aggregateFunction);
	}



    String[] parseArgs(String query) {
		List<String> result = new ArrayList<String>();

		String[] split = query.split(" ");
		String processingSection = "";
		for (String string : split) {
			processingSection += " " + string;
			processingSection = processingSection.trim();
			List<String> parsed = getParts(processingSection, false);
			if (parsed == null) continue;

			processingSection = "";

			String got = "";
			for (int i = 0; i < parsed.size(); i++) {
				got += parsed.get(i);
				if (i < parsed.size()-1) got += ",";

			}

			if (got.trim().length() > 0) result.add(got.trim());
		}

		return com.liquidlabs.common.collection.Arrays.toStringArray(result);
	}

	String getNextSegment(int pos, String query) {
		int nextSpace = query.indexOf(" ", pos);
		if (nextSpace == -1) {
			return query.substring(pos, query.length());
		}
		int nextBrace = query.indexOf("(", pos);
		if (nextBrace > nextSpace) {
			return query.substring(pos, nextSpace);
		}
		else {
			int nextClose = query.indexOf(")", nextBrace);
			int nextNextSpace = query.indexOf(" ", nextClose);
			if (nextNextSpace == -1) {
//				return query.substring(pos, nextClose+1);
				return query.substring(pos, query.length());
			}
			return query.substring(pos, query.indexOf(" ", nextClose));
		}
	}







	String removeTailEndFromPipe(String query) {
		if (query.indexOf("|") > -1) return query.substring(0, getPipeIndex(query)).trim();
		return query;
	}
	int getPipeIndex(String query) {
		byte match = "|".getBytes()[0];
		byte escapse = "\\".getBytes()[0];
		byte prev = "x".getBytes()[0];
		int pos = 0;
		for (byte b : query.getBytes()) {
			if (prev != escapse && b == match) {
				return pos;
			}
			prev = b;

			pos++;
		}
		return query.length();
	}

	int getQueryPart(String key, String logFilter) {
		int indexOf = logFilter.indexOf(key);
		if (indexOf != -1) {
			return Integer.parseInt(logFilter.substring(indexOf+1 + key.length(), logFilter.indexOf(")", indexOf)));
		}
		return -1;
	}


	String replaceVariables(String stringToReplace, String variables) {
		if (variables == null || stringToReplace.length() == 0) return stringToReplace;
		String[] vars = variables.split(",");
		for (int i = 0; i < vars.length; i++) {
			String variable = vars[i].trim();
			while (stringToReplace.contains("{" + i + "}")) {
				stringToReplace = stringToReplace.replace("{" + i + "}", variable);
			}
		}
		return stringToReplace;
	}
    public int getBuckets(List<Query> querys, long fromTimeMs, long toTimeMs, int multiplier) {
        int bucketsForWidth = getBucketsP(querys, fromTimeMs, toTimeMs, multiplier);
        while (bucketsForWidth > LogProperties.getMaxBucketThreshold()) {
            bucketsForWidth /= 2;
        }
        return bucketsForWidth;
    }
	private int getBucketsP(List<Query> querys, long fromTimeMs, long toTimeMs, int multiplier) {
		if (querys == null) return 60;

		DateTime fromTime = new DateTime(fromTimeMs);
		DateTime toTime = new DateTime(toTimeMs);
		Minutes m = Minutes.minutesBetween(fromTime, toTime);


		for (Query query : querys) {
			int queryPart = getQueryPart("buckets(", query.sourceQuery(),")");
			if (queryPart == 0) queryPart = getQueryPart("buckets('", query.sourceQuery(),"')");
			if (queryPart == 0) queryPart = getQueryPart("buckets(\"", query.sourceQuery(),"\")");
			if (queryPart > 0) return queryPart;
			int bucketWidth = getQueryPart("bucketWidth(", query.sourceQuery(),")");
			if (bucketWidth == 0) bucketWidth = getQueryPart("bucketWidth('", query.sourceQuery(),"')");
			if (bucketWidth == 0) bucketWidth = getQueryPart("bucketWidth(\"", query.sourceQuery(),"\")");
			if (bucketWidth > 0 && m.getMinutes()/bucketWidth < 60 && m.getMinutes() > bucketWidth) {
				int bucketsForWidth = m.getMinutes()/bucketWidth;
				return bucketsForWidth;
			}
			String bucketWidthString = getQueryPartAsString("bucketWidth(", query.sourceQuery(),")");
			if (bucketWidthString.endsWith("s")) {
				bucketWidth = Integer.parseInt(bucketWidthString.substring(0, bucketWidthString.length()-1));
				Seconds s = Seconds.secondsBetween(fromTime, toTime);
				int bucketsForWidth = s.getSeconds()/bucketWidth;
				return bucketsForWidth;
			}
			if (bucketWidthString.endsWith("ms")) {
				bucketWidth = Integer.parseInt(bucketWidthString.substring(0, bucketWidthString.length()-2));
				long deltaMs = toTimeMs - fromTimeMs;
				int bucketsForWidth = (int) (deltaMs/bucketWidth);
				return bucketsForWidth;
			}


			if (bucketWidthString.endsWith("m")) {
				bucketWidth = Integer.parseInt(bucketWidthString.substring(0, bucketWidthString.length()-1));
				int bucketsForWidth = m.getMinutes()/bucketWidth;
				return bucketsForWidth;
			}
			if (bucketWidthString.endsWith("h")) {
				 Hours hours = Hours.hoursBetween(fromTime, toTime);
				bucketWidth = Integer.parseInt(bucketWidthString.substring(0, bucketWidthString.length()-1));
				int bucketsForWidth = hours.getHours()/bucketWidth;
				return bucketsForWidth;
			}
			if (bucketWidthString.endsWith("d")) {
				 Days days = Days.daysBetween(fromTime, toTime);
				bucketWidth = Integer.parseInt(bucketWidthString.substring(0, bucketWidthString.length()-1));
				int bucketsForWidth = days.getDays()/bucketWidth;
				return bucketsForWidth;
			}
		}

		Days d = Days.daysBetween(fromTime, toTime);

		if (d.getDays() >= 90) {
			return Weeks.weeksBetween(fromTime, toTime).getWeeks() * 2 * multiplier;
		}
		if (d.getDays() >= 14) {
			return d.getDays() * 2 * multiplier;
		}
		if (d.getDays() >= 7) {
			return d.getDays() * 4  * multiplier;
		}
		if (d.getDays() >= 3) {
			return d.getDays() * 8  * multiplier;
		}
		Hours h = Hours.hoursBetween(fromTime, toTime);
		if (h.getHours() > 24) {
			return h.getHours()  * multiplier;
		}
		if (h.getHours() > 12) {
			return m.getMinutes() / 5;
		}
        if (h.getHours() >= 6) {
            return m.getMinutes() / 3;
        }

        if (h.getHours() >= 4) {
			return m.getMinutes() / 2;
		}
		if (m.getMinutes() >= 120) {
			return m.getMinutes();
		}
		Seconds s = Seconds.secondsBetween(fromTime, toTime);
		if (s.getSeconds() <= LogProperties.getBucketEventDetailThresholdSecs()) {
			if (s.getSeconds() > 60) return s.getSeconds()/2;
			if (s.getSeconds() < 30) return s.getSeconds() * 2;
			return s.getSeconds();
		}
		int mins = m.getMinutes();
		if (mins <= 5) return mins * 4;
		if (mins <= 10) return mins * 10;

		return m.getMinutes();
	}

	int getQueryPart(String key, String logFilter, String end) {
		int indexOf = logFilter.indexOf(key);
		if (indexOf != -1) {
			try {
				return Integer.parseInt(logFilter.substring(indexOf + key.length(), logFilter.indexOf(end, indexOf)));
			} catch (NumberFormatException ex){
			};
		}
		return 0;
	}
	String getQueryPartAsString(String key, String logFilter, String end) {
		int indexOf = logFilter.indexOf(key);
		if (indexOf != -1) {
			try {
				return logFilter.substring(indexOf + key.length(), logFilter.indexOf(end, indexOf));
			} catch (NumberFormatException ex){
			};
		}
		return "";
	}
    static public String getResourceGroup(List<String> terms) {
        String result = null;
        for (String term : terms) {
            String fromToken = "_resourceGroup" + ".equals(";
            if (term.contains(fromToken)) {
                result = StringUtil.substring(term, fromToken, ")");
            }
        }
        return result;
    }


}
