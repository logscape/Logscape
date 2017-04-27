package com.liquidlabs.log.search;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.ExpressionEvaluator;
import com.liquidlabs.common.regex.ExpressionEvaluatorListBased;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Contains;
import com.liquidlabs.log.search.filters.Equals;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.search.filters.Not;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.functions.FunctionBase;
import com.liquidlabs.log.search.tailer.TailerEmbeddedAggSpace;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;


public class Query implements Serializable{
    static final Logger LOGGER = Logger.getLogger(Query.class);
	private static final String DOT_STAR = ".*";

	private static final String COMMA = ",";

	private static final String OLD_SCHOOL = "old_school";

	private static final String STAR = "*";

	private static final long serialVersionUID = 1L;
	
	private String pattern;
	
	private List<Function> functions = new ArrayList<Function>();
	private List<Filter> filters = new ArrayList<Filter>();
	private int pos;
	private int sourcePos;
	private int groupId;
	private int hitLimit = -1;
	public boolean ignoreCase = false;

	private String original;
	private short topLimit = (short) LogProperties.getDefautTopLimit();
	private boolean topOther = false;
	private boolean replay = true;

	private transient ExpressionEvaluator expressionEvaluator;

	public Query(){}
	public Query(int pos, String pattern) {
		this(pos, pos, pattern, pattern, true);
	}
	
	public Query(int pos, int sourcePos, String pattern, String original, boolean replay) {
		this.pos = pos;
		this.sourcePos = sourcePos;
		this.pattern = pattern.trim();
		this.original = original.trim();
		this.replay = replay;
	}
	public int position() {
		return pos;
	}
	public void setTopLimit(short topLimit) {
		this.topLimit = topLimit;
	}
	public int topLimit() {
		return topLimit;
	}
	public boolean isTopOther(){
		return this.topOther;
	}
	
	public void addFunction(Function function) {
		this.functions.add(function);
	}
	
	public void addFilter(Filter filter) {
		if (filter != null) this.filters.add(filter);
	}


	public List<Function> functions() {
		List<Function> results = new CopyOnWriteArrayList<Function>();
        functions = consolidatePostAggFuctions(functions);
		for (Function function : functions) {
			if (function.isBucketLevel() && !postAggFunctions.contains(function)) {
				results.add(function.create());
			} else {
				results.add(function);
			}
		}
		return results;
	}


    private List<Function> consolidatePostAggFuctions(List<Function> functions) {
        // only run this on the WebServer
        getPostAggFunctions(functions);
        List<Function> results = new ArrayList<Function>();
        for (Function function : functions) {
            if (!postAggFunctions.contains(function)) results.add(function);
        }
        results.addAll(postAggFunctions);
        return results;
    }

    /**
     * Post Agg Functions are 1 per Query and Not Per bucket/
     * ie. field.count(,TAG) TAG.max()
     */
    transient List<Function> postAggFunctions;
    private void getPostAggFunctions(List<Function> functions) {
        if (postAggFunctions == null) {
            postAggFunctions = new ArrayList<Function>();
            for (Function function : functions) {
                postAggFunctions.addAll(FunctionBase.getPostAggFunctions(function, functions));
            }
        }
    }


    public List<Filter> filters() {
		return filters;
	}

	/**
	 * Return the RegexpPattern
	 * @return
	 */
	public String pattern() {
		return pattern;
	}

    transient String toStringCache;
	public String toString() {
        if (toStringCache == null) toStringCache =String.format("%s ptrn:'%s' sPos:%s pos:%s replay:%b fun%s filter%s", "Query", pattern, sourcePos, pos, replay, functions, filters());
		return toStringCache;
	}
	public String toStringId() {
		StringBuilder sb = new StringBuilder();
		for (Function fun : functions) {
			sb.append(fun.toStringId());
		}
		StringBuilder sbf = new StringBuilder();
		for (Filter filter : filters) {
			sbf.append(filter.toStringId());
		}
		return String.format("p:%s fun:%s fil:%s pos:%d spos:%d gId:%d", pattern, sb.toString(), sbf.toString(), pos,sourcePos, groupId);
	}

	final public MatchResult matches(String nextLine) {

        if (nextLine == null) {
            LOGGER.warn("Query got NULL");
            return this.expressionEvaluator.FALSE_matchResult;
        }
        int gotTo = 0;
		try {
			if (this.expressionEvaluator == null) {
				this.expressionEvaluator = new ExpressionEvaluator(pattern);
			}
            gotTo++;

            if (this.expressionEvaluator.isEvalError()) return this.expressionEvaluator.FALSE_matchResult;
            gotTo++;

            MatchResult evaluate = this.expressionEvaluator.evaluate(nextLine);
            gotTo++;
            if (evaluate.isMatch()) {
                  return evaluate;
                // filters are field based so may not occur on the raw line data
            }
            return evaluate;
		} catch (Throwable t) {
			LOGGER.warn("Query Failed to match:\n\t" + this.pattern + "\n\t to LINE:\n\t" + nextLine + "\n\t ex:" + t.toString()+ " GOT_TO:" + gotTo);
			return this.expressionEvaluator.FALSE_matchResult;
		}
	}

    /**
     * Returns list of ANDs where each item is a set of ORs - i.e. a.contains(A,B,C) AND b.contains(C,D,E)
     * @return
     */
    public List<ExpressionEvaluatorListBased.Item> getFilterContains() {
        ArrayList<ExpressionEvaluatorListBased.Item> results = new ArrayList<ExpressionEvaluatorListBased.Item>();
        for (Filter filter : filters) {
             // dont apply to system fields - only user field against 'contains')
            if (!filter.group().startsWith("_") && isContainsFilter(filter)) {
                results.add(new ExpressionEvaluatorListBased.Item(filter.values()));
            }
        }
        return results;
    }

    private boolean isContainsFilter(Filter filter) {
        return filter instanceof Contains;
    }

    transient int warned = 0;
	final public boolean isPassedByFilters(FieldSet fieldSet, String[] nextEvents, String lineData, MatchResult matchResult, int lineNumber) {
        if (filters.isEmpty()) return true;
		for (Filter filter : filters) {
			try {
                boolean postAggregate = Filter.FilterUtil.isPostAggregate(filter.group(), this.original);
                if (!postAggregate && !filter.isPassed(fieldSet, nextEvents, lineData, matchResult, lineNumber)) {
					return false;
				}
                // if its a group 0 filter it means the whole line - we passed so lets filter against hostname, filename and path

                if (!postAggregate && filter.group().equalsIgnoreCase("0")) {
					boolean hostPassed = !filter.isPassed(fieldSet, nextEvents, fieldSet.getFieldValue(FieldSet.DEF_FIELDS._host.name(), nextEvents), matchResult, lineNumber);
                    boolean pathPassed = !filter.isPassed(fieldSet, nextEvents, fieldSet.getFieldValue(FieldSet.DEF_FIELDS._path.name(), nextEvents), matchResult, lineNumber);
                    boolean filenamePassed  =!filter.isPassed(fieldSet, nextEvents, fieldSet.getFieldValue(FieldSet.DEF_FIELDS._filename.name(), nextEvents), matchResult, lineNumber);
					boolean linePassed = filter.isPassed(fieldSet, nextEvents, lineData, matchResult, lineNumber);
					if (!linePassed && !hostPassed && !pathPassed && !filenamePassed) return false;
                }
			} catch (Throwable t) {
				if (warned == 0) t.printStackTrace();
				warned++;
			}
		}
		return true;
	}
    final public boolean isPassedByFilters2(Map<String,Object> fields, String lineData, MatchResult matchResult, int lineNumber) {
        if (filters.isEmpty()) return true;
        for (Filter filter : filters) {
            try {
                String group = filter.group();
                Integer intValue = StringUtil.isInteger(group);
                if (intValue != null) {
                    group = matchResult.group(intValue);
                } else {
                    group = fields.get(filter.group()).toString();
                }
                if (!Filter.FilterUtil.isPostAggregate(filter.group(), this.original) && !filter.execute(group)) {
                    return false;
                }
            } catch (Throwable t) {
                if (warned == 0) t.printStackTrace();
                warned++;
            }
        }
        return true;
    }
	

	public Query copy(Map<String, Map<Long, AtomicLong>> hitCounter) {
		Query query = new Query(pos, sourcePos, pattern,  original, replay);
		query.hitCounter = hitCounter;
		query.functions = functions;
		query.filters = filters;
		query.hitLimit = hitLimit;
		query.groupId = groupId;
		query.topLimit = topLimit;
		query.ignoreCase = ignoreCase;
		return query;
	}

	public boolean equals(Object other) {
		Query q = (Query) other;
		return q.pos == pos && q.pattern.equals(pattern);
	}
	public String key() {
		return position()+pattern();
	}

	public int getSourcePos() {
		return sourcePos;
	}

	public int sourcePosition() {
		return sourcePos;
	}

	public String sourceQuery() {
		return original;
	}

	public void setHitLimit(int hitLimit) {
		this.hitLimit = hitLimit;
	}

	public int hitLimit() {
		return hitLimit;
	}
	
	public int groupId() {
		return groupId;
	}
	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}
	
	public boolean isReplay() {
		return  replay;
	}
	public void setReplay(boolean replay) {
		this.replay = replay;
	}

	public transient Map<String, Map<Long, AtomicLong>> hitCounter = new ConcurrentHashMap<String, Map<Long, AtomicLong>>();
	// increment bucket hit count for this bucketTime period so we can trigger a hitLimit
	public void increment(long bucketTime) {
		if (hitCounter != null) {
			Map<Long, AtomicLong> map = hitCounter.get(qKey);
			if (map == null) {
				map = new ConcurrentHashMap<Long, AtomicLong>();
				hitCounter.put(qKey, map);
			}
            AtomicLong time = map.get(bucketTime);
            if (time != null) time.incrementAndGet();
            else map.put(bucketTime, new AtomicLong());
		}
	}
	
	transient final String qKey = this.original + this.pos + "" + this.sourcePos + "" + this.groupId;
	final public boolean isHitLimitExceeded(long bucketTime) {
	
		if (bucketTime == -1 || this.hitLimit == -1) return false;
		
		if (hitCounter != null) {
            Map<Long, AtomicLong> timeMap = hitCounter.get(qKey);
            if (timeMap != null) {
                AtomicLong atomicLong = timeMap.get(bucketTime);
                if (atomicLong != null) return (atomicLong.get() >= this.hitLimit);
//            } else {
//                hitCounter.put(qKey, new FastMap<Long, AtomicLong>().shared());
//                hitCounter.get(qKey).put(bucketTime, new AtomicLong(1));
            }
        }
		return false;
	}
    final public int getHits(long bucketTime) {
        Map<Long, AtomicLong> longAtomicLongMap = hitCounter.get(qKey);
        if (longAtomicLongMap != null) {
            AtomicLong count = longAtomicLongMap.get(bucketTime);
            if (count != null) return (int) count.get();
        }
        return 0;
    }

	final public MatchResult isMatching(String lineData) {
		return matches(lineData);
	}
	public boolean isBucketLevel() {
		for (Function function : this.functions()) {
			if (function.isBucketLevel()) return true;
		}
		return functions.size() == 0;
	}

	public boolean isGroupBy() {
		return !functions.isEmpty();
	}
	public void setTopOther(boolean b) {
		this.topOther = b;
		
	}

    public void removeSystemFieldFilters() {
        Set<Filter> removeMe = new HashSet<Filter>();
        for (Filter filter : filters) {
            if (FieldSet.isDefaultField(filter.group())) {
                removeMe.add(filter);
            }
        }
        filters.removeAll(removeMe);
    }

    public boolean isFilter(boolean include, Map<String, String> systemFields) {
        for (Filter filter : filters) {
            if (!filter.group().startsWith("_")) continue;
            if (include) {
                if (!(filter instanceof Equals || filter instanceof Contains)) continue;
            } else {
                // only exclude filters
                if (!(filter instanceof  Not)) continue;
            }
            if (systemFields.containsKey(filter.group())) {
                if (!filter.execute(systemFields.get(filter.group()))) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean containsFields(FieldSet fieldSet) {
        List<Function> funcs = this.functions;
        if (funcs.isEmpty()) return true;
        if (funcs.size() > 0 && funcs.get(0).getApplyToField().startsWith("_")) return true;
        boolean isFound = false;

        for (Function func : funcs) {
            String fieldName = func.getApplyToField();
            if (fieldName.contains("+")) {
                fieldName = fieldName.substring(0, fieldName.indexOf("+"));
            }


            if (fieldName.startsWith("_")) isFound = true;
            // might be grabbing by group = i.e. numeric fieldName
            else if (StringUtil.isIntegerFast(fieldName)) isFound = true;
            if (!isFound && fieldSet.containsField(fieldName)) isFound = true;

			if (fieldName.contains("*") || fieldName.contains("[]")) return true;
        }

        return isFound;
    }
    public boolean containsFields(Map<String,Object> fieldSet, Map<String,String> discoFields) {
        List<Function> funcs = this.functions;
        if (funcs.isEmpty()) return true;
        boolean isFound = false;
        for (Function func : funcs) {
            String fieldName = func.getApplyToField();
            // assumed system field
            if (fieldName.startsWith("_")) return true;
            if (fieldName.contains("+")) {
                fieldName = fieldName.substring(0, fieldName.indexOf("+"));
            }
            // might be grabbing by group = i.e. numeric fieldName
            if (StringUtil.isIntegerFast(fieldName)) isFound = true;
            if (!isFound && fieldSet.containsKey(fieldName) || discoFields.containsKey(fieldName)) return true;
        }

        return isFound;
    }

}
