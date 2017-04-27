package com.liquidlabs.log;

import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.filters.*;
import com.liquidlabs.log.search.functions.By;
import com.liquidlabs.log.search.functions.Elapsed;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.search.functions.FunctionFactory;
import com.liquidlabs.log.search.functions.txn.SyntheticTransAccumulate;
import com.liquidlabs.log.search.functions.txn.SyntheticTransTrace;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by neil on 10/12/15.
 */
public class RequestParamAppliers {


    private final static Logger LOGGER = Logger.getLogger(RequestParamAppliers.class);


    public interface RequestParamApplier {
        boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString);
    }


    public static class ApplyVerbose implements RequestParamApplier {
        @Override
        public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
            return applyVerbose(request, paramParts);
        }
    }

    public static class ApplyAutoCancel implements RequestParamApplier {
        @Override
        public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
            return applyAutoCancel(request, paramParts);
        }
    }

    public static class ApplyHitLimit implements RequestParamApplier {
        @Override
        public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
            return applyHitLimit(query,paramParts);
        }
    }

    public static class ApplySynthTrans implements RequestParamApplier {
        @Override
        public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
            return applySynthTrans(query,paramParts);
        }
    }

    public static class ApplySynthTransTrace implements RequestParamApplier {
        @Override
        public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
            return applySynthTransTrace(query, paramParts);
        }
    }

    public static class ApplyElapsed implements RequestParamApplier {
        @Override
        public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
            return applyElapsed(queryString,query, paramParts);
        }
    }
    public static class ApplyByFirstOrLast implements RequestParamApplier {
        @Override
        public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
            return applyByFirstOrLast(query, paramParts);
        }
    }

    public static class ThingsToIgnore implements RequestParamApplier {
        @Override
        public boolean apply(LogRequest request, Query query, List<String> paramParts, int pos, String queryString) {
            return isSuitable(paramParts, "chart")
                    || isSuitable(paramParts, "bucketWidth")
                    || isSuitable(paramParts, "buckets")
                    | isSuitable(paramParts, "top")
                    | isSuitable(paramParts, "bottom")
                    | isSuitable(paramParts, "verbose")
                    || isSuitable(paramParts, "ignoreCase")
                    || isSuitable(paramParts, "lineChartMax")
                    || isSuitable(paramParts, "chartMax")
                    || isSuitable(paramParts, "offset")
                    || isSuitable(paramParts, "tips")
                    || isSuitable2(paramParts, new String[] {"summary", "index"})
                    || isSuitable(paramParts, "replay");
        }
    }
    private static boolean applyHitLimit(Query query, List<String> paramParts) {
        if (!isSuitable(paramParts, "hitLimit")) return false;


        query.setHitLimit(Integer.parseInt(paramParts.get(2)));
        return true;
    }

    private static boolean applyByFirstOrLast(Query query, List<String> paramParts) {
        boolean by =isSuitable(paramParts, "by");
        boolean first =isSuitable(paramParts, "first");
        boolean last =isSuitable(paramParts, "last");
        if (!by && !first && !last) return false;

        if (paramParts.size() == 3) {
            query.addFunction(new By(paramParts.get(1), paramParts.get(2), paramParts.get(0), !first));
        } else {
            query.addFunction(new By(paramParts.get(3), paramParts.get(2), paramParts.get(0), !first));
        }
        return true;
    }



    private static boolean applyAutoCancel(LogRequest request, List<String> paramParts) {
        if (paramParts == null || paramParts.size() != 3) return false;
        String string = paramParts.get(1);
        if (!string.equals("autocancel") && !string.equals("ttl")) return false;

        short parseShort = Short.parseShort(paramParts.get(2));
        if (parseShort == -1) parseShort = Short.MAX_VALUE;
        request.setTimeToLive(parseShort);
        return true;
    }
    private static boolean applyVerbose(LogRequest request, List<String> paramParts) {
        if (!isSuitable(paramParts, "verbose")) return false;
        request.setVerbose(Boolean.parseBoolean(paramParts.get(2)));
        return true;
    }


    private static boolean isSuitable(List<String> paramParts, String expected) {
        if (paramParts == null || paramParts.size() < 2) return false;
        if (!paramParts.get(1).endsWith(expected)) return false;
        return true;
    }
    private static boolean isSuitable2(List<String> paramParts, String[] expected) {
        if (paramParts == null || paramParts.size() < 2) return false;
        if (!paramParts.get(0).endsWith(expected[0])) return false;
        if (!paramParts.get(1).endsWith(expected[1])) return false;
        return true;
    }


    private static boolean applySynthTrans(Query query, List<String> paramParts) {
        if (!isSuitable(paramParts, "txn")) return false ;
        String tag = paramParts.size() > 2 ? paramParts.get(2) : paramParts.get(0);
        String group = paramParts.get(0);

        SyntheticTransAccumulate tx = null;
        if (paramParts.size() == 3) {
            tx = new SyntheticTransAccumulate(paramParts.get(2), group);
        }
        if (tx == null) throw new RuntimeException("Invalid parameters");
        query.addFunction(tx);
        return true;
    }

    private static boolean applySynthTransTrace(Query query, List<String> paramParts) {
        if (!isSuitable(paramParts, "trace")) return false;
        String linkField = paramParts.size() > 2 ? paramParts.get(2) : paramParts.get(0);
        String field = paramParts.get(0);

        SyntheticTransTrace tx = null;
        if (paramParts.size() == 4) {
            tx = new SyntheticTransTrace(field, linkField, paramParts.get(3));
        }
        if (tx == null) throw new RuntimeException("Invalid parameters");
        query.addFunction(tx);
        return true;
    }


    private static boolean applyElapsed(String queryString, Query query, List<String> paramParts) {
        if (paramParts == null || paramParts.size() < 4 || paramParts.size() > 7) return false;
        if (!paramParts.get(1).endsWith("elapsed")) return false;

        if(paramParts.size() == 5) {
            query.addFunction(new Elapsed(paramParts.get(2), paramParts.get(0), paramParts.get(3), paramParts.get(4), "ms", "", lineChart(queryString)));
        } else if (paramParts.size() == 6){
            query.addFunction(new Elapsed(paramParts.get(2), paramParts.get(0), paramParts.get(3), paramParts.get(4), paramParts.get(5), "", lineChart(queryString)));
        } else if (paramParts.size() == 7){
            query.addFunction(new Elapsed(paramParts.get(2), paramParts.get(0), paramParts.get(3), paramParts.get(4), paramParts.get(5), paramParts.get(6), lineChart(queryString)));
        }
        return true;
    }


    private static boolean lineChart(String queryString) {
        return queryString.contains("chart(line)");
    }



    static Filter applyNotFunction(String group, List<String> paramParts, int pos) {
        if (!isSuitable(paramParts, "not") && !isSuitable(paramParts, "exclude")) return null;

        if (paramParts.size() >= 1) {
            return new Not("not-"+pos, group, paramParts.subList(2, paramParts.size()));
        }
        return null;
    }
    static Filter applyGroupNotFunction(String group, List<String> paramParts, int pos) {
        if (!isSuitable(paramParts, "groupNot")) return null;

        if (paramParts.size() >= 2) {
            return new Not("gNot-"+pos, group, paramParts.subList(2, paramParts.size()));
        }
        return null;
    }
    static Filter applyContainsFunction(String group, List<String> paramParts, int pos) {
        if (!isSuitable(paramParts, "contains") && !isSuitable(paramParts, "include")) return null;


        if (paramParts.size() >= 2) {
            return new Contains("c-"+pos, group, paramParts.subList(2, paramParts.size()));
        }
        return null;
    }
    static Filter applyEqualsFunction(String group, List<String> paramParts, int pos) {
        if (!isSuitable(paramParts, "equals")) return null;

        if (paramParts.size() >= 2) {
            return new Equals("e-"+pos, group, paramParts.subList(2, paramParts.size()));
        } else if (paramParts.size() ==1) {
            return new Equals("e-"+pos, group, "");
        }
        return null;
    }
    static Filter applyGroupContainsFunction(String group, List<String> paramParts, int pos) {
        if (!isSuitable(paramParts, "groupContains")) return null;

        if (paramParts.size() >= 1) {
            return new Contains("gC-"+pos, group, paramParts.subList(2, paramParts.size()));
        }
        return null;
    }
    static Filter applyRangeIncludes(String group, List<String> paramParts, int pos) {
        if (!isSuitable(paramParts, "rangeIncludes")) return null;

        if (paramParts.size() == 4) {
            return new RangeIncludes("rI-"+pos, group,  new Double(paramParts.get(2)), new Double(paramParts.get(3)));
        }
        return null;
    }
    static Filter applyRangeExcludes(String group, List<String> paramParts, int pos) {
        if (!isSuitable(paramParts, "rangeExcludes")) return null;

        if (paramParts.size() == 4) {
            return new RangeExcludes("rE-"+pos, group, new Double(paramParts.get(2)), new Double(paramParts.get(3)));
        }
        return null;
    }


    public static class ParamSet {


        public ParamSet(){
        }

        public ParamSet(String function, String tag, String groupBy, String applyTo) {
            this.function = function;
            this.tag = tag;
            this.groupBy = groupBy;
            this.applyTo = applyTo;
        }

        public ParamSet (String function, String tag, String groupBy, String...params) {
            this.function = function;
            this.tag = tag;
            this.groupBy = groupBy;
            this.params = params;
        }

        public ParamSet(String function, String tag, String groupBy, String applyTo, Filter filter1) {
            this.function = function;
            this.tag = tag;
            this.groupBy = groupBy;
            this.applyTo = applyTo;
            this.filter1 = filter1;
        }
        public ParamSet(String function, String tag, String groupBy, String applyTo, Filter filter1, Filter filter2) {
            this.function = function;
            this.tag = tag;
            this.groupBy = groupBy;
            this.applyTo = applyTo;
            this.filter1 = filter1;
            this.filter2 = filter2;
        }
        public ParamSet(String filterName, String string, Number parseDouble) {
            this.filterName = filterName;
            this.groupBy = string;
            this.number = parseDouble;
        }
        String function;
        String tag;
        String groupBy;
        String applyTo;
        String[] params;
        Filter filter1;
        Filter filter2;
        String filterName;
        Number number;
    }
}
