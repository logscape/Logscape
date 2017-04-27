package com.liquidlabs.log.search.functions;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.field.FieldBase;
import com.liquidlabs.log.space.LogRequest;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import  parsii.eval.*;
import parsii.tokenizer.ParseException;

/**
 * From here:
 * https://github.com/scireum/parsii
 *
 * Example search
 * * | bytes.sum(_host, AGG) AGG.expr(AGG / 1024 / 1024)
 */

public class EvaluateExpr extends FunctionBase implements FunctionFactory, GlobalFunction {

    private final static Logger LOGGER = Logger.getLogger(EvaluateExpr.class);

    private static final long serialVersionUID = 1L;
    public static final String EACH = "EACH";

    String tag;
	public String groupByGroup;
	public String applyToGroup;

    Map<String, Double> groups = new ConcurrentHashMap<String, Double>();


	public EvaluateExpr() {}

	public EvaluateExpr(String tag, String groupByGroup, String applyToGroup) {
        super(EvaluateExpr.class.getSimpleName());
		this.tag = tag;
        if (tag.equals("0")) this.tag = "eval";
		this.groupByGroup = groupByGroup;
		this.applyToGroup = applyToGroup;
	}
	
	public boolean isBucketLevel() {
		return true;
	}
	public String group() {
		return applyToGroup;
	}
	
	public Function create() {
		return new EvaluateExpr(tag, groupByGroup, applyToGroup);
	}
	
	public boolean execute(FieldSet fieldSet, String[] fields, String pattern, long time, MatchResult matchResult, String rawLineData, long requestStartTimeMs, int lineNumber) {
		final String[] applyThenGroup = super.getValues(matchResult, fieldSet, fields, applyToGroup, groupByGroup, tag, lineNumber);
		execute(applyThenGroup[0], applyThenGroup[1]);
        return true;
	}

    // CPU: |  CPU.avg(,POST) +POST.eval(CPU * 100)
    // CPU: |  CPU.avg(_host,POST) +POST.eval(CPU * 100)
    // groupField = CPU!alteredcarbon.local || groupField = CPU
	final public void execute(String applyField, final String groupField) {
		if (applyField == null || applyField.length() == 0) return;
		if (groupField == null && groupByGroup != null && groupByGroup.length() > 0) return;


		try {
            String[] vars = groupField.split(FunctionBase.TAG_SEPARATOR);
            String varField = vars[0];
            String groupKey = vars.length == 2? vars[1] : varField;


            Scope scope = Scope.create();
            Variable a = scope.getVariable(varField);
            Expression expr = Parser.parse(groupByGroup, scope);
            a.setValue(Double.parseDouble(applyField));
            groups.put(groupKey, expr.evaluate());

            return;
		} catch (ParseException e) {
            e.printStackTrace();
        }
    }

	@SuppressWarnings("unchecked")
	public Map getResults() {
        if (applyToGroup == null || applyToGroup.length() == 0 || applyToGroup.equals("0")) {
            HashMap hashMap = new HashMap();
            hashMap.put("0",1);
            return hashMap;
        }
        return getGroups();
	}

	public String getTag() {
		return tag;
	}

	//////////// Bucket to Bucket version
	public void updateResult(String key, Map<String, Object> map) {
	}

	public void updateResult(String groupName, Number value) {
	}


    public void extractResult(FieldSet fieldSet, String currentGroupKey, String key, Map<String, Object> otherFunctionMap, ValueSetter valueSetter, long start, long end, int currentBucketPos,
                              int querySourcePos, int totalBucketCount, Map<String, Object> sharedFunctionData, long requestStartTimeMs, LogRequest request) {


        Scope scope = Scope.create();
//        Variable a = scope.getVariable(varField);
        Expression expr = null;
        try {
            expr = Parser.parse(groupByGroup, scope);
            //a.setValue(Double.parseDouble(applyField));
            double evaluate = expr.evaluate();
            if (valueSetter != null) valueSetter.set(tag, evaluate, true);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void extractResult(String key, Number object, ValueSetter valueSetter, Map<String, Object> sharedFunctionData, int querySourcePos, int currentBucketPos, int totalBucketCount,
                              Map<String, Object> sourceDataForBucket, long start) {
//        Double myValue = groups.get("RESULT");
        for (Entry<String, Double> entry : groups.entrySet()) {
            String entryKey = entry.getKey();
            if (valueSetter != null) valueSetter.set(applyTag(tag, entryKey), entry.getValue(), true);
        }
    }

	public Map<String, Double> getGroups() {
		return groups;
	}
	public void flushAggResultsForBucket(int querySourcePos, Map<String, Object> sharedFunctionData) {
	}

	@Override
	public String getApplyToField() {
		return applyToGroup;
	}
	public String groupByGroup() {
		return groupByGroup;
	}


	@Override
	public void handle(Map<String, Object> values, ValueSetter valueSetter) {

        if (groupByGroup.contains(EACH)) {
            handleEachValue(values, valueSetter);
        } else {
            handleSingleValue(values, valueSetter);
        }
    }

    private void handleEachValue(Map<String, Object> values, ValueSetter valueSetter) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Eval contains EACH, will handleEach values:" + values);

        Scope scope = Scope.create();

        Expression expr = null;
        try {
            expr = Parser.parse(groupByGroup, scope);
            if (LOGGER.isDebugEnabled()) LOGGER.debug("EACH expression:" + expr);
            for (Entry<String, Object> fieldValue : values.entrySet()) {
                Variable var = scope.getVariable(EACH);
                if (var != null) {
                    Object value = fieldValue.getValue();
                    if (value instanceof Integer) {
                        var.setValue(((Integer) fieldValue.getValue()).doubleValue());
                    } else if (value instanceof Double) {
                        var.setValue((Double) fieldValue.getValue());
                    } else if (value instanceof String) {
                        Double aDouble = StringUtil.isDouble((String) value);
                        if (aDouble != null) {
                            var.setValue(aDouble);
                        }
                    }


                }
                double evaluate = expr.evaluate();
                if (LOGGER.isDebugEnabled()) LOGGER.debug("EACH Expression:" + expr + " fieldKey:" + fieldValue.getKey() + " Evaluate:" + evaluate + " var:" + var);
                valueSetter.set(fieldValue.getKey(), evaluate, true);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private void handleSingleValue(Map<String, Object> values, ValueSetter valueSetter) {
        Scope scope = Scope.create();

        Expression expr = null;
        try {
            expr = Parser.parse(groupByGroup, scope);
            for (Entry<String, Object> fieldValue : values.entrySet()) {
                Variable var = scope.getVariable(fieldValue.getKey());
                if (var != null) {
                    Object value = fieldValue.getValue();
                    if (value instanceof Integer) {
                        var.setValue(((Integer) fieldValue.getValue()).doubleValue());
                    } else if (value instanceof Double) {
                        var.setValue((Double) fieldValue.getValue());
                    }


                }
            }
            double evaluate = expr.evaluate();
            valueSetter.set(tag, evaluate, true);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
