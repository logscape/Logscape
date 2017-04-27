package com.liquidlabs.vso.container.sla;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.vso.container.Action;
import com.liquidlabs.vso.container.Consumer;
import com.liquidlabs.vso.container.Metric;
import com.liquidlabs.vso.container.NullRule;
import com.liquidlabs.vso.container.NullTimePeriod;

public class GroovySLA implements SLA {
	//transient private static final Logger LOGGER = Logger.getLogger(GroovySLA.class);
	
	private List<TimePeriod> timePeriods = new ArrayList<TimePeriod>();
	private List<Variable> variables = new ArrayList<Variable>();
	private String consumerClass;
	
	public void addTimePeriod(TimePeriod period) {
		timePeriods.add(period);
	}
	public void setScriptLogger(Logger logger) {
		for (TimePeriod timePeriod : timePeriods) {
			for (Rule rule : timePeriod.getRules()) {
				rule.setScriptLogger(logger);
			}
		}
	}
	
	public Action evaluate(Integer resourceCount, Consumer consumer, Metric...metrics) {
		TimePeriod timePeriod = findTimePeriod(DateTimeUtils.currentTimeMillis());
		Action result = timePeriod.evaluateRule(resourceCount, consumer, metrics);
		if (timePeriod.getLabel() != null) result.setLabel(timePeriod.getLabel());
		return result;
	}
	public Rule getRule(Integer resourceCount){
		TimePeriod timePeriod = findTimePeriod(DateTimeUtils.currentTimeMillis());
		return timePeriod.getRule(resourceCount);
	}
	
	
	public TimePeriod findTimePeriod(long timeMs) {
		for (TimePeriod timePeriod : timePeriods) {
			if (timePeriod.isActive(timeMs)) {
				return timePeriod;
			}
		}
		return new NullTimePeriod(new NullRule());
	}
	public TimePeriod findTimePeriodInRange(long from, long to) {
		// scan in minute intervals
		for (long timeMs = from; timeMs <= to; timeMs += 60000) {
			for (TimePeriod timePeriod : timePeriods) {
				if (timePeriod.isActive(timeMs)) {
					return timePeriod;
				}
			}
		}
		return new NullTimePeriod(new NullRule());

	}

	public String getConsumerClass() {
		return consumerClass;
	}

	public void setConsumerClass(String consumerClass) {
		this.consumerClass = consumerClass;
	}

	public List<Variable> getVariables() {
		if (variables == null) variables = new ArrayList<Variable>();
		return variables;
	}

	public List<TimePeriod> getTimePeriods() {
		return timePeriods;
	}
	public int currentPriority(Integer resourceCount) {
		return findTimePeriod(DateTimeUtils.currentTimeMillis()).currentPriority(resourceCount);
	}

}
