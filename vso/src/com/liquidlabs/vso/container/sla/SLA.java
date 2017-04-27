package com.liquidlabs.vso.container.sla;

import java.util.List;

import org.apache.log4j.Logger;

import com.liquidlabs.vso.container.Action;
import com.liquidlabs.vso.container.Consumer;
import com.liquidlabs.vso.container.Metric;

public interface SLA {
	Action evaluate(Integer resourceCount, Consumer consumer, Metric...metrics);

	String getConsumerClass();

	List<Variable> getVariables();

	List<TimePeriod> getTimePeriods();

	void setScriptLogger(Logger logger);

	int currentPriority(Integer resourceCount);

	TimePeriod findTimePeriod(long timeMs);

	TimePeriod findTimePeriodInRange(long from, long to);

	Rule getRule(Integer resourceCount);

}
