package com.liquidlabs.vso.container;

import com.liquidlabs.vso.container.sla.Rule;
import com.liquidlabs.vso.container.sla.TimePeriod;


public class NullTimePeriod extends TimePeriod {

	public NullTimePeriod(Rule rule) {
		super(rule, "00:00", "23:59");
	}
	
	@Override
	public Action evaluateRule(Integer resourceCount, Consumer consumer, Metric[] metrics) {
		return new NullAction("NullTimePeriod");
	}

}
