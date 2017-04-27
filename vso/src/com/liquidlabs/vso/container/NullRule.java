package com.liquidlabs.vso.container;

import com.liquidlabs.vso.container.sla.Rule;

public class NullRule extends Rule {
	public NullRule() {
		super("foo", 3, 10);
	}
	
	public Action evaluate(Consumer consumer, Metric[] metrics) {
		return new NullAction("NullRule"); 
	}
}
