package com.liquidlabs.log.search.functions;

public class AverageDeltaPercent extends AverageDelta {

	public Function create() {
		return new AverageDeltaPercent(tag, groupByGroup, applyToGroup);
	}

	public AverageDeltaPercent() {}

	public AverageDeltaPercent(String tag, String groupType, String apply) {
		super(tag, groupType, apply);
	}
    // 34/35 * 100 = 98%  - 100 - 98 = -2% -
    // make it float around 100-
    public double calculateDelta(double currValue, double prevValue) {
        // 100+ is because rickshaw.chart doesnt plot values below 0 -
        return  (100.0 - (currValue/prevValue) * 100.0) * -1;
    }
}
