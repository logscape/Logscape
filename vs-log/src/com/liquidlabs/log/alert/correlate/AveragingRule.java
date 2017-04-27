package com.liquidlabs.log.alert.correlate;

import java.util.List;

public class AveragingRule implements Rule {

	private final int value;

	public AveragingRule(int value) {
		this.value = value;
	}
	
	@Override
	public boolean complete() {
		return false;
	}

	@Override
	public Rule copy() {
		return new AveragingRule(value);
	}

	@Override
	public boolean evaluate(Event event) {
		return false;
	}

	@Override
	public boolean evaluate(List<Event> events) {
        return events.size() > 0 ? total(events) / events.size() >= value : false;
		
	}

    private double total(List<Event> events) {
        double total = 0;
        for(Event e : events) {
            try {
                total += Double.parseDouble(e.getFieldValue());
            }catch(NumberFormatException nfe) {}
        }
        return total;
    }

    @Override
    public String describe(List<Event> events) {
        if (events.size() > 0) {
            return "Average of " + total(events)/events.size() + " >= " + value;
        }
        return "Average >= " + value;
    }

}
