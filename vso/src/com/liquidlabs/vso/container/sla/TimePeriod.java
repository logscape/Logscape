package com.liquidlabs.vso.container.sla;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.liquidlabs.vso.container.Action;
import com.liquidlabs.vso.container.Consumer;
import com.liquidlabs.vso.container.Metric;
import com.liquidlabs.vso.container.NullAction;

public class TimePeriod {
	
	private static long T24HOURS = 24 * 60 * 60 * 1000;
	private List<Rule> rules = new ArrayList<Rule>();
	private String start;
	private String label;
	private boolean isOneOff = false;
	boolean hasFired = false;


	public String getStart() {
		return start;
	}


	private String end;

	public TimePeriod() {
	}
 	public TimePeriod(Rule rule, String start, String end) {
		this.rules.add(rule);
		this.start = start;
		this.end = end;
	}

	public boolean isActive() {
		
		
		long millis = getMillis(start);
		long millis2 = getMillis(end);
		
		// rollback the clock if fromTime is GT toTime - 11PM to 6AM - we need 11PM yesterday
		if (millis > millis2) millis -= T24HOURS;
		boolean isActive = new Interval(millis, millis2).containsNow();
		isActive = handleHasFired(isActive);
		return isActive;
	}

	private boolean handleHasFired(boolean isActiveTimePeriod) {
		if (isOneOff) {
			if (hasFired && isActiveTimePeriod) {
				return false;
			}
			// toggle reset after it becomes inactive
			if (!isActiveTimePeriod) {
				hasFired = false;
			}
		}
		return isActiveTimePeriod;
	}
	
	public boolean isActive(long timeInMillis) {
		long millis = getMillis(start);
		long millis2 = getMillis(end);
		if (millis > millis2) millis -= T24HOURS;
		boolean isActive = new Interval(millis, millis2).contains(timeInMillis);
		isActive = handleHasFired(isActive);		
		return isActive;
	}

	private long getMillis(String hhmm) {
		DateTime time = new DateTime();
		time = time.withHourOfDay(Integer.valueOf(hhmm.split(":")[0]));
		time = time.withMinuteOfHour(Integer.valueOf(hhmm.split(":")[1]));
		time = time.minusSeconds(time.getSecondOfMinute());
		return time.getMillis();
	}

	public Action evaluateRule(Integer resourceCount, Consumer consumer, Metric[] metrics) {
		hasFired = true;
		if (rules == null || rules.size() == 0) throw new RuntimeException("No rules available in TimePeriod:");

		for (Rule rule : getRules()) {
			if (resourceCount <= rule.maxResources){
				return rule.evaluate(consumer, metrics);
			}
		}
		return new NullAction("rc:" + resourceCount + " metrics:" + Arrays.toString(metrics) + " ");
	}
	
	public Rule getRule(Integer resourceCount) {
		for (Rule rule : getRules()) {
			if (resourceCount <= rule.maxResources){
				return rule;
			}
		}
		return null;

	}


	public List<Rule> getRules() {
		if (rules == null) rules = new ArrayList<Rule>();
		return rules;
	}

	public void setRules(List<Rule> rules) {
		this.rules = rules;
	}

	public int currentPriority(Integer resourceCount) {
		for (Rule rule : getRules()) {
			if (resourceCount <= rule.maxResources){
				return rule.getPriority();
			}
		}
		return 0;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	public String getLabel() {
		return label;
	}

	public boolean isOneOff() {
		return isOneOff;
	}

	public void setOneOff(boolean oneOff) {
		this.isOneOff = oneOff;
	}

	public long getDurationSeconds() {
		long millis = getMillis(end) - getMillis(start);
		return (millis / 1000);
	}

	
}
