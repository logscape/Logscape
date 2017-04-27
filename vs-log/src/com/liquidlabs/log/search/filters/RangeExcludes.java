package com.liquidlabs.log.search.filters;

public class RangeExcludes extends RangeIncludes {
	private static final long serialVersionUID = 1L;

	public RangeExcludes() {}
	
	public RangeExcludes(String tag, String group, Number double1,Number double2) {
		super(tag, group, double1, double2);
	}

	public boolean execute(String val) {
		try {
			if (val == null) return false;
			Double value = Double.valueOf(val);
			return value < lowerRange.doubleValue() || value > upperRange.doubleValue();
		}catch (NumberFormatException nfe) {
		}
		
		return false;
	}

}
