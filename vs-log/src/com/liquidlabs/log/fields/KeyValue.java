package com.liquidlabs.log.fields;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class KeyValue implements Comparable {

	public String field;
	public String key;
	public double value;
	public String valueString;
	public int percent;

    static DecimalFormat format = new DecimalFormat("#,##0.###");

	
	public KeyValue() {
	}
	public KeyValue(String field, String key, double value, int percent) {
		this.field = field;
		this.key = key;
		setValue(value);
		this.percent = percent;
	}

	public void setValue(double value) {
		this.value = value;
		this.value = value;
		this.valueString = format.format(value);
		if (valueString.contains(".00")) {
			this.valueString = NumberFormat.getIntegerInstance().format(value);
		}
	}

	public String toString() {
		return String.format("%s.%s", key, value);
	}

	@Override
	public int compareTo(Object o) {
		KeyValue other = (KeyValue) o;
		return Double.compare(other.value, this.value);
	}
}
