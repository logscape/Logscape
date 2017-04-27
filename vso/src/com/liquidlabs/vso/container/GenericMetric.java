package com.liquidlabs.vso.container;

public class GenericMetric implements Metric {

	private String name;
	private Double value;

	public GenericMetric(String name, Double value) {
		this.name = name;
		this.value = value;
	}
	public GenericMetric(String name, Integer value) {
		this(name, value.doubleValue());
	}
	
	public String name() {
		return name;
	}

	public Double value() {
		return value;
	}
	
	@Override
	public String toString() {
		return String.format("Metric %s=%s",name,value);
	}
	/**
	 * Returns <code>true</code> if this <code>GenericMetric</code> is the same as the o argument.
	 *
	 * @return <code>true</code> if this <code>GenericMetric</code> is the same as the o argument.
	 */
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null) {
			return false;
		}
		if (o.getClass() != getClass()) {
			return false;
		}
		GenericMetric castedObj = (GenericMetric) o;
		return ((this.name == null ? castedObj.name == null : this.name.equals(castedObj.name)) && (this.value == null
			? castedObj.value == null
			: this.value.equals(castedObj.value)));
	}
	

}
