/**
 * 
 */
package com.liquidlabs.vso.container;

import com.liquidlabs.vso.container.Metric;

class PretendMetric implements Metric {

	private String name;
	private Double value;

	public PretendMetric(String name, Double value) {
		this.name = name;
		this.value = value;
	}
	
	public String name() {
		return name;
	}

	public Double value() {
		return value;
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[PretendMetric:");
		buffer.append(" name: ");
		buffer.append(name);
		buffer.append(" value: ");
		buffer.append(value);
		buffer.append("]");
		return buffer.toString();
	}
	
}