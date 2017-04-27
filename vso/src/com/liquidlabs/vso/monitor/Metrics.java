package com.liquidlabs.vso.monitor;

import java.util.Date;
import java.util.UUID;

import com.liquidlabs.orm.Id;
import com.liquidlabs.vso.container.Metric;

public class Metrics {

	@Id
	private String id = UUID.randomUUID().toString();
	private String ownerId;
	private String metrics;
	private String bundle;
	private String time;
	private long timeStamp;
	private int priority;
	private String consumerName;
	
	public Metrics() {}
	
	public Metrics(String ownerId, String bundle, Metric[] metrics, String consumerName, int priority) {
		this.ownerId = ownerId;
		this.bundle = bundle;
		StringBuilder builder = new StringBuilder();
		
		for (Metric metric : metrics) {
			builder.append(metric.name()).append("=").append(metric.value()).append(",");
		}
		if (builder.length() > 0) builder.deleteCharAt(builder.length()-1);
		this.metrics = builder.toString();
		this.consumerName = consumerName;
		this.priority = priority;
		time = new Date().toString();
		timeStamp = System.currentTimeMillis();
	}

	public String getOwnerId() {
		return ownerId;
	}

	public String getMetrics() {
		return metrics;
	}
	
	public String getBundle() {
		return bundle;
	}
	public String getTime() {
		return time;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	
	public String asString() {
		StringBuilder builder = new StringBuilder();
		builder.append(time).append(" -> ").append("consumer").append("=").append(consumerName).append(", priority=").append(priority).append(",").append(metrics).append("\n");
		return builder.toString();
	}

	public String getId() {
		return id;
	}
	public boolean equals(Object arg0) {
		Metrics other = (Metrics) arg0;
		return this.getTime().equals(other.getTime());
	}
	
}
