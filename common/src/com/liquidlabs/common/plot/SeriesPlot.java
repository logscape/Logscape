package com.liquidlabs.common.plot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Allows a series of XYPlots to be stored together. i.e. appName[1-4] with resourceCount history
 */
public class SeriesPlot {
	Map<String, XYPlot> series = new ConcurrentHashMap<String, XYPlot>();
	private boolean allowBackProjectionOnFirstItem = true;
	
	public SeriesPlot() {
	}
	public SeriesPlot(boolean allowBackProjectionOnFirstItem) {
		this.allowBackProjectionOnFirstItem = allowBackProjectionOnFirstItem;
	}

	public void add(String seriesName, XYPoint point) {
		if (!series.containsKey(seriesName)) {
			series.put(seriesName, new XYPlot());
		}
		series.get(seriesName).add(point);
	}

	public Set<String> getSeriesNames() {
		return series.keySet();
	}

	public XYPoint get(String seriesName, long timeMs) {
		XYPlot plot = getPlot(seriesName);
		return plot.getPoint(timeMs);
	}
	/**
	 * 
	 * @param seriesName
	 * @param timeMs
	 * @param range is used to determine the prior and post search ranges for this timeMs
	 * @return
	 */
	public XYPoint get(String seriesName, long timeMs, long range) {
		XYPlot plot = getPlot(seriesName);
		return plot.getPoint(timeMs, range);
	}

	public void incrementValue(String seriesName, long timeMs) {
		XYPlot plot = getPlot(seriesName);
		XYPoint last = plot.getLast(timeMs);
		last.setY(last.getY() + 1);
		plot.add(last);
	}
	public void decrementValue(String seriesName, long timeMs) {
		XYPlot plot = getPlot(seriesName);
		XYPoint last = plot.getLast(timeMs);
		last.setY(last.getY() - 1);
		plot.add(last);
	}
	
	public XYPlot get(String seriesName) {
		return getPlot(seriesName);
	}
	private XYPlot getPlot(String seriesName) {
		if (seriesName == null) throw new IllegalArgumentException("SeriesName cannot be NULL");
		XYPlot plot = series.get(seriesName);
		if (plot == null) {
				series.put(seriesName, new XYPlot(allowBackProjectionOnFirstItem));
		}
		return series.get(seriesName);
	}
	public void setValue(String seriesName, Long value) {
		if (!series.containsKey(seriesName)) {
			series.put(seriesName, new XYPlot());
		}
		XYPlot plot = series.get(seriesName);
		XYPoint last = plot.getLast(System.currentTimeMillis());
		last.setY(value);
		plot.add(last);
	}
	public void setValue(String seriesName, Long value, Long time) {
		if (!series.containsKey(seriesName)) {
			series.put(seriesName, new XYPlot());
		}
		XYPlot plot = series.get(seriesName);
		XYPoint last = plot.getLast(time);
		last.setY(value);
		plot.add(last);
	}

}
