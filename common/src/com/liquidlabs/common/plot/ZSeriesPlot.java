package com.liquidlabs.common.plot;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.joda.time.DateTime;


/**
 * Allows a series of XYPlots to be stored together. i.e. appName[1-4] with resourceCount history
 */
public class ZSeriesPlot<T> {
	Map<String, XZPlot<T>> series = new ConcurrentHashMap<String, XZPlot<T>>();
	private boolean allowBackwardsProjection;
	private T emptyItem;
	private int countToKeep = Integer.getInteger("vscape.plot.keep", 16 * 1024);
	
	public ZSeriesPlot() {
	}
	public ZSeriesPlot(boolean allowBackwardsProjection, int countToKeep) {
		this.allowBackwardsProjection = allowBackwardsProjection;
		this.countToKeep = countToKeep;
	}

	public ZSeriesPlot(boolean b, T emptyItem) {
		this.allowBackwardsProjection = b;
		this.emptyItem = emptyItem;
	}
	public void add(String seriesName, XZPoint<T> point) {
		getPlot(seriesName).add(point);
	}

	public Set<String> getSeriesNames() {
		return series.keySet();
	}

	public XZPoint<T> get(String seriesName, DateTime timeMs) {
		XZPlot<T> plot = getPlot(seriesName);
		return plot.getPoint(timeMs.getMillis());
	}

	private XZPlot<T> getPlot(String seriesName) {
		if (seriesName == null) throw new IllegalArgumentException("SeriesName cannot be NULL");
		XZPlot<T> plot = series.get(seriesName);
		if (plot == null) {
				series.put(seriesName, new XZPlot<T>(allowBackwardsProjection, emptyItem, countToKeep));
		}
		return series.get(seriesName);
	}

	public List<XZPoint<T>> getFrom(String series, DateTime fromTimeMs, DateTime toTimeMs, long intervalMs, boolean ignoreDuplicates, T fromZ) {
		
		int includeItemsFrom = getZPosition(series, fromZ, fromTimeMs, toTimeMs, intervalMs);
		ArrayList<XZPoint<T>> results = new ArrayList<XZPoint<T>>();
		T lastZ = null;
		int pos = -1;
		for (long time = fromTimeMs.getMillis(); time <= toTimeMs.getMillis(); time+=intervalMs) {
			
			DateTime now = new DateTime(time);
			
			pos++;
			
			if (pos <= includeItemsFrom) {
				continue;
			}
			
			XZPoint<T> point = get(series, now);
			if (point == null) continue;
			
			if (ignoreDuplicates && lastZ != null && lastZ.equals(point.getZ())) {
				continue;
			}
			lastZ = point.getZ();
			
			results.add(point);
			
		}
		return results;
	}

	private int getZPosition(String series, T fromZ, DateTime fromTimeMs, DateTime toTimeMs, long intervalMs) {
		if (fromZ == null) return -1;
		int result = -1;
		int count = 0;
		for (long time = fromTimeMs.getMillis(); time <= toTimeMs.getMillis(); time+=intervalMs) {
			DateTime now = new DateTime(time);
			XZPoint<T> point = get(series, now);
			if (point == null) continue;
			if (point.getZ() != null && point.getZ().equals(fromZ)) {
				result = count; 
			}
			count++;
		}
		
		return result;
	}

}
