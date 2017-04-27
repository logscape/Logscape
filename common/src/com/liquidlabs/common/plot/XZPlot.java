package com.liquidlabs.common.plot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Holds XYEvent Data and provides time consistent retrieval. will use linear interpolation
 * between known points. After the last point, it will simply extend the value
 *
 */
public class XZPlot<T> {
	long lastTimeMs = 0;
	
	Map<Long, XZPoint<T>> points = Collections.synchronizedMap(new LinkedHashMap<Long, XZPoint<T>>(){
		private static final long serialVersionUID = 1L;
		protected boolean removeEldestEntry(Entry<Long, XZPoint<T>> eldest) {
			
			// TODO: need to revisit this - may be better to warehouse the holder items, i.e. save every second one etc
			return size() > 2048;
		}
	});

	private boolean allowBackwardsProjection;

	private T emptyItem;
	
	public XZPlot() {
	}
	public XZPlot(final int countToKeep) {
		setPointsCollection(countToKeep);
	}
	
	public XZPlot(boolean allowBackwardsProjection, T emptyItem, int countToKeep) {
		setPointsCollection(countToKeep);
		this.allowBackwardsProjection = allowBackwardsProjection;
		this.emptyItem = emptyItem;
	}
	public void add(XZPoint<T> point) {
		lastTimeMs = point.getTimeMs();
		points.put(point.getTimeMs(), point);
	}

	public int getPointCount() {
		return points.size();
	}

	public XZPoint<T> getPoint(long atTime) {
		
		while (true) {
			try {
				if (points.size() == 0) {
					XZPoint<T> point = new XZPoint<T>(atTime, null);
					add(point);
					return point;
				}
				// direct hit
				if (points.containsKey(atTime)) return points.get(atTime);
				
				// before
				if (points.keySet().iterator().next() > atTime) {
					if (!allowBackwardsProjection) {
						if (emptyItem != null) return new XZPoint<T>(atTime, emptyItem);
						return null;
					}
					return getFirst(atTime);
				}
				
				// after
				if (atTime > lastTimeMs) {
					return getLast(atTime);
				}
				
				// between
				XZPoint<T> before = getPointBefore(atTime);
				if (before == null) return getFirst(atTime);
				XZPoint<T> after = getPointAfter(atTime);
				if (after == null) return getLast(atTime);
				
				return new XZPoint<T>(atTime, before.getZ());
			} catch (ConcurrentModificationException ex){
			}
		} 
	}

	public XZPoint<T> getFirst(long time) {
		XZPoint<T> next = new XZPoint<T>(points.values().iterator().next());
		next.setTimeMs(time);
		return next;
	}

	public XZPoint<T> getLast(long i) {
		ArrayList<XZPoint<T>> point = new ArrayList<XZPoint<T>>(points.values());
		XZPoint<T> lastPoint = points.size() == 0 ? new XZPoint<T>(i, null) : point.get(point.size()-1);
		XZPoint<T> point2 = new XZPoint<T>(lastPoint);
		point2.setTimeMs(i);
		return point2;
	}

	public XZPoint<T> getPointBefore(long i) {
		while (true) {
			try {
				XZPoint<T> result = null;
				for (Long long1 : points.keySet()) {
					if (long1 < i) {
						result = points.get(long1);
					} else {
						return result;
					}
				}
				return result;
			} catch (ConcurrentModificationException ex){
			}
	}
	
	}
	public XZPoint<T> getPointAfter(long i) {
		while (true) {
			try {
				for (Long long1 : points.keySet()) {
					if (long1 < i) {
					} else {
						return points.get(long1);
					}
				}
				return null;
			} catch (ConcurrentModificationException ex){
			}
		}
	}
	private void setPointsCollection(final int countToKeep) {
		points = Collections.synchronizedMap(new LinkedHashMap<Long, XZPoint<T>>(){
			private static final long serialVersionUID = 1L;
			protected boolean removeEldestEntry(Entry<Long, XZPoint<T>> eldest) {
				return size() > countToKeep;
			}
		});
	}

}
