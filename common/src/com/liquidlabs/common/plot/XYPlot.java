package com.liquidlabs.common.plot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Holds XYEvent Data and provides time consistent retrieval. will use linear interpolation
 * between known points. After the last point, it will simply extend the value
 *
 */
public class XYPlot {
	long lastTimeMs = 0;
	
	Map<Long, XYPoint> points = Collections.synchronizedMap(new LinkedHashMap<Long, XYPoint>(){
		private static final long serialVersionUID = 1L;
		protected boolean removeEldestEntry(Entry<Long, XYPoint> eldest) {
			return size() > 2048;
		}
	});

	private boolean allowBackProjectionOnFirstItem = true;
	
	public XYPlot() {
	}
	public XYPlot(boolean allowBackProjectionOnFirstItem) {
		this.allowBackProjectionOnFirstItem = allowBackProjectionOnFirstItem;
	}

	public void add(XYPoint point) {
		lastTimeMs = point.getTimeMs();
		points.put(point.getTimeMs(), point);
	}

	public int getPointCount() {
		return points.size();
	}
	
	public List<XYPoint> getFrom(long fromTimeMs, long toTimeMs, long intervalMs) {
		
		ArrayList<XYPoint> results = new ArrayList<XYPoint>();
		int pos = -1;
		for (long time = fromTimeMs; time <= toTimeMs; time+=intervalMs) {
			pos++;
			
			XYPoint point = getPoint(time);
			if (point == null) continue;
			results.add(point);
			
		}
		return results;
	}


	public XYPoint getPoint(long atTime) {
		while (true) {
			try {
				if (points.size() == 0) {
					XYPoint point = new XYPoint(atTime, 0);
					add(point);
					return point;
				}
				// direct hit
				if (points.containsKey(atTime)) return points.get(atTime);
				
				// before
				if (points.keySet().iterator().next() > atTime) {
					if (!allowBackProjectionOnFirstItem) return null;
					return getFirst(atTime);
				}
				
				// after
				if (atTime > lastTimeMs) {
					return getLast(atTime);
				}
				
				// between
				XYPoint before = getPointBefore(atTime);
				if (before == null) {
					return getFirst(atTime);
				}
				XYPoint after = getPointAfter(atTime);
				if (after == null) return getLast(atTime);
				
				long bTimeMs = before.getTimeMs();
				long aTimeMs = after.getTimeMs();
				double abTimeDelta = aTimeMs - bTimeMs;
				
				double nowTimeDelta = atTime - bTimeMs;
		
				long bY = before.getY();
				long aY = after.getY();
				double deltaY = aY - bY;
				
				long resultY = (long) ((nowTimeDelta / abTimeDelta) * deltaY + bY);
				
				return new XYPoint(atTime, resultY);
			} catch (ConcurrentModificationException ex){
			}
			
		}
	}
	public XYPoint getPoint(long atTime, long range) {
		while (true) {
			try {
				if (points.size() == 0) {
					XYPoint point = new XYPoint(atTime, 0);
					add(point);
					return point;
				}
				
				// before
				if (atTime + range/2 < points.keySet().iterator().next()) {
					if (!allowBackProjectionOnFirstItem) return null;
					return getFirst(atTime);
				}
				
				// after
				if (atTime - range/2 > lastTimeMs) {
					return getLast(atTime);
				}
				
				// get all points in range..and average it
				// 
				boolean finished = false;
				long startTime = atTime - range/2;
				long endTime = atTime + range/2;
				long moveTime = startTime;
				long yTotal = 0;
				while (!finished) {
					XYPoint pointAfter = getPointAfter(moveTime);
					if (pointAfter == null || pointAfter.getTimeMs() > endTime) {
						finished = true;
						continue;
					}
					yTotal += pointAfter.getY();
					moveTime = pointAfter.getTimeMs();
				}
				return new XYPoint(atTime, yTotal);
			} catch (ConcurrentModificationException ex){
			}
			
		}
	}

	public XYPoint getFirst(long time) {
		XYPoint next = new XYPoint(points.values().iterator().next());
		next.setTimeMs(time);
		return next;
	}

	public XYPoint getLast(long i) {
		ArrayList<XYPoint> point = new ArrayList<XYPoint>(points.values());
		XYPoint lastPoint = points.size() == 0 ? new XYPoint(i, 0) : point.get(point.size()-1);
		XYPoint point2 = new XYPoint(lastPoint);
		point2.setTimeMs(i);
		return point2;
	}

	public XYPoint getPointBefore(long i) {
		XYPoint result = null;
		for (Long long1 : points.keySet()) {
			if (long1 < i) {
				result = points.get(long1);
			} else {
				return result;
			}
		}
		return result;
	}
	public XYPoint getPointAfter(long i) {
		for (Long long1 : points.keySet()) {
			if (long1 > i) return points.get(long1);
		}
		return null;
	}

}
