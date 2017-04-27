package com.liquidlabs.common.plot;

import junit.framework.TestCase;

public class SeriesPlotFindHitsTest extends TestCase {

	
	private XYPlot plot;
	String series = "series";

	@Override
	protected void setUp() throws Exception {
		plot = new XYPlot();
	}
	
	
	public void testShouldFindSmallPoint() throws Exception {
		
		addValue(0, 0);
		addValue(0, 1000);
		addValue(0, 2000);
		addValue(0, 3000);
		addValue(100, 10000);
		addValue(0, 10000 + 3000);

		addValue(0, 15000);
		addValue(0, 16000);
		addValue(0, 17000);
		addValue(0, 18000);
		
		XYPoint point = plot.getPoint(3000, 18000);
		
		assertEquals(100, point.getY());
		
	}
	
	public void addValue(int value, int time) {
		XYPoint last = plot.getLast(time);
		last.setY(value);
		plot.add(last);
	}

	
}
