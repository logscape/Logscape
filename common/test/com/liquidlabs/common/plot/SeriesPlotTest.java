package com.liquidlabs.common.plot;

import java.util.Set;

import junit.framework.TestCase;

public class SeriesPlotTest extends TestCase {
	
	private SeriesPlot seriesPlot;

	@Override
	protected void setUp() throws Exception {
		seriesPlot = new SeriesPlot();
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testShouldStore2Series() throws Exception {
		seriesPlot.add("someSeriesName1", new XYPoint(1000, 100));
		seriesPlot.add("someSeriesName2", new XYPoint(1000, 100));
		Set<String> names = seriesPlot.getSeriesNames();
		assertEquals(2, names.size());
	}
	
	public void testShouldStoreReceiveSeries() throws Exception {
		seriesPlot.add("someSeriesName1", new XYPoint(1000, 100));
		seriesPlot.add("someSeriesName1", new XYPoint(2000, 100));
		XYPoint value = seriesPlot.get("someSeriesName1", 1500);
		assertNotNull(value);
	}
	
	public void testShouldIncrementValue() throws Exception {
		seriesPlot.add("someSeriesName1", new XYPoint(1000, 100));
		seriesPlot.incrementValue("someSeriesName1", 2000);
		assertEquals(101, seriesPlot.get("someSeriesName1", 2001).getY());
		
	}
	public void testShouldDecrementValue() throws Exception {
		seriesPlot.add("someSeriesName1", new XYPoint(1000, 100));
		seriesPlot.decrementValue("someSeriesName1", 2000);
		assertEquals(99, seriesPlot.get("someSeriesName1", 2001).getY());
		
	}

}
