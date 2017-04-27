package com.liquidlabs.common.plot;

import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import junit.framework.TestCase;

public class ZSeriesPlotTest extends TestCase {
	
	private static final String SERIES_NAME = "someSeriesName1";
	private ZSeriesPlot<String> seriesPlot;

	@Override
	protected void setUp() throws Exception {
		seriesPlot = new ZSeriesPlot<String>();
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	public void testShouldAllowDefaultItemWhenBackProjectionDisable() throws Exception {
		seriesPlot = new ZSeriesPlot<String>(false, "emptyString");
		seriesPlot.add(SERIES_NAME, new XZPoint<String>(1000, "100"));
		
		XZPoint<String> point = seriesPlot.get(SERIES_NAME, new DateTime(100));
		assertEquals("emptyString", point.getZ());
		
	}
	
	public void testShouldStore2Series() throws Exception {
		seriesPlot.add(SERIES_NAME, new XZPoint<String>(1000, "100"));
		seriesPlot.add("someSeriesName2", new XZPoint<String>(1000, "100"));
		Set<String> names = seriesPlot.getSeriesNames();
		assertEquals(2, names.size());
	}
	
	public void testShouldStoreReceiveSeries() throws Exception {
		seriesPlot.add(SERIES_NAME, new XZPoint<String>(1000, "100"));
		seriesPlot.add(SERIES_NAME, new XZPoint<String>(2000, "100"));
		XZPoint<String> value = seriesPlot.get(SERIES_NAME, new DateTime(1500));
		assertNotNull(value);
	}
	
	
	public void testShouldGetSeriesFromTimeXUpToLatestWithInterval() throws Exception {
		seriesPlot.add("s1", new XZPoint<String>(1000, "100"));
		seriesPlot.add("s1", new XZPoint<String>(2000, "200"));
		seriesPlot.add("s1", new XZPoint<String>(3000, "300"));
		seriesPlot.add("s1", new XZPoint<String>(4000, "400"));
		seriesPlot.add("s1", new XZPoint<String>(5000, "500"));
		
		List<XZPoint<String>> results = seriesPlot.getFrom("s1", new DateTime(1000), new DateTime(5000), 1000, false, null);
		assertEquals(5, results.size());
		
		assertEquals("100", results.get(0).getZ());
		assertEquals("500", results.get(4).getZ());
	}
	
	public void testShouldGetSeriesFromTimeIgnoreDuplicates() throws Exception {
		seriesPlot.add("s1", new XZPoint<String>(1000, "100"));
		seriesPlot.add("s1", new XZPoint<String>(2000, "100"));
		seriesPlot.add("s1", new XZPoint<String>(3000, "300"));
		seriesPlot.add("s1", new XZPoint<String>(4000, "300"));
		seriesPlot.add("s1", new XZPoint<String>(5000, "500"));
		
		List<XZPoint<String>> results = seriesPlot.getFrom("s1", new DateTime(1000), new DateTime(5000), 1000, true, null);
		assertEquals(3, results.size());
		
		assertEquals("100", results.get(0).getZ());
		assertEquals("500", results.get(2).getZ());
	}
	
	public void testShouldGetSeriesFromTimeWithZProvided() throws Exception {
		seriesPlot.add("s1", new XZPoint<String>(1000, "100"));
		seriesPlot.add("s1", new XZPoint<String>(2000, "200"));
		seriesPlot.add("s1", new XZPoint<String>(3000, "300"));
		seriesPlot.add("s1", new XZPoint<String>(4000, "400"));
		seriesPlot.add("s1", new XZPoint<String>(5000, "500"));
		
		List<XZPoint<String>> results = seriesPlot.getFrom("s1", new DateTime(1000), new DateTime(5000), 1000, true, "200");
		assertEquals(3, results.size());
		
		assertEquals("300", results.get(0).getZ());
		assertEquals("500", results.get(2).getZ());
	}
	

}
