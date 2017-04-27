package com.liquidlabs.common.plot;

import junit.framework.TestCase;

public class XZPlotTimeRetrievalTest extends TestCase {
	
	private XZPlot<String> plot;

	protected void setUp() throws Exception {
		plot = new XZPlot<String>();
	}
	
	public void testShouldRetrieve1SecondBasedPointsGoingUpOnYDim() throws Exception {
		
		plot.add(new XZPoint<String>(1000, "1000"));
		plot.add(new XZPoint<String>(10000, "10000"));
		
		XZPoint<String> point = plot.getPoint(2000);
		assertEquals("1000",point.getZ());
		
	}
	public void testShouldRetrieve1SecondBasedPointsGoingDownOnYDim() throws Exception {
		
		plot.add(new XZPoint<String>(1000, "10000"));
		plot.add(new XZPoint<String>(10000, "1000"));
		
		XZPoint<String> point = plot.getPoint(3000);
		assertEquals("10000",point.getZ());
		
		
	}

}
