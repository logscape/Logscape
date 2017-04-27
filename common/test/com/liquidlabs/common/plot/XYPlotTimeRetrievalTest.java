package com.liquidlabs.common.plot;

import com.liquidlabs.common.plot.XYPlot;
import com.liquidlabs.common.plot.XYPoint;

import junit.framework.TestCase;

public class XYPlotTimeRetrievalTest extends TestCase {
	
	private XYPlot plot;

	protected void setUp() throws Exception {
		plot = new XYPlot();
	}
	
	public void testShouldRetrieve1SecondBasedPointsGoingUpOnYDim() throws Exception {
		
		plot.add(new XYPoint(1000, 1000));
		plot.add(new XYPoint(10000, 10000));
		
		XYPoint point = plot.getPoint(2000);
		assertEquals(2000,point.getY());
		
		XYPoint point2 = plot.getPoint(5000);
		assertEquals(5000,point2.getY());
		
	}
	public void testShouldRetrieve1SecondBasedPointsGoingDownOnYDim() throws Exception {
		
		plot.add(new XYPoint(1000, 10000));
		plot.add(new XYPoint(10000, 1000));
		
		XYPoint point = plot.getPoint(3000);
		assertEquals(8000,point.getY());
		
		XYPoint point2 = plot.getPoint(6000);
		assertEquals(5000,point2.getY());
		
	}

}
