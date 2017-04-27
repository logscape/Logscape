package com.liquidlabs.common.plot;

import com.liquidlabs.common.plot.XYPlot;
import com.liquidlabs.common.plot.XYPoint;

import junit.framework.TestCase;

public class XYPlotBasicTest extends TestCase {
	
	
	private XYPlot plot;

	protected void setUp() throws Exception {
		
		plot = new XYPlot();
	}
	
	public void testShouldAddPoints() throws Exception {
		plot.add(new XYPoint(1000, 1));
		plot.add(new XYPoint(1001, 1));
		
		assertEquals(2, plot.getPointCount());
	}
	
	public void testShouldGetKnownPoint() throws Exception {
		plot.add(new XYPoint(1000, 10));
		XYPoint point = plot.getPoint(1000);
		assertNotNull(point);
		assertEquals(10, point.getY());
		assertEquals(10, point.getY());
	}
	
	public void testShouldReturnValueBefore() throws Exception {
		plot.add(new XYPoint(1000, 10));
		XYPoint point = plot.getPoint(100);
		assertNotNull(point);
		assertEquals(10, point.getY());
	}
	
	public void testShouldReturnValueAfter() throws Exception {
		plot.add(new XYPoint(1000, 10));
		XYPoint point = plot.getPoint(1100);
		assertNotNull(point);
		assertEquals(10, point.getY());
	}
	
	public void testShouldReturnValidItemForBetweenPoints() throws Exception {
		plot.add(new XYPoint(0, 0));
		plot.add(new XYPoint(100, 99));
		
		XYPoint point = plot.getPoint(50);
		assertEquals(50, point.getTimeMs());
		assertEquals(49, point.getY());		
	}
	
	public void testShouldReturnValidItemForBetweenPointsWithoutZeroYFloor() throws Exception {
		plot.add(new XYPoint(0, 1000));
		plot.add(new XYPoint(100, 1099));
		
		XYPoint point = plot.getPoint(50);
		assertEquals(50, point.getTimeMs());
		assertEquals(1049, point.getY());		
	}
	public void testShouldReturnValidItemForBetweenPointsFromSomePoint() throws Exception {
		plot.add(new XYPoint(1000, 1000));
		plot.add(new XYPoint(1100, 1099));
		
		XYPoint point = plot.getPoint(1050);
		assertEquals(1050, point.getTimeMs());
		assertEquals(1049, point.getY());		
	}
}
