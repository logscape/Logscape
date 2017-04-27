package com.liquidlabs.log.space.agg;

import com.liquidlabs.transport.serialization.ObjectTranslator;
import junit.framework.TestCase;

public class HistogramXMLTest extends TestCase {
	
	
	
//	public void testShouldDoRegExpItemsOk() throws Exception {
//		
//		Pattern p1 = Pattern.compile("[\\.\\:\\s]+");
//		String replaceAll = p1.matcher("some    thing").replaceAll("-");
//		assertEquals("some-thing", replaceAll);
//		
//		ClientHistoItem histogramItemXML = new ClientHistoItem();
//		assertEquals("alteredcarbon_local", histogramItemXML.fixForXML("alteredcarbon+local"));
//		assertEquals("some_field", histogramItemXML.fixForXML("some field"));
//		
//	}
	public void testShouldIncrementTheValuesProperly() throws Exception {
		
		ClientHistoItem histogramItemXML = new ClientHistoItem("label", 0, 1, 0, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		
		assertEquals(5, histogramItemXML.getTotalCount());
		
	}
	public void testShoulddDiffValuesProperly() throws Exception {
		
		ClientHistoItem histogramItemXML = new ClientHistoItem("label", 0, 1, 0, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		histogramItemXML.increment("label", 2, 1, 0);
		histogramItemXML.increment("label", 4, 1, 0);
		histogramItemXML.increment("label", 3, 1, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		
		assertEquals(11, histogramItemXML.getTotalCount());
	}
	
	public void testShouldSerializeIt() throws Exception {
		ClientHistoItem histogramItemXML = new ClientHistoItem("label", 0, 1, 0, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		histogramItemXML.increment("label", 2, 1, 0);
		histogramItemXML.increment("label", 4, 1, 0);
		histogramItemXML.increment("label", 3, 1, 0);
		histogramItemXML.increment("label", 1, 1, 0);
		
		ObjectTranslator ot = new ObjectTranslator();
		String string = ot.getStringFromObject(histogramItemXML);
		ClientHistoItem objectFromFormat = ot.getObjectFromFormat(ClientHistoItem.class, string);
		assertNotNull(objectFromFormat);
		

		
	}

}
