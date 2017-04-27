package com.liquidlabs.log.space.agg;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.search.functions.Average;
import com.liquidlabs.log.space.LogRequest;
import junit.framework.TestCase;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HistogramBucketToXMLAssemblerTest extends TestCase {
	
	private ClientHistoAssembler assembler;
	String subscriber = "sub";
	String requestId = "sub";
	Map<String, LogRequest> runningRequests = new HashMap<String, LogRequest>();

	@Override
	protected void setUp() throws Exception {
		
		assembler = new ClientHistoAssembler();
		runningRequests.put(subscriber, new LogRequest());
	}
	
	public void testShouldCreateXMLHistoFromOther() throws Exception {
		
		long fromTime = 1000000;
		long toTime = 2000000;
		
		HistoManager histo2Assembler = new HistoManager();
		
		// setup Histo-B1
		LogRequest request = new LogRequest("sub", fromTime, toTime);
		request.setBucketCount(10);
		
		Query query = new Query(0, "pattern");
		query.addFunction(new Average("average","group", "apply"));
		request.addQuery(query);
		List<Map<String, Bucket>> histoB1 = histo2Assembler.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, "", "");
		
		Bucket b1 = histoB1.get(0).values().iterator().next();
		Average av1 = (Average) b1.functions().get(0);
		av1.updateResult("stuff", 100);
		b1.convertFuncResults(true);
		b1.increment();
		
		
		List<ClientHistoItem> result = assembler.getClientHistoFromRawHisto(request, histoB1, new HashMap(), new ArrayList<FieldSet>());
		
		assertEquals(1, result.get(0).series().size());
		assertEquals("[average!stuff]", result.get(0).series().toString());
		assertEquals(100.0, result.get(0).getSeriesValue("average!stuff").value);
	}
	
	public void testShouldCreateXMLHistoFromTwoOther() throws Exception {
		
		long fromTime = 1000000;
		long toTime = 2000000;
		
		HistoManager histo2Assembler = new HistoManager();
		
		// setup Histo-B111111111111111
		LogRequest request = new LogRequest("sub", fromTime, toTime);
		request.setBucketCount(10);
		
		Query query = new Query(0,"pattern");
		query.addFunction(new Average("average","group", "apply"));
		request.addQuery(query);
		List<Map<String, Bucket>> histoB1 = histo2Assembler.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, null, null);
		
		Bucket b1 = histoB1.get(0).values().iterator().next();
		Average av1 = (Average) b1.functions().get(0);
		av1.updateResult("stuff", 100);
		b1.convertFuncResults(true);
		b1.increment();
		
		// setup Histo-B2222222222
		List<Map<String, Bucket>> histoB2 = histo2Assembler.newHistogram(new DateTime(fromTime), new DateTime(toTime), request.getBucketCount(), request.queries(), subscriber, null, null);
		
		Bucket b2 = histoB1.get(0).values().iterator().next();
		Average av2 = (Average) b2.functions().get(0);
		av2.updateResult("stuff", 200);
		b2.convertFuncResults(true);
		b2.increment();
		
		
		HistoManager reducer = new HistoManager();
		reducer.handle(histoB1, histoB1, request);
		
		
		List<ClientHistoItem> result = assembler.getClientHistoFromRawHisto(request, histoB1, new HashMap(), new ArrayList<FieldSet>());
		
		assertEquals(1, result.get(0).series().size());
		assertEquals("[average!stuff]", result.get(0).series().toString());
		assertEquals(150.0, result.get(0).getSeriesValue("average!stuff").value);
	}
}
