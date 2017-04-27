package com.liquidlabs.log.search.functions.txn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.search.functions.ValueSetter;
import com.liquidlabs.log.util.DateTimeExtractor;

/**
 * Converts the trace elements into a table like
 * 
 * Step | TraceLine Data | Host ?
 *  1   | timestamp .....
 *  1.1   | - trace 1
 *  1.2   | - trace 2
 *  2   | time stamp etc
 *  2.1   | - trace 1
 *  2.1   | - trace 2
 */
public class SynthTracer implements SynthTraceFunction {
	
	private static final Logger LOGGER = Logger.getLogger(SynthTracer.class);
	
	public void apply(Map<String, TxnTrace> trace, ValueSetter valueSetter) {
		
		int step = 1;
		DateTimeExtractor extractor = new DateTimeExtractor();
		long now = System.currentTimeMillis();
		
		Set<String> txnIds = trace.keySet();
		
		for (String txnId : txnIds) {
			TxnTrace txnTrace = trace.get(txnId);
			int line = 1;
			LOGGER.info("------------- TXID:" + txnId);
			for (String txnTraceLine : getSorted(txnTrace.traceParts.keySet())) {
				
				Date txnTime = extractor.getTime(txnTraceLine, now);
				
//				System.out.println("TX:" + txnTraceLine);
				
				int stepBase = step * 1000;
				String stepId = stepBase + "." + String.format("%03d", line);
				valueSetter.set("Detail-" + LogProperties.getFunctionSplit() + txnTraceLine.replaceAll("!", "").replaceAll(".by.", "") + ".by." + stepId, 0, true);
				line++;
				List<String> traceLines = txnTrace.traceParts.get(txnTraceLine);
				for (String traceLine : getSorted(traceLines)) {
					Date lineTime = extractor.getTime(traceLine, now);
//					System.out.println((stepBase + line) + " -- " + traceLine);
					stepId = stepBase + "." + String.format("%03d", line);
					valueSetter.set("Detail-" + LogProperties.getFunctionSplit() + "" + traceLine.replaceAll("!", "").replaceAll(".by.", "") + ".by." + stepId, 0, true);
					valueSetter.set("ElapsedMs-" + LogProperties.getFunctionSplit() + (lineTime.getTime() - txnTime.getTime()) + ".by." + stepId, 0, true);
					line++;
				}
			}
			step++;
		}
	}

	private List<String> getSorted(Collection<String> keySet) {
		final DateTimeExtractor extractor = new DateTimeExtractor();
		final long now = System.currentTimeMillis();
		ArrayList<String> results = new ArrayList<String>(keySet);
		Collections.sort(results, new Comparator<String>(){

			public int compare(String o1, String o2) {
				Date t1 = extractor.getTime(o1, now);
				Date t2 = extractor.getTime(o2, now);
				return t1.compareTo(t2);
			}
		});
		return results;
	}

}
