/**
 * 
 */
package com.liquidlabs.log.search.functions.txn;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TxnTrace {
	String txnId;
	public TxnTrace() {
	}
	public TxnTrace(String id) {
		this.txnId = id;
	}
	// map of txn items with their substring traces
	Map<String, List<String>> traceParts;
	private String lastTxnLine;
	public void addTxn(String txn) {
		this.lastTxnLine = txn;
		if (!getTraceParts().containsKey(txn)) getTraceParts().put(txn, new ArrayList<String>());
	}
	public void addTrace(String txn, String trace) {
		if (!getTraceParts().containsKey(txn)) {
			addTxn(txn);
		}
		getTraceParts().get(txn).add(trace);
	}
	public void addTrace(TxnTrace otherTrace) {
		this.getTraceParts().putAll(otherTrace.getTraceParts());
	}
	public String toString() {
		Map<String, List<String>> traceParts2 = getTraceParts();
		StringBuilder stuff = new StringBuilder();
		
		for (String line : traceParts2.keySet()) {
			stuff.append(line).append("\n");
			List<String> stepLines = traceParts2.get(line);
			for (String stepLine : stepLines) {
				stuff.append("--").append(stepLine).append("\n");
			}
		}
		return "Trace:" + txnId + "\n" + stuff;
	}
	public Map<String, List<String>> getTraceParts() {
		if (traceParts == null) traceParts = new LinkedHashMap<String, List<String>>();
		return traceParts;
		
	}
	public String lastTxnLine() {
		return lastTxnLine;
	}
}