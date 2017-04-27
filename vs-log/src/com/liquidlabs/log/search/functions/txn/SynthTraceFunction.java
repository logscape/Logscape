package com.liquidlabs.log.search.functions.txn;

import java.util.Map;

import com.liquidlabs.log.search.functions.ValueSetter;

public interface SynthTraceFunction {

	void apply(Map<String, TxnTrace> trace, ValueSetter valueSetter);

}
