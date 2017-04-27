package com.liquidlabs.log.search.functions;

import java.io.Serializable;

public interface FunctionFactory extends Serializable {
	Function create();

	String getTag();
}
