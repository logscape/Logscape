package com.liquidlabs.log.index;

import java.util.List;

public interface LineHandler {
	
	boolean process(List<Line> lines);

}
