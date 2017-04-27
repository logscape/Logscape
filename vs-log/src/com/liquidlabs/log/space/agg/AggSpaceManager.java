package com.liquidlabs.log.space.agg;

import java.util.List;

public interface AggSpaceManager {

	List<AggEngineState> addAggEngine(String criteria, String group);

	List<AggEngineState> bounceAggEngine(String host);

	List<AggEngineState> deleteAggEngine(String hostname);

	List<AggEngineState> loadAggEngines();

}
