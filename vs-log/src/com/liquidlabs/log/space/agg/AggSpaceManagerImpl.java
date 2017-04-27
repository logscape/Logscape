package com.liquidlabs.log.space.agg;

import java.util.ArrayList;
import java.util.List;

import com.liquidlabs.vso.work.WorkAllocator;
import com.liquidlabs.vso.work.WorkAssignment;

public class AggSpaceManagerImpl implements AggSpaceManager {
	
	private WorkAllocator workAllocator;

	public AggSpaceManagerImpl() {
	}
	public AggSpaceManagerImpl(WorkAllocator workAllocator){
		this.workAllocator = workAllocator;
	}

	public List<AggEngineState> addAggEngine(String criteria, String group) {
		return null;
	}

	public List<AggEngineState> bounceAggEngine(String host) {
		return null;
	}

	public List<AggEngineState> deleteAggEngine(String hostname) {
		return null;
	}

	public List<AggEngineState> loadAggEngines() {
		List<WorkAssignment> aggEngineAssignments = workAllocator.getWorkAssignmentsForQuery("serviceName contains AggSpace");
		ArrayList<AggEngineState> results = new ArrayList<AggEngineState>();
		for (WorkAssignment workAssignment : aggEngineAssignments) {
			results.add(new AggEngineState(workAssignment.getResourceId(),workAssignment.getResourceSelection(), "group"));
		}
		return results;
	}
}
