package com.liquidlabs.vso.container;

import java.util.Set;

import com.liquidlabs.transport.serialization.ObjectTranslator;

public class SLA1 {
	public enum Action { ADD, REMOVE, NOTHING };
	
	ObjectTranslator query = new ObjectTranslator();
	Action action;

	public String getCurrentTemplate() {
		return "mflops > 100 AND coreCount > 1 AND osName equals \"WINDOWS XP\" OR mflops > 100 AND osName equals \"WINDOWS XP\"";
	}

	public int getResourceRequestCount(Set<String> allocatedResources) {
		return 1;
	}

	public Action getAction(Set<String> allocatedResources) {
		// evaluation
		if (allocatedResources.size() < 5) this.action = Action.ADD;
		else if (allocatedResources.size() > 10) this.action = Action.REMOVE;
		else this.action = Action.NOTHING;
		System.out.println(getClass().getName() + " Action:" + this.action);
		
		return this.action;
	}
}
