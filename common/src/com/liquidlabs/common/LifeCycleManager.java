package com.liquidlabs.common;

import java.util.ArrayList;
import java.util.List;

public class LifeCycleManager {
	List<LifeCycle> listeners = new ArrayList<LifeCycle>();
	public void addLifeCycleListener(LifeCycle listener) {
		this.listeners.add(listener);
	}
	public void start() {
		for (LifeCycle listener : this.listeners) {
			listener.start();
		}
	}
	public void stop() {
		for (LifeCycle listener : this.listeners) {
			listener.stop();
		}
	}
}
