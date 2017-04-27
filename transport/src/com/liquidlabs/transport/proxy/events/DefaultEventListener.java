package com.liquidlabs.transport.proxy.events;

import java.util.ArrayList;
import java.util.List;

public class DefaultEventListener implements EventListener {
	List<Event> events = new ArrayList<Event>();

	public String getId() {
		return "DefaultEventListener";
	}
	public void notify(Event event) {
		events.add(event);
	}

	public List<Event> getEvents() {
		return events;
	}

	public void setEvents(List<Event> events) {
		this.events = events;
	}
}
