package com.liquidlabs.log.alert.correlate;

public interface Window {

	Window eventReceived(Event event);

	Window copy();

}