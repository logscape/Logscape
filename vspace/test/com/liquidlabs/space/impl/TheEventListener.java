/**
 * 
 */
package com.liquidlabs.space.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;

public class TheEventListener implements EventListener{

	private String id;
	private CountDownLatch countDownLatch;
	int myCount;

	public TheEventListener(String id, int expectedEvents){
		this.id = id;
		countDownLatch = new CountDownLatch(expectedEvents);
	}
	@Override
	public String getId() {
		return id;
	}

	@Override
	public void notify(Event event) {
		myCount++;
		countDownLatch.countDown();
	}
	
	public boolean waitForEvents() throws InterruptedException {
		return countDownLatch.await(5, TimeUnit.SECONDS);
	}
	
}