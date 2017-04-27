package com.liquidlabs.transport.proxy.events;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class BlockingEventListener implements EventListener {

	private CountDownLatch countDownLatch = new CountDownLatch(1);
	public String getId() {
		return "BlockingEventListener";
	}
	public void notify(Event event) {
		countDownLatch.countDown();
	}
	public void waitUntilNotified(long timeout){
		try {
			countDownLatch.await(timeout, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
	}
}
