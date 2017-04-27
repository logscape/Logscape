package com.liquidlabs.transport.proxy;

import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.EventListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Example implementation of how to write a remote proxy object
 */
public class RemoteEventListener implements EventListener {

    private String listenerId;
    public int invocationCount = 0;
    private CountDownLatch latch;

    public RemoteEventListener() {
        this("fdfd", 0);
    }

    public RemoteEventListener(String listenerId) {
        this(listenerId, 0);
    }

    public RemoteEventListener(String listenerId, int expectedCount) {
        this.listenerId = listenerId;
        this.latch = new CountDownLatch(expectedCount);
    }

    public void notify(Event event) {
        invocationCount++;
        latch.countDown();
    }

    public String getId() {
        return listenerId;
    }

    @Override
    public int hashCode() {
        return 31 + listenerId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;

        return o instanceof RemoteEventListener && ((RemoteEventListener) o).listenerId.equals(listenerId);
    }

    public boolean await(int seconds) throws InterruptedException {
        return latch.await(seconds, TimeUnit.SECONDS);
    }
}
