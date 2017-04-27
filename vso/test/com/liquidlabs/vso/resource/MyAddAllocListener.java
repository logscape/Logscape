package com.liquidlabs.vso.resource;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MyAddAllocListener implements AllocListener {
    private CountDownLatch latch;

    public MyAddAllocListener(CountDownLatch latch) {
        this.latch = latch;
    }

    public void add(String requestId, List<String> resourceIds, String owner, int priority) {
        latch.countDown();
    }

    public void take(String requestId, String owner, List<String> resourceIds) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public List<String> release(String requestId, List<String> resourceIds, int releaseCount) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void pending(String requestId, List<String> resourceIds, String owner, int priority) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void satisfied(String requestId, String owner, List<String> resourceIds) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}
}
