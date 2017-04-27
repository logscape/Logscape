/**
 * 
 */
package com.liquidlabs.common.collection;

class High<T> implements Queue<T>{
	private static int BUFFER = 1000;
	private final PriorityQueue<T> underlying;
	
	public High(PriorityQueue<T> underlying) {
		this.underlying = underlying;
	}

	
	public boolean accept(long timestamp) {
		return timestamp > System.currentTimeMillis()- (PriorityQueue.MAX_HIGH_PRIORITY_AGE - BUFFER);
	}

	@Override
	public Queue<T> put(T object, long timestamp) {
		if(accept(timestamp)) {
			return underlying.put(object, timestamp); 
		}
		return this;
	}

	@Override
	public int size() {
		return underlying.size();
	}

	@Override
	public T take() {
		return underlying.take();
	}
	
	@Override
	public T take(int timeout) {
		return underlying.take(timeout);
	}
	
	public void clear() {
		underlying.clear();
	}
	
}