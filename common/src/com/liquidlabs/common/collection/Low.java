/**
 * 
 */
package com.liquidlabs.common.collection;

class Low<T> implements Queue<T> {

	private final PriorityQueue<T> priorityQueue;

	public Low(PriorityQueue<T> priorityQueue) {
		this.priorityQueue = priorityQueue;
	}

	@Override
	public Queue<T> put(T object, long timestamp) {
		return priorityQueue.put(object, timestamp);
	}

	@Override
	public int size() {
		return priorityQueue.size();
	}

	@Override
	public T take() {
		return priorityQueue.take();
	}
	
	@Override
	public T take(int timeout) {
		return priorityQueue.take(timeout);
	}
	@Override
	public void clear() {
		priorityQueue.clear();
	}
}