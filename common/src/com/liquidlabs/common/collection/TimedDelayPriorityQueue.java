package com.liquidlabs.common.collection;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Uses a timebased delay for items that may have not arrived in sequence.
 * Sequencing is provided by the client when putting items, and retrieval is based up
 * delaying the head item if an out of sequence item is found.
 */
public class TimedDelayPriorityQueue<T> {

	private long lastId = -1;
	private final long delayPeriod;
	private final TimeUnit delayUnit;
	boolean experiencedDelay = false;
	PriorityBlockingQueue<QueueItem<T>> priorityQueue = new PriorityBlockingQueue <QueueItem<T>>(256);

	public TimedDelayPriorityQueue(long delayPeriod, TimeUnit delayUnit){
		this.delayPeriod = delayPeriod;
		this.delayUnit = delayUnit;
	}
	
	public void put(long sequenceId, T item) {
		priorityQueue.add(new QueueItem<T>(sequenceId, item));
	}
	public boolean isEmpty(){
		return priorityQueue.isEmpty();
	}
	public T getLatest() throws InterruptedException{
		long releaseTime = System.currentTimeMillis() + delayPeriod;
		
		experiencedDelay = false;
		while (priorityQueue.peek().getId() != lastId+1 &&  System.currentTimeMillis() < releaseTime){
			experiencedDelay = true;
			Thread.sleep(50);
		}
		if (priorityQueue.size() > 0) {
			QueueItem<T> take = priorityQueue.take();
			lastId = take.getId();
			return take.getValue();
		}
		throw new IllegalStateException("Cannot take item from Empty Queue");
	}
	
	public static class QueueItem<T> implements Comparable<QueueItem<T>> {
		public QueueItem(Long id, T value){
			this.id = id;
			this.value = value;
		}
		Long id;
		T value;
		public Long getId() {
			return id;
		}
		public void setId(Long id) {
			this.id = id;
		}
		public T getValue() {
			return value;
		}
		public void setValue(T value) {
			this.value = value;
		}
		public int compareTo(QueueItem<T> o) {
			return id.compareTo(o.getId());
		}
	}

	public void clear() {
		this.lastId = 0;
		this.priorityQueue.clear();
	}

	public boolean isExperiencedDelay() {
		return experiencedDelay;
	}
}
