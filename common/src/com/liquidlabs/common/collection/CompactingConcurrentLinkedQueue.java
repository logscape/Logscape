package com.liquidlabs.common.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Allows you to overcome the extremely slow memory leak of a ConcurrentLinkedQueue
 * where empty pointers remain for those items removed.
 * Note: LinkedBocking Queue does not have the leak - however periodic compactions have
 * proven that performance is 20% faster when re-allocating the queue
 */

@SuppressWarnings("unchecked")
public class CompactingConcurrentLinkedQueue<T> implements BlockingQueue {
	ReentrantLock writeLock = new ReentrantLock();
	private long takeCount;

	private LinkedBlockingQueue<T> delegate;
	private long compactionMod = 1000;
	
	public CompactingConcurrentLinkedQueue() {
	}

	public void compactNow() {
		writeLock.lock();
		try {
			this.delegate = new LinkedBlockingQueue<T>(this.delegate);
		} finally {
			writeLock.unlock();
		}
	}
	private void checkCompaction() {
		if (++takeCount % compactionMod == 0) {
			compactNow();
		}
	}

	public CompactingConcurrentLinkedQueue(long compactionMod) {
		this.compactionMod = compactionMod;
		delegate = new LinkedBlockingQueue<T>();
	}

	public boolean addAll(Collection c) {
		try {
			writeLock.lock();
			return delegate.addAll(c);
		} finally {
			writeLock.unlock();
		}
	}

	public void clear() {
		try {
			writeLock.lock();
			delegate.clear();
		} finally {
			writeLock.unlock();
		}
	}

	public boolean contains(Object obj) {
		return delegate.contains(obj);
	}

	public boolean containsAll(Collection c) {
		return delegate.containsAll(c);
	}

	public T element() {
		return delegate.element();
	}

	public boolean equals(Object obj) {
		return delegate.equals(obj);
	}

	public int hashCode() {
		return delegate.hashCode();
	}

	public boolean isEmpty() {
		return delegate.isEmpty();
	}

	public Iterator<T> iterator() {
		return delegate.iterator();
	}

	public T peek() {
		return delegate.peek();
	}

	public T poll() {
		writeLock.lock();
		try {
			checkCompaction();
			return delegate.poll();
		} finally {
			writeLock.unlock();
		}
	}

	public T remove() {
		writeLock.lock();
		try {
			checkCompaction();
			return delegate.remove();
		} finally {
			writeLock.unlock();
		}
	}

	public boolean remove(Object obj) {
		writeLock.lock();
		try {
			checkCompaction();
			return delegate.remove(obj);
		} finally {
			writeLock.unlock();
		}
	}

	public boolean removeAll(Collection c) {
		writeLock.lock();
		try {
			checkCompaction();
			return delegate.removeAll(c);
		} finally {
			writeLock.unlock();
		}
	}

	public boolean retainAll(Collection c) {
		return delegate.retainAll(c);
	}

	public int size() {
		return delegate.size();
	}

	public Object[] toArray() {
		return delegate.toArray();
	}

	public Object[] toArray(Object[] a) {
		return delegate.toArray(a);
	}

	public String toString() {
		return delegate.toString();
	}

	public boolean offer(Object o) {
		writeLock.lock();
		try {
			return delegate.offer((T) o);
		} finally {
			writeLock.unlock();
		}
	}

	public boolean add(Object o) {
		writeLock.lock();
		try {
			return delegate.add((T) o);
		} finally {
			writeLock.unlock();
		}

	}
	public Object take() throws InterruptedException {
		return delegate.take();
	}
	@Override
	public int drainTo(Collection c) {
		return delegate.drainTo(c);
	}
	@Override
	public int drainTo(Collection c, int maxElements) {
		return delegate.drainTo(c, maxElements);
	}
	@Override
	public boolean offer(Object e, long timeout, TimeUnit unit) throws InterruptedException {
		return delegate.offer((T) e, timeout, unit);
	}
	@Override
	public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
		return poll(timeout, unit);
	}
	@Override
	public void put(Object e) throws InterruptedException {
		delegate.put((T) e);
	}
	@Override
	public int remainingCapacity() {
		return delegate.remainingCapacity();
	}
	
}
