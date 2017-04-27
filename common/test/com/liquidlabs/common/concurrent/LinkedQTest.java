package com.liquidlabs.common.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.joda.time.DateTimeUtils;

/**
 * Concurrent Benchmarking results on my machines 1millions Items, 100 threads
 * 
 *  (ArrayBlockingQueue, ConcurrentQueue, LinkedBlockingQueue)
 *  
	Java 6
	1000000items, Elapsed:13964
	1000000items, Elapsed:1336
	1000000items, Elapsed:1165
	
	
	Java 5
	1000000items, Elapsed:15036
	1000000items, Elapsed:2197
	1000000items, Elapsed:1178

 * @author neil
 *
 */
public class LinkedQTest extends TestCase {
	int taken = 0;
	boolean finished = false;
	int threads = 10;
	final int itemsPerThread = 1000;
	ExecutorService executor = com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("worker","stuff");
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		Thread.sleep(1000);
	}
	public void testABQ() throws Exception {
		final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(1000);
		qTestq(threads, itemsPerThread, executor, new ABC(queue));
    }
	
	public void testConcurrentLQ() throws Exception {
		ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
		qTestq(threads, itemsPerThread, executor, queue);
	}
	public void testConcurrentLBQ() throws Exception {
		LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
		qTestq(threads, itemsPerThread, executor, queue);
	}
	public void testNothing() throws Exception {
		
	}
	private void qTestq(final int threads, final int itemsPerThread, ExecutorService executor, final Queue<String> queue) throws InterruptedException {
		finished = false;
		taken = 0;
		final CountDownLatch latch = new CountDownLatch(itemsPerThread * threads);
		executor.submit(new Runnable() {
			public void run() {
				try {
					while (!finished) {
						String poll;
						poll = queue.poll();
//							poll = queue.take();
						if (poll == null) continue;
						taken++;
						if (taken == itemsPerThread * threads) finished = true;
						latch.countDown();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
		long start = DateTimeUtils.currentTimeMillis();
		
		for (int i = 0; i < threads; i++) {
			executor.submit(new Runnable() {
				public void run() {
					for (int j = 0; j < itemsPerThread; j++){
//						queue.offer(Integer.toString(j));
						queue.add(Integer.toString(j));
					}
				}
			});
		}
		boolean await = latch.await(10, TimeUnit.SECONDS);
		System.err.println("Finished:" + await + " taken:" + this.taken + " " + itemsPerThread * threads + "items, Elapsed:" + (DateTimeUtils.currentTimeMillis() - start));
	}
	
	public static class ABC implements Queue<String> {
		private final ArrayBlockingQueue<String> d;

		public ABC(ArrayBlockingQueue<String> d){
			this.d = d;
			
		}

		public boolean add(String e) {
			try {
				d.put(e);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			return false;
		}

		public String element() {
			return null;
		}

		public boolean offer(String e) {
			try {
				d.put(e);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			return false;
		}

		public String peek() {
			return null;
		}

		public String poll() {
			try {
				return d.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}

		public String remove() {
			try {
				return d.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return null;
		}

		public boolean addAll(Collection<? extends String> c) {
			return false;
		}

		public void clear() {
		}

		public boolean contains(Object obj) {
			return false;
		}

		public boolean containsAll(Collection<?> c) {
			return false;
		}

		public boolean isEmpty() {
			return false;
		}

		public Iterator<String> iterator() {
			return null;
		}

		public boolean remove(Object obj) {
			return false;
		}

		public boolean removeAll(Collection<?> c) {
			return false;
		}

		public boolean retainAll(Collection<?> c) {
			return false;
		}

		public int size() {
			return 0;
		}

		public Object[] toArray() {
			return null;
		}

		public <T> T[] toArray(T[] a) {
			return null;
		}
	}

}