package com.liquidlabs.common.collection;

import com.liquidlabs.common.LifeCycle;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Multipool<K, V extends LifeCycle> {

	private static final int LONG_TIMEOUT = Integer.getInteger("llabs.mpool.long.timeout", 10 * 60);
	private static final int TIMEOUT = Integer.getInteger("llabs.mpool.timeout", 120);
	private static final int SIZE_LIMIT = Integer.getInteger("llabs.mpool.size", 100);
	private static final Logger LOGGER = Logger.getLogger(Multipool.class);
	private final ScheduledFuture<?> mainFuture;

	Map<K, Pool<K, V>> map = new ConcurrentHashMap<K, Pool<K, V>>();
	private final ScheduledExecutorService scheduler;
	private CleanupListener listener;

	public Multipool(ScheduledExecutorService scheduler) {

		this.scheduler = scheduler;
		mainFuture = scheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {

				long now = System.currentTimeMillis();
				for (Pool<K, V> pool : map.values()) {
					Set<Long> itemsTimesMs = pool.poolObjects.keySet();
					for (Long itemPutTime : itemsTimesMs) {
						try {
							if (map.values().size() > 1) {
								pool.cleanup(now, itemPutTime, TIMEOUT, listener);
							} else {
								pool.cleanup(now, itemPutTime, LONG_TIMEOUT, listener);
							}

						} catch (Throwable t) {
							System.err.println("MPool Cleanup failed" + t.getMessage());
						}
					}
				}
			}
		}, 60, 60, TimeUnit.SECONDS);
	}

	synchronized public V get(K key) {
		if (!map.containsKey(key))
			return null;
		return map.get(key).take();
	}

	/**
	 * @param key
	 * @param value
	 */
	public void put(final K key, final V value) {
		
		Runnable task = new Runnable() {
			@Override
			public void run() {
            if (value == null) return;

            synchronized (map) {

                /// create a pool for this entry
                if (!map.containsKey(key)) map.put(key, new Pool<K, V>(key));


                boolean isToBeDiscardedCausePoolIsFull = map.get(key).put(value);

                if (!isToBeDiscardedCausePoolIsFull) {
                    LOGGER.info("Discarding:" + key);
                    discard(value);
                }
			}
            }
		};
    task.run();
//scheduler.execute(task);
	}

	public static class Pool<K, V extends LifeCycle> {

		Map<Long, V> poolObjects = new ConcurrentHashMap<Long, V>();
		private final K key;
        public int items = 0;

		public Pool(K key) {
			this.key = key;
		}

		private void cleanup(Long now, Long putTime, long timeout, CleanupListener listener) {
			if (now - putTime > timeout) {
				V remove = poolObjects.remove(putTime);
				try {
					if (remove != null) {
						remove.stop();
						if (listener != null) listener.stopping(remove);
					}

				} catch (Throwable t){
                } finally {
                    items = poolObjects.size();
                }

			}
		}

		synchronized public V take() {
			if (poolObjects.isEmpty()) return null;
            try {
                Long next = poolObjects.keySet().iterator().next();
                return poolObjects.remove(next);
            } catch (Throwable t) {
                return null;
            } finally {
                items = poolObjects.size();
            }
		}

		public boolean put(V value) {
			if (poolObjects.size() > SIZE_LIMIT || value == null) return false;
			long currentTimeMillis = System.currentTimeMillis();
			while (poolObjects.containsKey(currentTimeMillis)) currentTimeMillis++;
			poolObjects.put(currentTimeMillis, value);
            items = poolObjects.size();
			return true;
		}
        public Collection<V> values() {
            return poolObjects.values();
        }
	}

    public Map<K, Pool<K, V>> map(){
        return map;
    }
	public Collection<V> values() {
		ArrayList<V> results = new ArrayList<V>();
		Collection<Pool<K, V>> values = map.values();
		for (Pool<K, V> pool : values) {
			results.addAll(pool.poolObjects.values());
		}
		return results;
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[Multipool:");
		buffer.append(" map: ");
		buffer.append(map.size());
		buffer.append(" pools: ");
		Set<K> keySet = map.keySet();
		for (K k : keySet) {
			Pool<K, V> pool = map.get(k);
			buffer.append(" p:").append(k).append(" size:").append(pool.poolObjects.size());
		}
		buffer.append("]");
		return buffer.toString();
	}
	public int size() {
		int size = 0;
		for (Map.Entry<K, Pool<K, V>> kPoolEntry : map.entrySet()) {
			size +=kPoolEntry.getValue().poolObjects.size();
		}
		return size;
	}

	public void discard(final V value) {
		scheduler.execute(new Runnable() {
			public void run() {
				try {
					value.stop();
					listener.stopping(value);
				} catch (Throwable t) {
					System.out.println("MPool, discard failed:" + t.getMessage());
				}
			}
		});		
		
	}
	public void stop() {
		mainFuture.cancel(true);
	}
	public void registerListener(CleanupListener listener) {
		this.listener = listener;
	}
	public static class CleanupListener<V> {
		public void stopping(final V object){
		}
	}
}
