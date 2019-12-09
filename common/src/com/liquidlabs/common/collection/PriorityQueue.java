package com.liquidlabs.common.collection;

import com.liquidlabs.common.DateUtil;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PriorityQueue<T> implements Queue<T> {
    public static final String PRIORITY_QUEUE_MAX_AGE_DAYS = "priority.queue.max.age.days";
    public static final long MAX_HIGH_PRIORITY_AGE = ((long)Integer.getInteger(PRIORITY_QUEUE_MAX_AGE_DAYS, 1)) * DateUtil.DAY;
    private static final int LOW_PRIORITY_BUFFER = 5;

    private BlockingQueue<T> high;
    private BlockingQueue<T> low;

    public PriorityQueue(int highPriortyQueueSize, int lowPriorityQueueSize) {
        high = new LinkedBlockingQueue<T>(highPriortyQueueSize);
        low = new LinkedBlockingQueue<T>(lowPriorityQueueSize + LOW_PRIORITY_BUFFER);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " high:" + high.size() +"/" + high.remainingCapacity() + " low:" + low.size() + "/" + low.remainingCapacity();
    }

    /* (non-Javadoc)
     * @see com.liquidlabs.common.collection.Queue#take()
     */
    public T take() {
        return take(100);
    }

    public T take(int timeout) {
        try {
            T result = high.poll(timeout, TimeUnit.MILLISECONDS);
            if (result != null) return result;
            return low.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {}
        return null;
    }

    public void clear() {
        if (high != null) high.clear();
        if (low != null) low.clear();
    }

    /* (non-Javadoc)
     * @see com.liquidlabs.common.collection.Queue#put(T, long)
     */
    public Queue<T> put(T object, long timestamp) {
        try {
            if (!isQueued(object)) {
                if (isHighPriority(timestamp)) {
                    high.put(object);
                } else {
                    // dont block on the low priority q - let it spin so we can keep collecting stuff
                    if (low.remainingCapacity() > 0) {
                        low.put(object);
                    }
                }
            }
            return low.remainingCapacity() <= LOW_PRIORITY_BUFFER ? new High<T>(this) : new Low<T>(this);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isQueued(T object) {
        return high.contains(object) || low.contains(object);
    }

    private boolean isHighPriority(long timestamp) {
        return timestamp > System.currentTimeMillis() - MAX_HIGH_PRIORITY_AGE;
    }

    /* (non-Javadoc)
     * @see com.liquidlabs.common.collection.Queue#size()
     */
    public int size() {
        return high.size() + low.size();
    }

}
