package com.liquidlabs.common;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 26/03/14
 * Time: 08:54
 * To change this template use File | Settings | File Templates.
 */
public class Pool {

    public interface Factory {
        public Object newInstance();
    }

    private final int initialPoolSize;
    private final int maxPoolSize;
    private final Factory factory;
    private transient Object[] pool;
    private transient int nextAvailable;
    private transient Object mutex = new Object();

    public Pool(int initialPoolSize, int maxPoolSize, Factory factory) {
        this.initialPoolSize = initialPoolSize;
        this.maxPoolSize = maxPoolSize;
        this.factory = factory;
    }

    public Object fetchFromPool() {
        Object result = null;
        synchronized (mutex) {
            if (pool == null) {
                pool = new Object[maxPoolSize];
                for (nextAvailable = initialPoolSize; nextAvailable > 0; ) {
                    putInPool(factory.newInstance());
                }
            }
            boolean timedOut = false;
            int waits = 0;
            while (nextAvailable == maxPoolSize && !timedOut) {
                try {
                    mutex.wait(10 * 10000);
                    if (waits++ > 10) {
                        throw new RuntimeException("Timeouts wait for resource " +
                                "for a free item in the pool : ");
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Interrupted whilst waiting " +
                            "for a free item in the pool : " + e.getMessage());
                }
            }
            if (!timedOut) {
                result = pool[nextAvailable++];
            } else {
                nextAvailable--;
            }
            if (result == null) {
                result = factory.newInstance();
                putInPool(result);
                ++nextAvailable;
            }
        }
        return result;
    }

    public void putInPool(Object object) {
        synchronized (mutex) {
            pool[--nextAvailable] = object;
            mutex.notify();
        }
    }
    public String toString() {
        return super.toString() + " :" + factory.toString() + " Used:"  + nextAvailable + " Capacity:" + maxPoolSize;
    }

    private Object readResolve() {
        mutex = new Object();
        return this;
    }
}
