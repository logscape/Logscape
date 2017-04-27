package com.logscape.disco.indexer.mapdb;


import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import com.logscape.disco.indexer.Db;
import com.logscape.disco.indexer.mapdb.MapDb;


import java.util.Collection;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DbWrapper<K, V> implements Db<K, V> {
    public static final String DB_NAME = "dbName";
    public static final int COMMIT_INTERVAL = Integer.getInteger("map.db.commit.interval",10000);

    private volatile MapDb<K, V> active;
    private volatile MapDb<K, V> inactive;
    private final String dbName;
    private final ReadWriteLock dbSwitcherooLock = new ReentrantReadWriteLock();
    private volatile boolean isCompacting = false;
    private final BlockingQueue<Runnable> removalQueue = new ArrayBlockingQueue<Runnable>(1000);
    private final EventMonitor eventMonitor;


    public DbWrapper(MapDb dbOne, MapDb dbTwo, String dbName, EventMonitor eventMonitor) {
        this.active = dbOne;
        this.inactive = dbTwo;
        this.dbName = dbName;
        this.eventMonitor = eventMonitor;
        scheduler.scheduleWithFixedDelay(new RemovalThread(removalQueue), 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public String getName() {
        return this.dbName;
    }

    private final ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");

    @Override
    public V get(final K key) {
        return runWithReadLock(new Callable<V>() {
            @Override
            public V call() throws Exception {
                return active.get(key);
            }
        });
    }

    @Override
    public void remove(final K key) {
        runWithReadLock(new Callable<V>() {
            @Override
            public V call() throws Exception {
                return active.get(key);
            }
        });
    }

    @Override
    public void put(final K key, final V value) {
        runWithReadLock(new Callable<V>() {
            @Override
            public V call() throws Exception {
                active.put(key, value);
                return null;
            }
        }) ;
    }




    @Override
    public void remove(final K fromId, final K toId) {
        runWithReadLock(new Callable<V>() {
            @Override
            public V call() throws Exception {
                if (isCompacting) {
                    queueRemoval(fromId, toId);
                }
                active.remove(fromId, toId);
                eventMonitor.raise(new Event("kvIndexRemove").with(DB_NAME, dbName).with("logId", fromId));
                active.commit();
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
    }


    public void compact() {

        eventMonitor.raise(new Event("compact_start").with(DB_NAME, dbName));
        long start = System.currentTimeMillis();

        try {
            runWithWriteLock(new Runnable() {
                @Override
                public void run() {
                    isCompacting = true;
                    theSwitcheroo();
                }
            });


            eventMonitor.raise(new Event("compact_ia_start").with(DB_NAME, dbName));
            inactive.compact();
            eventMonitor.raise(new Event("compact_ia_finished").took(System.currentTimeMillis() - start).with(DB_NAME, dbName));

            runWithWriteLock(new Runnable() {
                @Override
                public void run() {
                    theSwitcheroo();

                    int count = 0;

                    Set<K> ks = inactive.keySet();
                    for (K k : ks) {
                        V v = inactive.get(k);
                        active.put(k,v);
                        if (count++ % COMMIT_INTERVAL == 0) active.commit();
                    }
                    active.commit();

                    isCompacting = false;
                }
            });

        } finally {
            eventMonitor.raise(new Event("compact_finished").took(System.currentTimeMillis() - start).with(DB_NAME, dbName));
            inactive.clearAndCompact();
        }
    }


    @Override
    public Set<K> keySet() {
        return active.keySet();
    }

    @Override
    public Set<K> keySet(K fromId, K toId) {
        return active.keySet(fromId, toId);
    }

    @Override
    public Collection<V> values() {
        return active.values();
    }

    @Override
    public int size() {
        return active.size();
    }


    @Override
    public void commit() {
        runWithReadLock(new Callable<V>() {
            @Override
            public V call() throws Exception {
                active.commit();
                inactive.commit();
                return null;
            }
        });
    }

    @Override
    public void release() {

    }

    @Override
    public void close() {
        runWithWriteLock(new Runnable() {
            public void run()  {
                active.close();
                inactive.close();
            }
        });
    }

    @Override
    public void clear() {

    }

//    public void commitNow() {
//        active.commit();
//        inactive.commit();
//    }
//
//    public void closeNow() {
//        active.close();
//        inactive.close();
//    }

    private class RemovalThread implements Runnable {
        private final BlockingQueue<Runnable> queue;

        public RemovalThread(final BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            final Runnable peek = queue.peek();
            dbSwitcherooLock.readLock().lock();
            try {
                if (peek != null && !isCompacting) {
                    final Runnable runnable = queue.poll(1, TimeUnit.SECONDS);
                    if (runnable != null) {
                        runnable.run();
                    }
                }
            } catch (InterruptedException e) {

            } finally {
                dbSwitcherooLock.readLock().unlock();
            }
        }
    }

    private void queueRemoval(final K fromId, final K toId) {
        removalQueue.offer(new Runnable() {
            @Override
            public void run() {
                remove(fromId, toId);
            }
        });
    }


    private void runWithWriteLock(Runnable runnable) {
        dbSwitcherooLock.writeLock().lock();
        try {
            runnable.run();
        } finally {
            dbSwitcherooLock.writeLock().unlock();
        }
    }


    volatile int readFailures = 0;
    private V runWithReadLock(Callable<V> callable) {
        dbSwitcherooLock.readLock().lock();
        try {
            return callable.call();
        } catch (Exception e) {
            if (readFailures++ < 10) {
                eventMonitor.raiseWarning(new Event("kvReadFailure").with(DB_NAME, dbName), e);
            } else {
                if (readFailures < 1000) {
                    eventMonitor.raise(new Event("kvReadFailure" + readFailures).with("Exception",e.toString()));
                }
            }
            return null;
        } finally {
            dbSwitcherooLock.readLock().unlock();
        }
    }

    private void theSwitcheroo() {
        MapDb temp = active;
        active = inactive;
        inactive = temp;
    }


}
