package com.liquidlabs.log.search;

import java.util.concurrent.locks.ReadWriteLock;

public interface SearchLockManager {
	
	void unlock(ReadWriteLock lock);
	ReadWriteLock acquireReadLock(String indexed);

}
