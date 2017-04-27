package com.liquidlabs.common.collection.cache;

public interface SimpleAction<T> {

	T execute() throws InterruptedException;

}
