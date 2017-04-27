package com.liquidlabs.common.collection;

public interface Queue<T> {
	
	public T take(int timeout);
	
	public T take();

	public Queue<T> put(T object, long timestamp);

	public int size();

	public void clear();

}