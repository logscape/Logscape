/**
 * 
 */
package com.liquidlabs.common.collection;

interface QueueAcceptor<T> {
	boolean accept(long timestamp);
}