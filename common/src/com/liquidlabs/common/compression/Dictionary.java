package com.liquidlabs.common.compression;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public interface Dictionary {

	Collection<? extends String> keySet();

	int size();

	void increment(String word);

	boolean containsKey(String tabKey);

	int getCount(String dictItemOnLine);

	void put(String word, AtomicLong count);

	String getCompressorType();

}
