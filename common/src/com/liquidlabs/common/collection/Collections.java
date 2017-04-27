package com.liquidlabs.common.collection;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Collections {
	
	
	@SuppressWarnings("unchecked")
	public static void nullSafeSort(List collection) {
		removeNulls(collection);
		java.util.Collections.sort(collection);
		
	}
	@SuppressWarnings("unchecked")
	public static void removeNulls(Collection myVal) {
		Iterator iterator = myVal.iterator();
		while (iterator.hasNext()) {
			Object current = iterator.next();
			if (current == null) iterator.remove();
		}
	}

}
