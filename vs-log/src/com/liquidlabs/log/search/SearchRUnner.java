package com.liquidlabs.log.search;

import com.liquidlabs.log.space.LogRequest;

public interface SearchRUnner {

	int search(LogRequest request);

	void removeCompleteTasks();

}
