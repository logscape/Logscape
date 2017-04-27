package com.liquidlabs.log.search;

import com.liquidlabs.log.space.LogRequest;

public interface SearchRunnerI {

	int search(LogRequest request);

	void removeCompleteTasks();

}
