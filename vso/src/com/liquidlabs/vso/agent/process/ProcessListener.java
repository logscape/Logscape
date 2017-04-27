package com.liquidlabs.vso.agent.process;

import com.liquidlabs.vso.work.WorkAssignment;

public interface ProcessListener {
	void processExited(WorkAssignment work, boolean isFault, int exitCode, Throwable throwable, String stderr);
}
