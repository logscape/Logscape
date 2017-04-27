package com.liquidlabs.vso.agent;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.vso.work.WorkAssignment;
import org.apache.log4j.Logger;

public class StatusUpdatingExceptionHandler implements Thread.UncaughtExceptionHandler {
    private ResourceAgent agent;
    private WorkAssignment workAssignment;
    private static final Logger LOGGER  = Logger.getLogger(StatusUpdatingExceptionHandler.class);

    public StatusUpdatingExceptionHandler(ResourceAgent agent, WorkAssignment workAssignment) {
        this.agent = agent;
        this.workAssignment = workAssignment;
    }

    public void uncaughtException(Thread thread, Throwable throwable) {
        LOGGER.error(String.format("UNCAUGHT EXCPEPTION in thread[%s] ex:%s", thread.getName(), throwable.getMessage(), throwable));
        agent.updateStatus(workAssignment.getId(), LifeCycle.State.ERROR, createErrorMessage(throwable));
    }

    private String createErrorMessage(Throwable throwable) {
        return throwable.toString() + "\n" + ExceptionUtil.stringFromStack(throwable, 4096);
    }
}
