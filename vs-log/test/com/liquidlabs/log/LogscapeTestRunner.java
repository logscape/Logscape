package com.liquidlabs.log;

import org.junit.internal.runners.InitializationError;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.notification.RunNotifier;

import java.lang.reflect.Method;

public class LogscapeTestRunner extends JUnit4ClassRunner {
    public LogscapeTestRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }


    @Override
    protected Object createTest() throws Exception {
        System.out.println("createTest");
        return super.createTest();
    }

    @Override
    protected void invokeTestMethod(Method method, RunNotifier notifier) {
        super.invokeTestMethod(method, notifier);
    }
}
