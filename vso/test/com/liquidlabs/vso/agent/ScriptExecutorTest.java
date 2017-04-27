package com.liquidlabs.vso.agent;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.vso.work.WorkAssignment;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ScriptExecutorTest {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
    private HashMap<String, Object> variables = new HashMap<String, Object>();

    public class MyThingThatGetsUpdated {
        public String value;
    }

    @Test
    public void shouldIdentifyBatScript() throws Exception {
        assertTrue(ScriptExecutor.isNonGroovyScript("test.bat "));

    }

    @Test
    public void shouldGetTwoMinInterval() throws Exception {
        ResourceAgent agent = mock(ResourceAgent.class);
        String outdir = "build/ScriptExecutorTest";
        ScriptExecutor executor = new ScriptExecutor(Executors.newCachedThreadPool(), agent, outdir, scheduler);
        int result = 0;
        result = executor.getStartOffsetSecs(2 * 60, 5 * 60);
        assertEquals(60, result);

        result = executor.getStartOffsetSecs(2 * 60, 3 * 60);
        assertEquals(60, result);


        int secondOfHour = 29 * 60;

        result = executor.getStartOffsetSecs(2 * 60, secondOfHour);
        assertEquals(60, result);

        result = executor.getStartOffsetSecs(5 * 60, 51 * 60);
        assertEquals(240, result);

        result = executor.getStartOffsetSecs(5 * 60, 51 * 60 + 2);
        assertEquals(238, result);


    }

    @Test
    public void shouldExecuteVBS() throws Exception {
        ResourceAgent agent = mock(ResourceAgent.class);
        String outdir = "build/ScriptExecutorTest";
        ScriptExecutor executor = new ScriptExecutor(Executors.newCachedThreadPool(), agent, outdir, scheduler);
        FileUtil.copyFile(new File("test-data/scriptExecutorTest.vbs"), new File("build/ScriptExecutorTest-1.0/scriptExecutorTest.vbs"));
        WorkAssignment workAssignment = new WorkAssignment();
        workAssignment.setServiceName("Service");
        workAssignment.setPauseSeconds(1);
        workAssignment.setScript("scriptExecutorTest.vbs");
        workAssignment.setBundleId("../build/ScriptExecutorTest-1.0");
        executor.execute(workAssignment, variables , "WORK-ID");
        System.out.println("Finished");
    }

    @Test
    public void shouldExecuteSuccessfully() throws Exception {
        HashMap<String, Object> variables = new HashMap<String, Object>();
        MyThingThatGetsUpdated thingThatGetsUpdated = new MyThingThatGetsUpdated();
        variables.put("thing", thingThatGetsUpdated);
        WorkAssignment assignment = new WorkAssignment();
        assignment.setScript("import java.io.*;\nthing.value = 'hello'");
        assignment.setSystemService(true);
        assignment.setServiceName("Service");
        assignment.setPauseSeconds(1);
        ScriptExecutor scriptExecutor = new ScriptExecutor(new MyExecutor(), mock(ResourceAgent.class), "foo", scheduler);
        scriptExecutor.execute(assignment, variables, "workId");
        assertThat(thingThatGetsUpdated.value, is("hello"));
    }


    @Test
    public void shouldUpdateErrorWhenExceptionOccured() throws Exception {
        ResourceAgent agent = mock(ResourceAgent.class);
        WorkAssignment assignment = new WorkAssignment();
        assignment.setScript("throw new RuntimeException('It Broke')");
        assignment.setSystemService(true);
        assignment.setServiceName("Service");
        assignment.setPauseSeconds(1);

        ScriptExecutor scriptExecutor = new ScriptExecutor(new MyExecutor(), agent, "foo", scheduler);
        scriptExecutor.execute(assignment, new HashMap<String, Object>(), "workId");
        verify(agent).updateStatus(anyString(), eq(LifeCycle.State.ERROR), contains("It Broke"));
    }


    @Test
    public void shouldUpdateErrorWhenExceptionHandlerCalled() throws Exception {
        ResourceAgent agent = mock(ResourceAgent.class);
        WorkAssignment assignment = new WorkAssignment();
        assignment.setScript("exceptionHandler.uncaughtException(Thread.currentThread(), new RuntimeException('Boom'))");
        assignment.setSystemService(true);
        assignment.setServiceName("Service");
        assignment.setPauseSeconds(1);

        ScriptExecutor scriptExecutor = new ScriptExecutor(new MyExecutor(), agent, "foo", scheduler);
        scriptExecutor.execute(assignment, new HashMap<String, Object>(), "workId");
        verify(agent).updateStatus(anyString(), eq(LifeCycle.State.ERROR), contains("Boom"));
    }

    class MyExecutor extends ThreadPoolExecutor {
        public MyExecutor() {
            super(1, 1, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
        }


        @Override
        public Future<?> submit(Runnable runnable) {
            runnable.run();
            return null;
        }
    }
}
