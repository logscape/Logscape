package com.liquidlabs.vso.deployment;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import groovy.lang.Binding;
import org.junit.Test;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.EmbeddedServiceManager;
import com.liquidlabs.vso.agent.process.ProcessHandler;
import com.liquidlabs.vso.agent.process.ProcessListener;
import com.liquidlabs.vso.work.WorkAssignment;

public class ScriptForkerTest {

    @Test
    public void shouldBindArgsProperly() throws Exception {


        Binding binding = new Binding();

        ScriptForker.bindGroovyArgsVariable(new String[] { "a.groovy", "param1"}, binding);
        assertEquals(1, ((String[]) binding.getVariable("args")).length);

        binding = new Binding();

        ScriptForker.bindGroovyArgsVariable(new String[] { "a.groovy"}, binding);
        assertEquals(0, ((String[]) binding.getVariable("args")).length);
    }



        @Test
    public void shouldQuotedParams() throws Exception {


        String scriptArgs1 = "\"first\"";
        String[] params1 = new ScriptForker().getParamsByNewOrOldConvention(scriptArgs1);
        assertEquals(1, params1.length);
        assertEquals("first", params1[0]);

        String scriptArgs2 = "\"first\"   \"second\"";
        String[] params2 = new ScriptForker().getParamsByNewOrOldConvention(scriptArgs2);
        assertEquals(2, params2.length);
        assertEquals("first", params2[0]);
        assertEquals("second", params2[1]);


        String scriptArgs = "\"first\" \"second\"    \"third\"";
        String[] params = new ScriptForker().getParamsByNewOrOldConvention(scriptArgs);
        assertEquals(3, params.length);
        assertEquals("first", params[0]);
        assertEquals("second", params[1]);
        assertEquals("third", params[2]);
    }


    @Test
    public void shouldGetBindingsFromArgs() throws Exception {

        String scriptArgs = "1=one 2=two 3=three";
        Binding binding = new Binding();
        HashMap<String, String> bindings = new HashMap<String, String>();
        ScriptForker.getBindingsFromArgs(scriptArgs.split(" "), binding, bindings);
        assertEquals("one",bindings.get("1"));

    }


        @Test
	public void shouldSplitScriptAndParams() throws Exception {
        ScriptForker runner = new ScriptForker();
        String[] result = runner.getScriptAndParams("myGroovy.groovy bundleId=some user=me");
        assertEquals("myGroovy.groovy", result[0]);
        assertEquals("bundleId=some user=me", result[1]);

        result = runner.getScriptAndParams("myGroovy.groovy");
        assertEquals("myGroovy.groovy", result[0]);
        assertEquals("", result[1]);
    }
	
//	@Test
	public void shouldManageProcess() throws Exception {
		VSOProperties.setWorkingDir("deployed-bundles");
		String canonicalPath = new File(VSOProperties.getWorkingDir()).getCanonicalPath();
		System.out.println("Path:" + canonicalPath);
		
		ScriptForker runner = new ScriptForker();
		Map<String, Object> vars = new HashMap<String, Object>();
		String script = "forever.groovy";
		WorkAssignment wa = new WorkAssignment("Agent","",0,"DemoApp-1.0","ScriptRunnerTEST", script, 10);
		wa.setScript(script);
		
		final Process process = runner.runForked(script, false, vars, wa);
		
		ProcessHandler proc = new ProcessHandler();
		proc.addListener(new ProcessListener(){
			public void processExited(WorkAssignment work, boolean isFault,
					int exitCode, Throwable throwable, String stderr) {
				System.out.println("exited:" + work);
				
			}
		});
		int oid = proc.manage(wa, process);
		
		Thread.sleep(999 * 1000);
		System.out.println("here::::::::;");
		
		
	}
	
	@Test
	public void runScript() throws Exception {
		
		
		VSOProperties.setWorkingDir("deployed-bundles");
		final Map<String, Object> vars = new HashMap<String, Object>();
		String canonicalPath = new File(VSOProperties.getWorkingDir()).getCanonicalPath();
		System.out.println("Path:" + canonicalPath);
		
		EmbeddedServiceManager esm = new EmbeddedServiceManager("");
		vars.put("serviceManager", esm);	
		vars.put("pout", System.out);
		vars.put("perr", System.err);
		
		final ScriptForker runner = new ScriptForker();
		final String scriptName = "simpleLifeCycle.groovy";
		WorkAssignment wa = new WorkAssignment("Agent","",0,"MicrosoftHPCApp-1.0","TEST_SCRIPT", scriptName, 10);
		wa.setScript(scriptName);
		
		Runnable run = new Runnable() {
			@Override
			public void run() {
				try {
				System.out.println("Running task:" + new File(".").getAbsolutePath());
				Object runString = runner.runString(FileUtil.readAsString("test-data/"+scriptName), "param1 param2", vars, this.getClass().getClassLoader(),  "ddd"+ scriptName);
				System.out.println("Got:" + runString);
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
		};
		Future<?> submit = Executors.newCachedThreadPool().submit(run);
		esm.registerFuture(submit);
		submit.get(10, TimeUnit.SECONDS);
		System.out.println("stopping-----------------------");
		esm.stop();
		Thread.sleep(1000);
		
		
	}

	void printOutput(String type, InputStream err) throws IOException {
		byte [] buf = new byte[err.available()];
		int read = err.read(buf, 0, buf.length);
		String data = new String(buf, 0, read);
		if (data.length() > 0) System.out.println(type + " Data:" + data);
	}
}
