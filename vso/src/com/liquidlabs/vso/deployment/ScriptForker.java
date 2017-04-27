package com.liquidlabs.vso.deployment;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyShell;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import groovy.lang.Script;
import org.apache.log4j.Logger;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.process.ProcessMaker;
import com.liquidlabs.vso.work.WorkAssignment;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.joda.time.DateTimeUtils;

/**
 * Runs a Groovy script i.e. downloads the bundle (if it doesnt exist) looks up
 * each service type in the lookup space and populates the environment or passes
 * the values into the SLAContainer or other process (worker)
 */
public class ScriptForker {
	private static final Logger LOGGER = Logger.getLogger(ScriptForker.class);
    private static GroovyClassLoader gcl = new GroovyClassLoader();

	public Process runForked(String script, boolean background, Map<String, Object> variables, WorkAssignment workAssignment) throws Exception {
		if (script.contains(".groovy") && !workAssignment.isSystemService()) {
			LOGGER.info("Running GroovyScriptFile:" + workAssignment.getId());

            String bundleDir = workAssignment.isSystemService() ? "system-bundles" : "deployed-bundles";
	    	final String fullBundleDir = bundleDir + "/" + workAssignment.getBundleId();

			ProcessMaker processMaker = new ProcessMaker(bundleDir + "/"  + workAssignment.getBundleId(), VSOProperties.getWorkingDir() +  "/" + workAssignment.getBundleId(), workAssignment.isBackground(), variables, false);
			String[] cmdArgs = convertVarsToCmdLine(variables);
            String[] scriptAndParams = getScriptAndParams(script);
            String[] paramsSplit = getParamsByNewOrOldConvention(scriptAndParams[1]);
			String[] args = Arrays.append(new String[] { scriptAndParams[0],  } , paramsSplit);
            args = Arrays.append(args, cmdArgs);
            boolean debuggingFlagTrue = isDebuggingFlagTrue(variables);
            dumpProcessDebug(workAssignment, args, debuggingFlagTrue);
            processMaker.java(getClassPath(fullBundleDir), ScriptForker.class.getName(), args);
            Process process = processMaker.fork();
            return process;
			
		} else if (script.contains("processMaker.java")) {
			LOGGER.info("Running processMaker.java:" + workAssignment.getId());
			ProcessMaker processMaker = installProcessMaker(background, variables);
            boolean debuggingFlagTrue = isDebuggingFlagTrue(variables);
            dumpProcessDebug(workAssignment, new String[] { variables.toString() }, debuggingFlagTrue);

            runString(script, "", variables, getClass().getClassLoader(), workAssignment.getId());
			return processMaker.fork();
		} else {
			// inline groovy script
			File tempFile = File.createTempFile("task", "groovy");
			FileOutputStream fos = new FileOutputStream(tempFile);
			fos.write(workAssignment.getScript().getBytes());
			fos.close();
			LOGGER.info("Running GroovyScript:" + workAssignment.getId() + " File:" + tempFile.getCanonicalPath());
			String bundleDir = workAssignment.isSystemService() ? "system-bundles" : "deployed-bundles";
	    	final String fullBundleDir = bundleDir + "/" + workAssignment.getBundleId();
			ProcessMaker processMaker = new ProcessMaker(bundleDir + "/"  + workAssignment.getBundleId(), VSOProperties.getWorkingDir() +  "/" + workAssignment.getBundleId(), workAssignment.isBackground(), variables, false);
			String[] cmdArgs = convertVarsToCmdLine(variables);
			String[] args = Arrays.append(new String[] { tempFile.getCanonicalPath() } , cmdArgs);

            boolean debuggingFlagTrue = isDebuggingFlagTrue(variables);
            dumpProcessDebug(workAssignment, args, debuggingFlagTrue);

            processMaker.java(getClassPath(fullBundleDir), ScriptForker.class.getName(), args);
			return processMaker.fork();
		}
	}

    private void dumpProcessDebug(WorkAssignment workAssignment, String[] args, boolean debuggingFlagTrue) {
        if (debuggingFlagTrue) {
            dumpErrMsg("Running:" + workAssignment.getId(), workAssignment);
            dumpErrMsg("CMD:" + Arrays.asList(args), workAssignment);
        }
    }

    String[] getParamsByNewOrOldConvention(String scriptAndParam) {
        if (scriptAndParam.startsWith("\"") && scriptAndParam.contains("\"") && scriptAndParam.endsWith("\"")) {
            String pp = scriptAndParam.substring(1);
            pp = pp.substring(0, pp.length()-1);
            List<String> results = new ArrayList<String>();
            String[] split = pp.split("\"\\s+\"");
            for (String s : split) {
                results.add(s);
            }
            return (String[]) results.toArray(new String[0]);

        } else {
            return scriptAndParam.split("\\s+");
        }
    }

    String[] getScriptAndParams(String script) {
        String match = ".groovy";
        int offset = script.indexOf(match);
        String scriptname = script.substring(0, offset) + match;
        if (offset + match.length() == script.length()) return new String[] { scriptname, "" };
        return new String[] { scriptname, script.substring(offset + match.length() +1) };
    }

    private String getClassPath(String bundlePath) {
		String path = bundlePath;
		File lib = new File(bundlePath, "lib");
		File[] libraries = lib.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.getName().toLowerCase().endsWith(".jar") || file.getName().toLowerCase().endsWith(".zip");
			}
		});
		if (libraries != null) {
			for (File file : libraries) {
				try {
					path += ":" + file.getCanonicalPath();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return path;
	}

	private String[] convertVarsToCmdLine(Map<String, Object> variables) {
		ArrayList<String> argss = new ArrayList<String>();
		for (String key : variables.keySet()) {
			try {
			argss.add(String.format("%s=%s", key, variables.get(key).toString()));
			} catch (Exception ex) {
				//LOGGER.warn("Failed to setVar:" + key, ex);
			}
		}
		return argss.toArray(new String[0]);
	}

    private static Map<String, Class> cachedScripts = new ConcurrentHashMap<>();
	public Object runString(String script, String params, final Map<String, Object> variables, ClassLoader classLoader, String filename_groovy) {
        Object result = null;
        String key = script.hashCode()+params.hashCode()+filename_groovy+variables.keySet().toString();
        //System.out.println("KEY:" + key);
        try{
            Class scriptClass = null;
            if(cachedScripts.containsKey(key)){
                scriptClass = cachedScripts.get(key);
            } else {
                script = addScriptImports(script);

                String tempFile = sanitiseFilename(params, filename_groovy);
                File embeddedScript = new File(tempFile);
                if(!embeddedScript.exists()){
                    embeddedScript.getParentFile().mkdirs();
                    embeddedScript.createNewFile();
                }
                BufferedWriter scriptWriter = new BufferedWriter(new FileWriter(embeddedScript));
                scriptWriter.write(script);
                scriptWriter.close();

                scriptClass = gcl.parseClass(embeddedScript);

                embeddedScript.delete();

                cachedScripts.put(key, scriptClass);
            }

            Script scriptS  = null;
            try {
                scriptS = (Script) scriptClass.newInstance();
            } catch (InstantiationException e) {
                LOGGER.error("Failed to instantiate script:"+filename_groovy);
            }

            if (scriptS != null) {
                scriptS.setBinding(new Binding((variables)));
                scriptS.setProperty("args", new String[]{params});
                result = scriptS.run();
            }

        } catch (Exception e) {
            LOGGER.error("Failed to execute script:" + e, e);
        }
		return result;
	}

    String sanitiseFilename(String params, String filename_groovy) {
        return "./work/tmp/groovyscript" + StringUtil.replaceAll(new String[]{":", "/", "\\", "-", "", " ", ".", "_",}, "", filename_groovy+params.hashCode());
    }

    private static String addScriptImports(String script) {
		script = "import com.liquidlabs.vso.*;\n" + script;
		script = "import com.liquidlabs.space.*;\n" + script;
		script = "import com.liquidlabs.transport.*;\n" + script;
		script = "import com.liquidlabs.common.*;\n" + script;
		return script;
	}


	private ProcessMaker installProcessMaker(boolean background, Map<String, Object> variables) {
		File workDir = new File(VSOProperties.getWorkingDir() + File.separator + ((Integer)variables.get("profileId")).toString());
		workDir.mkdirs();
		ProcessMaker processMaker = new ProcessMaker((String) variables.get("WorkingDirectory"), FileUtil.getPath(workDir), background, variables, false);
		variables.put("processMaker", processMaker);
		return processMaker;
	}
	
	/**
	 * call with script.groovy followed by variable bindings k1=value1 k2=value2 etc
	 * i.e. DemoApp-1.0/hello.groovy id=100 id=200
	 * - Expects the caller to configure classpath properly
	 */
	public static void main(String[] args) {
		try {
			String scriptname = args[0];
			Binding binding = new Binding();
            HashMap<String, String> bindings = new HashMap<String, String>();
            getBindingsFromArgs(args, binding, bindings);
            binding.setProperty("_bindings", bindings);
            bindGroovyArgsVariable(args, binding);
            GroovyShell shell = new GroovyShell(binding);
			Object evaluate = shell.evaluate(new File(scriptname));
			if (evaluate != null) {
				System.out.println("log:" + evaluate.toString());
			}
		} catch (Exception e) {
            LOGGER.error("Groovy ScriptRunner Failed:" + e.toString() + " Arguments:" + Arrays.toString(args));
			e.printStackTrace();
			System.err.println("Groovy ScriptRunner Exception:" + e.toString());
			LOGGER.warn("Process failed" , e);
		}
	}

    protected static void bindGroovyArgsVariable(String[] args, Binding binding) {
        if (args.length > 1) {
            binding.setVariable("args", Arrays.subArray(args, 1, args.length));
        } else {
            binding.setVariable("args", new String[0]);
        }
    }

    static void getBindingsFromArgs(String[] args, Binding binding, HashMap<String, String> bindings) {
        for (String arg : args) {
            if (arg.contains("=")) {
                String key = arg.substring(0, arg.indexOf("="));
                String value = arg.substring(key.length() +1);
                binding.setVariable(key, value);
                bindings.put(key, value);
            }
        }
    }

    private boolean isDebuggingFlagTrue(Map<String, Object> environment) {
        boolean debugging = false;
        if (environment.get("debug") != null) {
            if (environment.get("debug").toString().contains("true")) {
                debugging = true;

            }
        }
        return debugging;
    }
    private void dumpErrMsg(String msg, WorkAssignment workAssignment) {
        try {
            FileOutputStream ferr = new FileOutputStream(getErrOutFile(workAssignment));
            ferr.write((new Date() + " " + msg + "\n").getBytes());
            ferr.close();
        } catch (Throwable t) {}
    }
    private String getErrOutFile(final WorkAssignment workAssignment) {
        String result = getWorkOut(workAssignment, "err");
        return result;
    }
    private String getWorkOut(final WorkAssignment workAssignment, String extension) {
        String dateDir = DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis());
        String outdir = "work/" + workAssignment.getBundleId() + "/" + dateDir + "/";
        String result = outdir + "/"  + workAssignment.getServiceName() + "." + extension;
        new File(result).getParentFile().mkdirs();
        return result;
    }

}
