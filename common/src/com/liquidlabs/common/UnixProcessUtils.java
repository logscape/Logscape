package com.liquidlabs.common;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;

public abstract class UnixProcessUtils {

	final static Logger LOGGER = Logger.getLogger(UnixProcessUtils.class);
	
    public static String SIGNAL_TERM = "TERM";
    public static String SIGNAL_INT = "INT";
    public static String SIGNAL_HUP = "HUP";
    public static String SIGNAL_KILL = "KILL";
    public static String SIGNAL_QUIT = "QUIT";

    private static final String UNIX_PROCESS_CLASS_NAME = "java.lang.UNIXProcess";

    public static boolean isUnixProcess(Process process) {
        return UNIX_PROCESS_CLASS_NAME.equals(process.getClass().getName());
    }
    public static String getPSInfo(int pid) {
        String cmd = "ps -fp " +  pid;
        
        if (isSolaris()) {
            cmd = "pargs " + pid;
        }
        // dont use lsof - its a killer
        //String cmd = "lsof -p " +  pid;
        Process proc = null;
        try {
            proc = Runtime.getRuntime().exec(cmd);
            InputStream inputStream = proc.getInputStream();
            InputStream errStream = proc.getErrorStream();
            BufferedReader bufferedInReader = new BufferedReader(new InputStreamReader(inputStream));
            BufferedReader bufferedErrReader = new BufferedReader(new InputStreamReader(errStream));
            proc.waitFor();

            String err = bufferedErrReader.readLine();
            StringBuilder sb = new StringBuilder();
            String s = "";
            while ( (s = bufferedInReader.readLine()) != null) {
                sb.append(s).append("\n");
            }
            if (err != null && err.length() > 0) {
                LOGGER.warn("PS/LS Error:" + err);
            }

            return sb.toString();

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (InterruptedException e) {
        }
        return "";
        //LOGGER.info(String.format("%s exit:%d out:%s err:%s", pid, proc.exitValue(), bufferedInReader.readLine(), bufferedErrReader.readLine()));

    }

    public static int getUnixPid(Process process) {
        if (!isUnixProcess(process)) {
           return -1;
        }
        try {
            return (Integer) getPrivateField(process, "pid");
        } catch (Exception exception) {
            return -1;
        }
    }

    public static void killUnixProcess(Process process, String signal) {
        int pid = getUnixPid(process);
        try {
            new ProcessBuilder("kill", String.format("-s %s",signal), Integer.toString(pid)).start();
        } catch (IOException ioException) {
        	LOGGER.warn("Kill error:" + ioException + " pid:" + pid, ioException);
        }
    }
    
    public static void killTree(Process process) {
    	killAll(getUnixPid(process), "-9");
    }
    
    public static void killTree(int pid) {
    	killAll(pid, "-9");
    }

    static String unixScript = new java.io.File("scripts/unix_kill.sh").exists() ? "scripts/unix_kill.sh" : "unix_kill.sh";
    
    private static void killAll(int pid, String sig) {
    	try {

            String command = String.format("%s %s %d", unixScript, sig, pid);
            LOGGER.info(String.format("Killing %s with %s Command:%s", pid, sig, command));

            Process proc = Runtime.getRuntime().exec(command);
			InputStream inputStream = proc.getInputStream();
			InputStream errStream = proc.getErrorStream();
            BufferedReader bufferedInReader = new BufferedReader(new InputStreamReader(inputStream));
            BufferedReader bufferedErrReader = new BufferedReader(new InputStreamReader(errStream));
            proc.waitFor();
            LOGGER.info(String.format("%s exit:%d cmd:%s out:%s err:%s", pid, proc.exitValue(), command, bufferedInReader.readLine(), bufferedErrReader.readLine()));

		} catch (Exception e) {
        	LOGGER.warn("KillAll error:" + e + " pid:" + pid);
		}
    }
    public static void main(String[] args) {
    	try {
            String psInfo = UnixProcessUtils.getPSInfo(19442);
            System.out.println(psInfo);
            //UnixProcessUtils.killAll(Integer.parseInt(args[0]), "-TERM");
    	} catch (Throwable t) {
    		LOGGER.warn(t);
    	}
	}
    
    public static void quitTree(int pid) {
    	killAll(pid, "-TERM");
    }
    public static void quitTree(Process process) {
    	killAll(getUnixPid(process), "-TERM");
    }

    public static Object getPrivateField(Object instance, String fieldName) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        return getPrivateField(instance.getClass(), instance, fieldName);
    }

    public static Object getPrivateField(Class<?> type, Object instance, String fieldName) throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Field field = type.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    public static boolean isSolaris() {
            return System.getProperty("os.name").toUpperCase().contains("SUN");
    }
}
