package com.liquidlabs.vso.deployment;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.collection.*;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.MBeanGetter;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.agent.ResourceProfile;
import com.liquidlabs.vso.agent.ScriptExecutor;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.BundleSerializer;
import com.liquidlabs.vso.deployment.bundle.Service;
import com.liquidlabs.vso.work.WorkAssignment;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/09/2013
 * Time: 17:17
 * To change this template use File | Settings | File Templates.
 */
public class ServiceTestHarness {
    private static String token = "";
    private static String tag="";

    public static enum FIELDS  { bundleName, serviceName, output, type, token, tag };
    public static String RUNNER_KEY = "_LOGSCAPE_serviceRunner_LOGSCAPE_";
    public static boolean debug = Boolean.getBoolean("debug");
    private static String json = "";

    public static void main(String[] args) {

        writePidFile();

        VSOProperties.setProcessStartingWaitMs(1);
        try {
            json = buildDefaultJSON();

            if (args.length > 0) {
                System.out.println("Given:" + args[0]);
            } else {
                printUsage();
                System.exit(-1);

            }
            // args
            // ./myApp-1.0.bundle service.xxxx overrides.properties
            String bundleFileName = args[0];
            String serviceName = args[1];//"UNIX_CPU_wHOST_wTSTAMP";
//            bundleFileName = "/WORK/LOGSCAPE/TRUNK/logscapeApps/repo/UnixApp-1.0/UnixApp.bundle";

            Outputter out = new Stdout();

            JSONObject obj = null;
            if (args.length == 1) {
                if (args[0].endsWith(".json")) {
                    File jsonFile = new File(args[0]);
                    if (!jsonFile.exists()) {
                        System.err.println("FileNotFound:" + jsonFile.getAbsolutePath());
                    }
                    String jsonString = FileUtils.readFileToString(jsonFile);

                    obj = new JSONObject(jsonString);

                } else if (args[0].startsWith("{")) {
                    obj = new JSONObject(args[0]);

                }
                if (obj == null) {
                    System.err.println("Failed to start.");
                    printUsage();
                    System.exit(1);
                }
                bundleFileName = obj.getString(FIELDS.bundleName.name());
                serviceName = obj.getString(FIELDS.serviceName.name());
                out = new Composite( obj.getJSONArray(FIELDS.output.name()));
                try {
                    token = obj.getString(FIELDS.token.name());
                    tag = obj.getString(FIELDS.tag.name());
                } catch (Throwable t) {

                }
            }

            bundleFileName = makePathNative(bundleFileName);


            System.out.println("Bundle:" + bundleFileName);
            System.out.println("Service:" + serviceName);
            System.out.println("Token:" + token);
            System.out.println("Tag:" + tag);


            Bundle bundle = new BundleSerializer(new File("")).loadBundle(bundleFileName);

            Properties bundleOverrides = new Properties();


            File bundleFile = new File(bundleFileName);

            final String deployBundlesDir = bundleFile.getParentFile().getAbsoluteFile().getParent();
            if (! bundleFile.getParent().contains("App-")) {
                System.err.println("The app needs to be deployed in a Directory using its bundle name. i.e. WindowsApp-1.0");
                System.exit(1);

            }

            String name = bundleFile.getName();
            name = name.substring(0, name.indexOf(".")) + "-override.properties";
            File overrideFile = new File(bundleFile.getParentFile().getParent(), name);
            System.out.println("Checking for Overrides: "+ overrideFile.getAbsolutePath());

            if (overrideFile.exists()) {
                System.out.println("Loading Overrides:");
                bundleOverrides.load(new FileInputStream(overrideFile));

                for (Service aService : bundle.getServices()) {
                    aService.overrideWith(bundleOverrides);
                }
            } else {
                System.out.println("No override loaded");
            }


            List<Service> servicesToRun = new ArrayList<Service>();
            if (serviceName.equals("*")) {
                servicesToRun = bundle.getServices();
            } else {
                Service service = bundle.getService(serviceName);
                if (service == null) {
                    System.err.println("Service not found:" + serviceName);
                    System.err.println("All Services:" + bundle.getServices());
                    System.exit(1);
                }
                servicesToRun.add(service);
            }
            for (final Service service : servicesToRun) {
                runService(service,  deployBundlesDir, out);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private static void writePidFile() {
        new File("./pids").mkdir();
        final File pidFile = new File("./pids/" + new MBeanGetter().getPid() + ".pid");
        try {
            FileOutputStream fos = new FileOutputStream(pidFile);
            String json = "";
                try {
                    JSONObject js = new JSONObject();
                    js.put("bundle", "serviceRunner");
                    js.put("service", "serviceRunner");
                    js.put("date", DateUtil.shortDateFormat.print(System.currentTimeMillis()));
                    js.put("timestamp", DateUtil.shortDateTimeFormat2.print(System.currentTimeMillis()));
                    js.put("cmd:", ServiceTestHarness.class.toString().toString());
                    json =  js.toString();
                } catch (Throwable t) {
                }

            fos.write(json.getBytes());
            fos.close();
        } catch (Throwable t) {

        }
        pidFile.deleteOnExit();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                pidFile.delete();
            }
        });
    }

    private static void printUsage() throws JSONException {
        System.out.println("Usage (stdout):" + json.replace("\"","'"));
        System.out.println("Usage (network):" + buildNetworkJson());
    }
    public static String makePathNative(String path) {
        // nix
        if (File.separator.equals("/")) {
            if (path.contains("\\")) path = path.replaceAll("\\\\", File.separator);
        } else {
            if (path.contains("/")) {
                path = path.replaceAll("\\/", "\\\\");
                path = path.replaceAll("\\\\\\\\", "\\\\");
                if (path.startsWith("\\") && path.contains(":")) path = path.substring(1);
            }
            if (path.contains("\\\\")) path = StringUtils.replace(path, "\\\\", File.separator);
        }
        return path;
    }

    private static String buildNetworkJson() throws JSONException {
        JSONObject js = new JSONObject();
        js.put(FIELDS.bundleName.name(), "WindowsApp-1.0\\WindowsApp.bundle");
        js.put(FIELDS.serviceName.name(), "*");
        js.put(FIELDS.token.name(), "123:456:789");

        List<JSONObject> outputs = new ArrayList<JSONObject>();

        JSONObject sockOut = new JSONObject();
        sockOut.put(FIELDS.type.name(), SocketOut.class.getSimpleName());
        sockOut.put("remoteAddr", "10.28.1.160");
        sockOut.put("port", "9991");

        outputs.add(sockOut);
        js.put(FIELDS.output.name(), new JSONArray(outputs));
        return js.toString().replace("\"","'");
    }

    private static String buildDefaultJSON() throws JSONException {
        JSONObject js = new JSONObject();
        js.put(FIELDS.bundleName.name(), "WindowsApp-1.0\\WindowsApp.bundle");
        js.put(FIELDS.serviceName.name(), "*");
        js.put(FIELDS.token.name(), "123:456:789");
//        js.put(FIELDS.tag.name(), "MyApp");


        List<JSONObject> outputs = new ArrayList<JSONObject>();

        JSONObject stdout = new JSONObject();
        stdout.put(FIELDS.type.name(), Stdout.class.getSimpleName());

        outputs.add(stdout);
        js.put(FIELDS.output.name(), new JSONArray(outputs));
        return js.toString();
    }

    private static void runService(final Service service, String deployBundlesDir, final Outputter out) throws Exception {

        WorkAssignment work = WorkAssignment.fromService(service, "0");
        HashMap<String, Object> vars = new HashMap<String, Object>();

        ScriptExecutor scriptExecutor = new ScriptExecutor(null, new ResourceAgentStub(), deployBundlesDir, Executors.newScheduledThreadPool(5));
        scriptExecutor.dumpToStdOut = debug;
        scriptExecutor.outputListener = new ScriptExecutor.OutEvents() {
            @Override
            public void print(String serviceA, String output) {
                out.print(service.getFullBundleName(), service.getName(), output);
            }

            @Override
            public void flush() {
                out.flush();
            }
        };
        Future<?> execute = scriptExecutor.execute(work, vars, work.getId());
    }


    public static class ResourceAgentStub implements ResourceAgent {

        @Override
        public void successfullyDeployed(String bundleName, String hash) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void errorDeploying(String bundleName, String hash, String errorMessage) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void unDeployed(String bundleName, String hash) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getId() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void start(WorkAssignment workAssignment) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void update(WorkAssignment workAssignment) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void stop(WorkAssignment workAssignment) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void addDeployedBundle(String bundleNameAndVersion, String releaseDate) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void removeBundle(String bundleName) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void bounce(boolean shouldSleep) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void systemReboot(long seedTime) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public List<ResourceProfile> getProfiles() {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void go() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void updateStatus(String workId, State status, String msg) {
            if (debug) System.out.println("Update Status:" + workId + " State:" + status + " msg:" + msg);
        }

        @Override
        public void editResourceProperties(String type, String location, String maxHeap) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void start() {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void stop() {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }
    public static class SocketOut extends Stdout {
        private String remoteAddr;
        private int port;
        private Socket socket;
        private BufferedOutputStream bos;

        public SocketOut(JSONObject config) throws JSONException {

            this.remoteAddr = config.getString("remoteAddr");
            this.port = config.getInt("port");

            bind(remoteAddr, port);
            System.out.println("Connecting:" + remoteAddr + ":" + port);
        }

        private void bind(String remoteAddr, int port)  {

            try {
                this.socket = new Socket(remoteAddr, port);
                this.bos = new BufferedOutputStream(this.socket.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void print(String bundleId, String service, String message) {
            if (socket == null || !socket.isBound() || !socket.isConnected()) {
                bind(remoteAddr, port);
            }

            try {
                this.bos.write(getMessage(bundleId, service, message).getBytes());
            } catch (Throwable t) {
                t.printStackTrace();
                // return logic in here...
            }
        }

        @Override
        public void flush() {
            try {
                this.bos.flush();
            } catch (Throwable t) {
                try {
                socket.close();
                } catch (Throwable t2) {
                    t2.printStackTrace();
                }
                bind(remoteAddr, port);
            }
        }

        // Lifted from Meter.class
        public static String LS = "LOGSCAPETOKEN";
        public static String TAG = "LOGSCAPETAG";


        private String getMessage(String bundleId, String service, String message) {
            try {
                JSONObject result = new JSONObject();
                result.put("type", RUNNER_KEY);
                result.put("host", hostname);
                result.put("ipAddress", ip);
                result.put("service", service);
                result.put("destFile",  DateUtil.shortDateFormat.print(System.currentTimeMillis())  + "/" + bundleId + "/" + DateUtil.shortDateFormat.print(System.currentTimeMillis()) + "/" + service + ".out");
                result.put("bundle", bundleId);
                result.put("message", message);
                result.put(FIELDS.tag.name(), TAG + ":" + tag);
                result.put(FIELDS.token.name(), LS + ":" + token);
                return result.toString() + "\n";
            } catch (JSONException e) {
                e.printStackTrace();
                return "";
            }
        }
    }

    public static class Composite implements Outputter {
        private List<Outputter> outs = new ArrayList<Outputter>();

        public Composite(JSONArray out)  throws JSONException {
            int length = out.length();
            for (int i = 0; i < length; i++) {
                JSONObject obj = out.getJSONObject(i);
                String outType = obj.getString(FIELDS.type.name());
                if (outType.equals(Stdout.class.getSimpleName())) {
                    outs.add(new Stdout());
                } else if (outType.equals(SocketOut.class.getSimpleName())) {
                    outs.add(new SocketOut(obj));
                }
            }
        }

        @Override
        public void print(String bundleId, String service, String message) {
            for (Outputter out : outs) {
                try {
                    out.print(bundleId, service, message);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        public void flush(){
            for (Outputter out : outs) {
                try {
                    out.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }
    public static class Stdout implements Outputter {
        protected String hostname = "unknown";
        protected String ip = "0.0.0.0";

        public Stdout() {
            try {
                hostname = InetAddress.getLocalHost().getHostName();
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void print(String bundleId, String service, String message) {
            System.out.println("Host:" + hostname + "/" + ip + " Service:" + service + " Msg:" + message);
        }
        public void flush(){};
    }

    public interface Outputter {
        void print(String bundleId, String service, String message);
        void flush();
    }
}
