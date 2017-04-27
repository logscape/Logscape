package com.logscape;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.PrintUrlBuilder;
import com.liquidlabs.services.ServicesLookup;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 03/07/2013
 * Time: 11:01
 * http://localhost:8080/print/?Workspace=Home&user=sysadmin
 */
public class PrintServlet extends HttpServlet {

    // http://www.a4papersize.org/a4-paper-size-in-pixels.php
    // A4 - 72 PPI	595 Pixels	842 Pixels
    // A4 - 400 PPI	3307 Pixels	4677 Pixels
    private static String pageConfig = System.getProperty("PageSize","A4");
    private static double[] A4_WH =  new double[] { 595.0, 842.0 };
    private static double[] LET_WH =  new double[] { 612.0, 792.0 };



    private static double F400_DPI = Double.parseDouble(System.getProperty("print.dpi","400.0"))/72.0;

    public static final int WIDTH = (int) (F400_DPI * (pageConfig.equals("A4") ? A4_WH[0]  : LET_WH[0]));
    public static final int HEIGHT = (int) (F400_DPI * (pageConfig.equals("A4") ? A4_WH[1] : LET_WH[1]));


    Logger auditLogger = Logger.getLogger("AuditLogger");

    String exeTEMP = "phantomjs{EXT}";
    String webPhantomRoot = System.getProperty("phantom.root", "work/jetty-0.0.0.0-" + LogProperties.getWebSslPort() + "-print.war-_print-any-/webapp/");
    String version = System.getProperty("phantom.version","1.9.1");
    String procTaskTEMP = webPhantomRoot + "phantom/phantomjs-" + version + "-{PLAF}/bin/{EXE}";

    String workTemp = LogProperties.getPrintQ(LogProperties.getWebSslPort());

    Logger logger = Logger.getLogger(PrintServlet.class);
    int port = Integer.getInteger("web.app.port", 8080);

    ThreadPoolExecutor printingTasks = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.getInteger("printserver.concurrent", 2), new NamingThreadFactory("PrintServer"));

    Map<String, Future> tasks = new ConcurrentHashMap<String, Future>();



    public PrintServlet() {
        ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD);
    }


    /**
     * Call with
     * print??Workspace=Home&user=sysadmin
     * it will return the png file
     *
     * @param req
     * @param resp
     * @throws ServletException
     * @throws IOException
     */
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        File workDir = new File(workTemp);
        if(!workDir.exists()){
            workDir.mkdirs();
        }
        String reportName = req.getParameter("name");
        String orientation = req.getParameter("orientation");
        if (orientation == null) orientation = "Portrait";

        if (reportName == null) {
            logger.error("Parameter:name was not found in:" + req.getParameterMap());
            resp.getWriter().write("Parameter:name=XXX not specified");
            return;
        }



        String taskUID = req.getParameter("user") + "-" + reportName;
        String id = req.getParameter("clientId");

        int width = WIDTH;
        int height = HEIGHT;
        if (orientation.toUpperCase().equals("LANDSCAPE")) {
            width = HEIGHT;
            height = WIDTH;
        }

        if (id == null || id.length() == 0) id = System.currentTimeMillis() + "";
        // make the id - filesystem safe
        if (id.contains(":")) {
            id.replaceAll(":", "-");
            id.replaceAll("\\s+", "-");
            id.replaceAll("\\\\", "-");
            id.replaceAll("/", "-");
        }
        String format = req.getParameter("format");

        // PNG,CSV,PDF
        if (format == null || format.length() == 0) {
            format = "png";
        }

        ServletContext ctx = getServletContext();
        if (req.getParameter("user") == null || taskUID.contains("undefined")) {
            PrintWriter writer = resp.getWriter();
            writer.write("Print Failed, Invalid taskUID:" + taskUID);
            writer.flush();
            return;
        }

        PrintUrlBuilder urlBuilder = new PrintUrlBuilder(ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD).getLogSpace());
        urlBuilder.withName(reportName).withParam("user", req.getParameter("user")).withParam("client", "printServer");
        urlBuilder.withParam("clientId", id).withParam("format", format);

        boolean timeAdded = false;
        if (req.getParameter("from") != null) {
            timeAdded = true;
            urlBuilder.withParam("from", req.getParameter("from"));
        }
        if (req.getParameter("to") != null) {
            timeAdded = true;
            urlBuilder.withParam("to", req.getParameter("to"));
        }
        if (req.getParameter("lastMins") != null) {
            timeAdded = true;
            urlBuilder.withParam("lastMins", req.getParameter("lastMins"));
        }
        if (!timeAdded) urlBuilder.withParam("lastMins", 10);

        String url = urlBuilder.build();

        logger.info("Printing:" + url + " UID:" + taskUID  + " ClientId:" + id);

        auditLogger.info("event:PRINT clientIp:" + req.getRemoteAddr() + " url:" + url + " UID:" + taskUID);
        try {
            //http://localhost:8080/print/?Workspace=Home&user=sysadmin
            //http://localhost:8888/play/?Search=Demo&user=sysadmin&client=printServer&format=csv&clientId=MyTest#

            //print/?Search=Demo&user=sysadmin&client=printServer&format=csv&clientId=MyTest#
            //http://localhost:8888/play/?Name=Demo&user=sysadmin&client=printServer&format=csv&clientId=MyTest#

            final String filename = workTemp + "/" + getDate() + "/" + System.currentTimeMillis() + "-" + reportName;
            String outfile = filename + ".png";

            logger.info("OutFile:" + filename + " Size:" + width + " x " + height);

            runPhantom(taskUID, webPhantomRoot + "template.js", url, filename + ".png", width+"", height+"",  F400_DPI+"");
            if (format.equals("pdf")) {
                runPhantom(taskUID, webPhantomRoot + "pdf-generator.js", filename + ".png", filename + ".pdf", width+"", height+"");
                outfile = filename + ".pdf";
            }


            if (format.equals("csv")) {
                String csvFile =  workTemp + "/" + getDate() + "/" + id + ".csv";
                if (new File(csvFile).exists()) {
                    writeOutputFile(resp, ctx, csvFile);
                } else {
                    writeOutputFile(resp, ctx, "none");

                }
            } else {
                writeOutputFile(resp, ctx, outfile);
            }

            final String finalId = id;
            ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD).getProxyFactory().getScheduler().schedule(new Runnable() {
                @Override
                public void run() {
                    new File(filename + ".png").delete();
                    new File(filename + ".pdf").delete();
                    new File(workTemp + finalId + ".csv").delete();
                }
            }, 5, TimeUnit.DAYS);

        } catch (Throwable t) {
            logger.error("Failed print", t);
        }
    }


    private void writeOutputFile(HttpServletResponse resp, ServletContext ctx, String outfile) throws IOException {
        String mime = ctx.getMimeType(outfile);
        if (mime == null || !new File(outfile).exists()) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return;
        }

        resp.setContentType(mime);
        File file = new File(outfile);
        resp.setContentLength((int) file.length());

        FileInputStream in = new FileInputStream(file);
        OutputStream out = new BufferedOutputStream(resp.getOutputStream());

        // Copy the contents of the file to the output stream
        byte[] buf = new byte[4 * 1024];
        int count = 0;
        while ((count = in.read(buf)) >= 0) {
            out.write(buf, 0, count);
        }
        out.close();
        in.close();
    }

    Map<String, Process> processmap = new ConcurrentHashMap<>();
    private void runPhantom(final String uid, final String js_job, final String... args) throws IOException {

        Callable<Integer> task = new Callable<Integer>() {

            @Override
            public Integer call() throws Exception {
                // now call phantom and wait for the process to finish
                List<String> command = new ArrayList<String>();
                command.add(getPhantomTask());
                command.add(js_job);
                for (String arg : args) {
                    command.add(arg);
                }

                ProcessBuilder builder = new ProcessBuilder(command);
                final Process process;

                process = builder.start();
                processmap.put(uid, process);
                InputStream is = process.getInputStream();

                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                logger.info("START PID:" + process.toString() + " cmd:" + command);

                String line;
                while ((line = br.readLine()) != null) {
                    logger.info(line);
                }

                logger.info("FINISHED");
                return 1;
            }


        };

        /**
         * Make the tasks queue to prevent OOM
         */
        String taskUid = "name" + uid;
        if (tasks.containsKey(taskUid)) {
            Future remove = tasks.remove(taskUid);
            if (!remove.isDone() && !remove.isCancelled()) {
                logger.warn("Killing Duplicate Task UID:" + taskUid);
                remove.cancel(true);
            }

        }

        logger.info("CurrentPrintQueueSize: Tasks:" + printingTasks.getActiveCount() + " Queue:" + printingTasks.getQueue().size());
        Future<Integer> future = printingTasks.submit(task);
        tasks.put(taskUid, future);
        try {
            future.get(Integer.getInteger("max.print.wait", 10), TimeUnit.MINUTES);
        } catch (CancellationException e) {
            logger.error("Task has been cancelled UID:"+ uid);
        } catch (Exception e) {
            logger.error("UID:"+ uid + " Failed:",e);
        }
        // Try and cleanup futures and kill child processes
        tasks.remove(taskUid);
        try {
            try {
                future.cancel(true);
            } catch (Exception e) {
                //e.printStackTrace();
            }
            Process process = processmap.remove(uid);
            if (process != null) process.destroy();
        } catch (Throwable t) {
            logger.warn("Killing process:", t);
        }

    }

    public String getPhantomTask() {

        String ext = isWindows() ? ".exe" : "";
        String exe = exeTEMP.replace("{EXT}", ext);
        String plaf = getPlaf();
        String processExe = procTaskTEMP.replace("{PLAF}", plaf).replace("{EXE}", exe);
        try {
            new File(processExe).setExecutable(true);
        } catch (Throwable t) {

        }

        return processExe;

    }

    private String getPlaf() {
        if (isWindows()) return "windows";
        else if (isOSX()) return "macosx";
        else if (isLinux64()) return "linux-x86_64";
        else if (isLinux32()) return "linux-i686";
        return "windows";

    }

    public static boolean isWindows() {
        return System.getProperty("os.name").toUpperCase().contains("WINDOW");
    }

    public static boolean isOSX() {
        String osName = System.getProperty("os.name");
        return osName.toUpperCase().contains("DARWIN") || osName.toUpperCase().contains("MAC OS X");
    }


    public static boolean isLinux64() {
        return System.getProperty("os.name").toUpperCase().contains("LINUX") && System.getProperty("os.arch").toUpperCase().contains("AMD64");

    }

    public static boolean isLinux32() {
        return System.getProperty("os.name").toUpperCase().contains("LINUX") &&
                System.getProperty("os.arch").toUpperCase().contains("86");
    }

    public String getDate() {
        return DateUtil.shortDateFormat.print(System.currentTimeMillis());
    }
}
