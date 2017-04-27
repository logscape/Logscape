package com.liquidlabs.log.server;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.Pair;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.InMemoryIndexer;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportFactoryImpl;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.agent.ResourceAgentImpl;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.lookup.ServiceInfo;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;
import java.util.concurrent.Executors;

import static java.lang.String.format;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 09/01/2013
 * Time: 17:18
 * To change this template use File | Settings | File Templates.
 */
public class LogHttpServer {

    public static final int FROM_END = 32 *  1024;
    // jquery filter to collapse everything:
    // $(".word_split:not(:contains('ResourceAgent'))").show()
    static String spanPre = "<span>";
    static String spanMark = "<span>";
    static String spanPost = "</span>";
    static int pageSize = Integer.getInteger("log.page.size", 5) * 1000;

    static final Logger LOGGER = Logger.getLogger(LogHttpServer.class);
    static int port = LogProperties.getLogHttpPort();
    static int tailLines = 30;
    static final String WEB_PORT = System.getProperty("override.web.port");
    static final long timestamp = System.currentTimeMillis();

    private Set<String> clientIpds = new HashSet<String>();
    static private boolean enforceIndexed = System.getProperty("httplogserver.enforce.indexed","true").equals("true");

    public LogHttpServer(Indexer indexer) throws Exception {
        LOGGER.info("Starting HTTP Server Port:" + port);
        Server server = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(port);
        System.out.println("LogHttpServer Port:" + port);
        connector.setHost("0.0.0.0");
        server.addConnector(connector);
        FileTailingWebSocketHandler webSocketHandler = new FileTailingWebSocketHandler(indexer);
        webSocketHandler.setHandler(new DefaultHttpHandler(indexer, clientIpds));
        server.setHandler(webSocketHandler);
        server.start();

        addLocalIps();
    }

    private void addLocalIps() {
        Enumeration<NetworkInterface> networkInterfaces = null;
        try {
            networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface net = (NetworkInterface) networkInterfaces.nextElement();
                Set<InetAddress> eaddr = new HashSet<InetAddress>(Collections.list(net.getInetAddresses()));
                for (Iterator iterator = eaddr.iterator(); iterator.hasNext();) {
                    InetAddress inetAddress = (InetAddress) iterator.next();
                    clientIpds.add(inetAddress.getHostAddress());
                    clientIpds.add(inetAddress.getHostName());
                    clientIpds.add(inetAddress.getCanonicalHostName());
                }
                Enumeration<NetworkInterface> subInterfaces = net.getSubInterfaces();
                while (subInterfaces.hasMoreElements()) {
                    NetworkInterface nextElement = subInterfaces.nextElement();
                    Set<InetAddress> myaddr = new HashSet<InetAddress>(Collections.list(nextElement.getInetAddresses()));
                    eaddr.removeAll(myaddr);
                    InetAddress inetAddress = myaddr.iterator().next();
                    clientIpds.add(inetAddress.getHostAddress());
                    clientIpds.add(inetAddress.getHostName());
                    clientIpds.add(inetAddress.getCanonicalHostName());
                }
            }
        } catch (Exception e) {

        }

    }

    public void addClientAddress(String clientAddress) {
        try {
            URI uri = new URI(clientAddress);
            String host = uri.getHost();
            clientIpds.add(host);
            clientIpds.add(InetAddress.getByName(host).getHostAddress());
            clientIpds.add(InetAddress.getByName(host).getHostName());
            clientIpds.add(InetAddress.getByName(host).getCanonicalHostName());


        } catch (Throwable t) {

        }
        LOGGER.info("ClientIps:" + clientIpds);
    }


    public static void main(String[] args) throws Exception {
        System.setProperty("vso.base.port", "10000");
        System.setProperty("lookup.url","stcp://localhost:11000");
        enforceIndexed = false;
        System.out.println("Starting On :" + LogProperties.getLogHttpPort() + " Dir:" + new File(".").getAbsolutePath());
        final LogHttpServer httpServer = new LogHttpServer(new InMemoryIndexer());
        synchronized (httpServer) {
            httpServer.wait();
        }
    }


    public enum ReturnFormat { DOWNLOAD, RAW, HTML }
    public static class DefaultHttpHandler extends AbstractHandler {

        private final Indexer indexer;
        private Set<String> clientIpds;

        public DefaultHttpHandler(Indexer indexer, Set<String> clientIpds) {
            this.indexer = indexer;
            this.clientIpds = clientIpds;
        }

        private boolean isProxied(String param) {
            return param != null && Boolean.valueOf(param);
        }

        @Override
        public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse response) throws IOException, ServletException {

            try {
                String path = httpServletRequest.getPathInfo();
                boolean proxied = isProxied(request.getParameter("proxied"));

                if (!proxied) {
                    if (clientIpds.size() > 0) {
                        if (!clientIpds.contains(httpServletRequest.getRemoteAddr())) {
//                            String message = "Invalid Access from Client:" + httpServletRequest.getRemoteAddr();
//                            throw new RuntimeException(message);
                            LOGGER.warn("Invalid access from client:" +httpServletRequest.getRemoteAddr() + " addr:" + clientIpds );
                        }
                    }
                }

                if (path.endsWith(".css")) {
                    response.setContentType("text/css; charset=UTF-8");
                    String content = loadResource(path);
                    response.getWriter().write(content);
                    response.getWriter().flush();
                    response.setStatus(HttpServletResponse.SC_OK);
                    request.setHandled(true);

                    return;
                }
                final String SOURCE = getSource(proxied);

                ReturnFormat returnFormat = getReturnFormat(path);
                path = path.replace(".html", "");
                path = path.replace(".raw", "");

                try {
                    File file = new File(path);
                    if (path.startsWith("//")) path = path.substring(1);

                    if (enforceIndexed && !indexer.isIndexed(path)) {
                        String otherPath = FileUtil.makePathNative(path);
                        if (!indexer.isIndexed(otherPath)) {
                            throw new RuntimeException(NetworkUtils.getHostname() + " File is not Indexed, Access Denied:'" + otherPath + "'");
                        }
                    }

                    if (returnFormat == ReturnFormat.HTML) {
                        returnHtmlContent(httpServletRequest, response, path, proxied, SOURCE, file);
                    } else if (returnFormat == ReturnFormat.RAW) {
                        returnRAWContent(httpServletRequest, response, path, proxied, SOURCE, file);
                    } else {
                        returnDownload(response, file);
                    }
                } finally {
                    request.setHandled(true);
                }
            } catch (Throwable t) {
                String s1 = ExceptionUtil.stringFromStack(t, -1).replaceAll("\n", "<br>");
                response.setContentType("text/html; charset=UTF-8");
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().write(s1);
                request.setHandled(true);
                return;
            }
        }

        private void returnDownload(HttpServletResponse response, File file) throws IOException {
            RAF raf = RafFactory.getRafSingleLine(file.getAbsolutePath());
            String responseFilename = file.getName();
            if (responseFilename.endsWith(".gz")) responseFilename = responseFilename.replace(".gz","");
            if (responseFilename.endsWith(".snap")) responseFilename = responseFilename.replace(".snap", "");

            response.setContentType("text/plain; charset=UTF-8");
            response.setHeader("Content-disposition", format("attachment; filename=%s", responseFilename));
            response.setStatus(HttpServletResponse.SC_OK);

            OutputStream outputStream = new BufferedOutputStream(response.getOutputStream());
            String line;
            while ((line = raf.readLine()) != null) {
                outputStream.write(line.getBytes());
                outputStream.write('\n');
            }
            outputStream.close();
        }
        private void returnRAWContent(HttpServletRequest httpServletRequest, HttpServletResponse response, String path, boolean proxied, String SOURCE, File file) throws IOException {
            response.setContentType("text/plain; charset=UTF-8");
            response.setStatus(HttpServletResponse.SC_OK);


            Object from = httpServletRequest.getParameter("from");
            Object requestedSeekPos = httpServletRequest.getParameter("pos");


            Writer writer = new BufferedWriter(response.getWriter());

            LogLineOutput output = new LogLineOutput(writer);



            final Pair<Long, Long> startingPos = seekPosAndLineNo(file, from, requestedSeekPos);
            if (from == null) from = startingPos._1.toString();
//            String theTemplate = getHeader(path, Long.parseLong(from.toString()), SOURCE, file.length(), proxied);
            final ChunkOfFileHandler chunkOfFileHandler = new ChunkOfFileHandler(writer, output, file, pageSize);

            final Pair<Long, Long> next = chunkOfFileHandler.nextChunk("", startingPos._1, startingPos._2, Long.parseLong(from.toString()));

            writer.write(footer(next,Long.parseLong(from.toString())));

            writer.close();
        }


        private void returnHtmlContent(HttpServletRequest httpServletRequest, HttpServletResponse response, String path, boolean proxied, String SOURCE, File file) throws IOException {
            response.setContentType("text/html; charset=UTF-8");
            response.setStatus(HttpServletResponse.SC_OK);


            Object from = httpServletRequest.getParameter("from");
            Object requestedSeekPos = httpServletRequest.getParameter("pos");


            Writer writer = new BufferedWriter(response.getWriter());

            LogLineOutput output = new LogLineOutput(writer);

            String closeDiv = " </div>\n" +
                    "        </pre>\n";


            final Pair<Long, Long> startingPos = seekPosAndLineNo(file, from, requestedSeekPos);
            if (from == null) from = startingPos._1.toString();
            String theTemplate = getHeader(path, Long.parseLong(from.toString()), SOURCE, file.length(), proxied);
//                        writer.write(theTemplate);
            final ChunkOfFileHandler chunkOfFileHandler = new ChunkOfFileHandler(writer, output, file, pageSize);

            final Pair<Long, Long> next = chunkOfFileHandler.nextChunk(theTemplate, startingPos._1, startingPos._2, Long.parseLong(from.toString()));

            writer.write(closeDiv);
            writer.write(footer(next,startingPos._1));

            writer.close();
        }

        private ReturnFormat getReturnFormat(String path) {
            path = path.toUpperCase();
            ReturnFormat[] values = ReturnFormat.values();
            for (ReturnFormat format : values) {
                if (path.endsWith("." + format.name())) return format;
            }

            return ReturnFormat.DOWNLOAD;
        }

        private Pair<Long, Long> seekPosAndLineNo(File file, Object from, Object requestedSeekPos) {
            long fromLine = from == null ? 1 : Long.parseLong(from.toString());
            if (requestedSeekPos != null && requestedSeekPos.toString().equalsIgnoreCase("END")) {
                if (!FileUtil.isCompressedFile(file.getName())) {
                    LogFile logFile1 = indexer.openLogFile(file.getAbsolutePath());
                    long fLine = logFile1.getLineCount() - 300;
                    if (fLine < 1) fLine = 1;
                    long pos = indexer.filePositionForLine(logFile1.getFileName(), fLine);
                    return new Pair<Long, Long>(fLine, pos);

                }
                /**
                 * Compressed files are a PITA - we dont want to scan the file because thats slow
                 * so instead use the timeseries index to get the position of the last lines
                 */
                if (FileUtil.isCompressedFile(file.getName()) && indexer != null) {
                    LogFile logFile = indexer.openLogFile(FileUtil.getPath(file));
                    if (logFile != null) {
                        int line =  logFile.getLineCount() > 10 ? logFile.getLineCount() - 10 : logFile.getLineCount();
                        int fline = line > 10 ? line - 10 : 1;
                        List<Line> lines = indexer.linesForNumbers(FileUtil.getPath(file), (int) fline, (int) line + 10);
                        if (lines != null && lines.size() > 0) {
                            return new Pair<Long, Long>((long)lines.get(0).number(), lines.get(0).position());
                        }
                    }
                }
            }
            long seekPos = requestedSeekPos == null? 0 : Long.parseLong(requestedSeekPos.toString());

            if(from == null && requestedSeekPos == null) {
                return new Pair<Long, Long>(fromLine,seekPos);
            }

            if(from == null) {
                final long[] lineAndPosSeek = FileUtil.getLineAndPosSeek(file, seekPos);
                return new Pair<Long, Long>(lineAndPosSeek[0], lineAndPosSeek[1]);
            }

            if (requestedSeekPos == null) {
                long sLine = fromLine - 10 < 1 ? 1 : fromLine - 10;
                LogFile logFile = indexer.openLogFile(FileUtil.getPath(file));
                if (fromLine > logFile.getLineCount()) {
                    int line = logFile.getLineCount() - 40;
                    if (line < 1) line = 1;
                    long pos = indexer.filePositionForLine(FileUtil.getPath(file), line);
                    return new Pair<Long, Long>((long) line, pos);
                } else {
                    List<Line> lines = indexer.linesForNumbers(FileUtil.getPath(file), (int) sLine, (int) fromLine + 10);
                    return new Pair<Long, Long>((long) lines.get(0).number(), lines.get(0).position());
                }
            }

            return new Pair<Long, Long>(fromLine, seekPos);
        }
        private String footer(Pair<Long, Long> next, long from) {
            Long previousLine = from - pageSize < 1 ? 1 : from - pageSize;
            return "<div id='stuff' style='display:none'>\n" + " <prev line='" + previousLine + "'/>\n" + "<next line='" + next._1 + "' pos='" + next._2 + "'/>\n" + "</div>\n" + "</body>\n" + "</html>";
        }

        private String loadResource(String name) throws IOException {

            InputStream template = getClass().getResourceAsStream(name);
            if (template == null) {
                String msg = "Failed to load template:" + name + " Dir:" + new File(".").getAbsolutePath();
                LOGGER.error(msg);
                return "<!DOCTYPE html>\n" +
                        "<html>\n" +
                        "<body>\n" +
                        "<h1>" + msg + "</h1>\n" +
                        "</body>\n" +
                        "</html>";
            }
            int available = template.available();
            if (available == 0) {
                LOGGER.error("Failed to load template");
                return "<!DOCTYPE html>\n" +
                        "<html>\n" +
                        "<body>\n" +
                        "<h1>Failed to load template from filesystem: 0 length</h1>\n" +
                        "</body>\n" +
                        "</html>";

            }
            byte[] bytes = new byte[available];
            template.read(bytes);
            return new String(bytes);

        }

        private String getHeader(String path, long fromLine, String SOURCE_URL, long filePos, boolean proxied) throws IOException {
            String template = loadResource("/lhs-template.html");
            template = template.replaceAll("\\{SOURCE\\}", SOURCE_URL);
            template = template.replaceAll("\\{VERSION\\}", Long.toString(timestamp));
            template = template.replace("{title}", path);
            template = template.replace("{urlStart}", path + ".html?from=0");
            long prevLine = fromLine - pageSize;
            if (prevLine < 0) prevLine = 0;
            prevLine = (prevLine / pageSize) * pageSize;
            long nextLine = (prevLine + pageSize) + pageSize;
            template = template.replace("{urlPrev}", path + ".html?from=" + prevLine);
            template = template.replace("{urlNext}", path + ".html?from=" + nextLine);
            template = template.replace("{urlEnd}", path + ".html?pos=END");
            template = template.replace("{tailFile}", path);
            template = template.replaceAll("\\{WS_URL\\}", getHost() + ":" + port);
            template = template.replace("{PROXIED}", Boolean.toString(proxied));

            String[] hostname = LogFile.getHostnameFromPath(path);
            if (hostname != null) {
                template = template.replace("{FILENAME}", hostname[0] + " : "+ hostname[1]);
            } else {
                template = template.replace("{FILENAME}", NetworkUtils.getHostname() + " : " + path);

            }
            return template.replace("{lineNumber}", "" + fromLine);
        }

        private String getHost() {
            return NetworkUtils.getIPAddress();
        }

        LookupSpace lookupSpace = null;

        public String getSource(boolean proxied) {
            if (proxied) {
                return "";
            }
            try {
                if (lookupSpace == null) {
                    // TODO: make this lookup the root.war address properly
                    lookupSpace = (LookupSpace) ResourceAgentImpl.runtimeVariables.get("lookupSpace");
                    if (lookupSpace == null) {
                        // non-embedded mode / standalone - test
                        TransportFactoryImpl transportFactory = new TransportFactoryImpl(ExecutorService.newDynamicThreadPool("pftest", "PROXY_FACTORY_TEST"), "test");
                        transportFactory.start();
                        ProxyFactoryImpl proxyFactory = null;
                        proxyFactory = new ProxyFactoryImpl(transportFactory, Config.TEST_PORT, Executors.newFixedThreadPool(10), "testService");
                        proxyFactory.start();
                        lookupSpace = LookupSpaceImpl.getLookRemoteSimple(VSOProperties.getLookupAddress(), proxyFactory, "TESTING");
                    }
                }
                List<ServiceInfo> service = lookupSpace.findService("name equals Dashboard");
                String jmxURLs = "";
                if (service.size() > 0) {
                    for (ServiceInfo serviceInfo : service) {
                        jmxURLs = serviceInfo.getJmxURL();
                    }
                }

                String uri = jmxURLs.split(" ")[0];


                URI uriU = new URI(uri);
                String host = uriU.getHost();
                if (WEB_PORT != null) {
                    return host + ":" + WEB_PORT;
                }
                return "http://" + host + ":" + uriU.getPort();
            } catch (Throwable t) {
                t.printStackTrace();
            }
            return "";
        }
    }

}
