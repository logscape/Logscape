package com.liquidlabs.logserver;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.vso.agent.ResourceProfile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.log4j.Logger;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.LookupSpaceImpl;
import com.liquidlabs.vso.lookup.ServiceInfo;
import org.joda.time.DateTime;

public class LogServerImpl implements LogServer {

    private final static Logger LOGGER = Logger.getLogger(LogServer.class);

    private LookupSpace lookupSpace;
    private URI uri;
    private String id;
    private ServiceInfo serviceInfo;
    transient private ProxyFactory proxyFactory;
    String rootDirectory = ".";
    AtomicInteger activity = new AtomicInteger();
    transient Map<String, Integer> lastLineCount = new ConcurrentHashMap<String, Integer>();
    static boolean isStrict = Boolean.parseBoolean(System.getProperty("zone.strict", "true"));

    public LogServerImpl() {
    }
    public LogServerImpl(String rootDirectory, ProxyFactory proxyFactory, URI uri, LookupSpace lookupSpace) {
        this.rootDirectory = rootDirectory;
        this.proxyFactory = proxyFactory;
        this.uri = uri;
        this.lookupSpace = lookupSpace;
        this.id = getClass().getSimpleName() + UID.getUUIDWithHostNameAndTime();

    }

    @Override
    public boolean isAvailable(String hostFileNameForHash) {
        return true;
    }

    public String getId() {
        return id;
    }

    public int getStartLine(String hostFileNameForHash, String hostname, String filename) {
        LogMessage msg = new LogMessage(hostname, filename, 0, 1, 0);
        // look for cached value
        File file = msg.getFile(getRootDirForHost(hostname));
        String path = file.getPath();
        if (!file.exists() || file.length() == 0) {
            LOGGER.info("getStartLine: Detected Missing(Rolled?) file:" + path + " return -1");
            lastLineCount.remove(path);
            return 1;
        }
        Integer lastLineCount = this.lastLineCount.get(path);
        if (lastLineCount != null) return lastLineCount;

        int lineCount = msg.getLineCount(getRootDirForHost(msg.host));
        LOGGER.info("Forwarder getStartLine:" + msg.canonicalFile + " returning:" + lineCount);
        this.lastLineCount.put(path, lineCount);
        return lineCount;
    }
    public long getStartPos(String hostFileNameForHash, LogMessage msg) {
        File file = msg.getFile(getRootDirForHost(msg.host));

        String path = file.getPath();
        if (!file.exists() || file.length() == 0) {
            LOGGER.info("getStartPos: Detected Missing(Rolled?) file:" + path + " return -1");
            return -1;
        }
        LOGGER.info("Forwarder getStartPos:" + msg.canonicalFile + " returning:" + file.length());
        return file.length();
    }

    @Override
    public void deleteAccount(String id) {
        LOGGER.info("Deleting Account:" + id);
        FileUtil.deleteDir(new File(this.getRootDirForHost(id)));
    }

    @Override
    public void deleteAccountFiles(final String userId, final long maxAge) {
        proxyFactory.getScheduler().submit(new Runnable() {
            @Override
            public void run() {
                deleteOldFilesPrivate(userId, maxAge);
            }
        });


    }

    private void deleteOldFilesPrivate(String userId, long maxAge) {
        LOGGER.info("Deleting Old Data:" + userId + " Age:" + new DateTime(maxAge));
        long started = System.currentTimeMillis();
        File file = new File(this.getRootDirForHost(id));
        final List<String> startsWithYears = new ArrayList<String>(Arrays.asList("14,15,16,17,18,19,20"));
        Collection collection = FileUtils.listFiles(file, new IOFileFilter() {
                    @Override
                    public boolean accept(File file) {
                        return false;
                    }

                    @Override
                    public boolean accept(File dir, String name) {
                        return false;
                    }
                }, new IOFileFilter() {
                    @Override
                    public boolean accept(File file) {
                        String filename = file.getName();
                        for (String s : startsWithYears) {
                            if (filename.startsWith(s)) return true;
                        }
                        return false;
                    }

                    @Override
                    public boolean accept(File dir, String name) {
                        String filename = dir.getName();
                        for (String s : startsWithYears) {
                            if (filename.startsWith(s)) return true;
                        }

                        return false;
                    }
                }
        );
        int deleted = 0;
        for (Object o : collection) {
            if (o instanceof  File) {
                File dirFile = (File) o;
                if (dirFile.lastModified() < maxAge) {
                    LOGGER.info("Deleting:" + dirFile);
                    FileUtil.deleteDir(dirFile);
                    deleted++;
                }
            }
        }
        long ended = System.currentTimeMillis();
        LOGGER.info("DeletingFinished:" + userId + " ElapsedSec:" + (ended - started)/1000 + " ItemsDeleted:" + deleted);
    }

    public int handle(String canonicalFileWithSrcHost, LogMessage msg) {
        String path = msg.getFile(getRootDirForHost(msg.host)).getPath();
        if (path == null) lastLineCount.remove(path);

        activity.incrementAndGet();
        try {
            msg.write(getRootDirForHost(msg.host));
        } finally {
            activity.decrementAndGet();
        }
        return activity.get();
    }

    public void roll(String canonicalFileWithHost, String host, String fromFile, String toFile) {
        LogMessage msg = new LogMessage(host, fromFile, 0, 0, 0);
        File file = msg.getFile(getRootDirForHost(host));
        LOGGER.info("Roll:" + file.getPath() + " to:" + toFile);
        lastLineCount.remove(file.getPath());

        if (toFile == null) {
            deleted(host, fromFile, canonicalFileWithHost);
        } else {
            String rootDirForHost = getRootDirForHost(host);
            msg.roll(rootDirForHost, toFile);
        }
    }

    public void deleted(String hostFileNameForHash, String host, String filename) {
        String rootDirForHost = getRootDirForHost(host);
        new LogMessage(host, filename, 0, 0, 0).delete(rootDirForHost);
    }


    public static void main(String[] args) {
        LogServerImpl.boot(Integer.parseInt(args[0]), args[1], args[2]);
        try {
            while (true) {
                Thread.sleep(10 * 1000);
            }
        } catch (InterruptedException e) {
            LOGGER.info(e);
        }

    }

    public static void boot(int port, String lookupAddress, String rootDir) {
        try {

            port = NetworkUtils.determinePort(port);

            LOGGER.info("Booting: port:" + port + " lu:" + lookupAddress);
            ProxyFactoryImpl proxyFactory = new ProxyFactoryImpl(port, ExecutorService.newDynamicThreadPool("worker","logServer"), "LogServer");
            proxyFactory.start();
            LookupSpace lookupSpace = LookupSpaceImpl.getRemoteService(lookupAddress, proxyFactory, "LogSpaceBoot");

            LOGGER.info("LogServer FileDir:" + new File(rootDir).getAbsolutePath());
            new File(rootDir).mkdirs();
            LogServerImpl logServer = new LogServerImpl(rootDir, proxyFactory, proxyFactory.getAddress(), lookupSpace);
            logServer.start();
            proxyFactory.registerMethodReceiver(LogServer.NAME, logServer);

            LOGGER.info("Using LUSpace:" + lookupSpace);
            new ResourceProfile().scheduleOsStatsLogging(proxyFactory.getScheduler(), LogServer.class, LOGGER);

        } catch (Throwable t) {
            LOGGER.error(t.getMessage());
        }
    }
    public static LogServer getRemoteService(String whoAmI, LookupSpace lookupSpace, ProxyFactory proxyFactory) {

        LogServer remoteService = SpaceServiceImpl.getRemoteService(whoAmI, LogServer.class, lookupSpace, proxyFactory, LogServer.NAME, true, System.getProperty("fwdr.strict.zone", "true").equals("true"));
        LOGGER.info("Using LogServer:" + remoteService);
        return remoteService;
    }

    public void start() {
        serviceInfo = new ServiceInfo(LogServer.NAME, uri.toString(), null, JmxHtmlServerImpl.locateHttpUrL(), "vs-log-server-1.0", LogServer.NAME, VSOProperties.getZone(), VSOProperties.getResourceType());
        try {
            serviceInfo.meta = new File(rootDirectory).getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOGGER.info(String.format("Registering service[%s] %s Location:%s", LogServer.NAME, serviceInfo, serviceInfo.getLocationURI()));

        // TODO: should be using a lease manager
        lookupSpace.registerService(serviceInfo, -1);
    }

    public void stop() {
        lookupSpace.unregisterService(serviceInfo);
        proxyFactory.stop();
    }
    public void setRootDir(String rootDir) {
        FileUtil.mkdir(rootDir);
        this.rootDirectory = rootDir;
    }
    private String getRootDirForHost(String host) {
        return String.format("%s/%s", rootDirectory, host.replace(":","_"));
    }
}
