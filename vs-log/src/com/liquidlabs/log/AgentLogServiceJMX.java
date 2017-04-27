package com.liquidlabs.log;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.CompactCharSequence;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.jmx.JmxHtmlServerImpl;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.logscape.disco.indexer.*;
import com.liquidlabs.log.space.DataSourceArchiver;
import com.liquidlabs.log.space.WatchDirectory;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.lookup.LookupSpace;
import com.liquidlabs.vso.lookup.ServiceInfo;
import com.liquidlabs.vso.work.InvokableImpl;
import com.liquidlabs.vso.work.InvokableUI;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AgentLogServiceJMX implements AgentLogServiceJMXMBean, InvokableUI {

    final static Logger LOGGER = Logger.getLogger(AgentLogServiceJMX.class);
    private final AgentLogService agent;
    private final WatchVisitor watchVisitor;
    private DataSourceArchiver archiver;
    String id = "";

    public AgentLogServiceJMX(AgentLogService agent, WatchVisitor watchVisitor, LookupSpace lookupSpace, ProxyFactory proxyFactory, DataSourceArchiver archiver) {
        this.agent = agent;
        this.watchVisitor = watchVisitor;
        this.archiver = archiver;
        this.id = "AgentLogServiceMX-" + NetworkUtils.getHostname();
        try {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName("com.liquidlabs.logscape:Service=Indexer");
            if (mbeanServer.isRegistered(objectName)) return;
            mbeanServer.registerMBean(this, objectName);
        } catch (Exception e) {
            LOGGER.error(e);
        }
        try {
            registerServiceWithLookup(lookupSpace, proxyFactory);
        } catch (Throwable t) {
            LOGGER.error("Failed to register MX", t);
        }
    }
    public String nukeDBSchemaFile(String args) {
        return "Schema File Deleted: " + new File("work/DB/db.schema.id").delete();
    }

    public int getTailerBacklog(){
        return TailerImpl.getBacklog();
    }

    @Override
    public void causeIntentionalCrash() {
        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("BOOOOM!");
                    }
                }

        );
        System.exit(10);

    }

    public String archiveRulesFireNow(String args) {
        return this.archiver.run();
    }
    @Override
    public String getQueueSizes() {
        //agent.
        final StringBuilder results = new StringBuilder();
        Map<String, Integer> poolSizes = agent.getPoolSizes();
        for (Map.Entry<String, Integer> pool : poolSizes.entrySet()) {
            results.append(pool.getKey() + " = " + pool.getValue()).append(" ");
        }

        return results.toString();
    }
    @Override
    public int getTailerCount() {
        return agent.getTailers().size();
    }
    @Override
    public int getTailerLimit() {
        return agent.getTailerLimit();
    }
    @Override
    public String getVisitorQueue() {
        return watchVisitor.queue.toString();
    }
    public int getIndexFileCount() {
        return agent.getIndexer().size();
    }
    @Override
    public int getCurrentSearchCount() {
        return agent.getTailerAggSpace().getCurrentSearchCount();
    }
    @Override
    public int getCurrentHistoAggCount() {
        return agent.getTailerAggSpace().getHistoAggsCount();
    }

    @Override
    public boolean isDiscoveryEnabled() {
        return  !KvIndexFactory.isDisabled;
    }

    @Override
    public void suspendFieldDiscovery(boolean enabled) {
        KvIndexFactory.isDisabled = !enabled;
    }

    public String discoveredFieldDBStats(String filter) {
        long start = System.currentTimeMillis();
//        KvIndexDb kvIndexDb = new KvIndexDb(KvIndexFeed.kvIndexDB, PersisitDbFactory.getDictionary());
        String stats = "";
        long end = System.currentTimeMillis();
        return " Elapsed(ms):" + (end - start) + "<br>" + stats.replace("\n","<br>");
    }
    @Override
    public String discoveredFieldDBOrhpaned(String filter) {
        long start = System.currentTimeMillis();
        String stats = "";
        long end = System.currentTimeMillis();
        return " Elapsed(ms):" + (end - start) + "<br>" + stats.replace("\n","<br>");
    }

    public String indexedFilesLineCount(final String filename) {
        Indexer indexer = agent.getIndexer();
        final AtomicLong events = new AtomicLong();
        long lineStoreSize = indexer.getLineStoreSize();
        return "Total minute granularity line events:" + lineStoreSize;

    }

    public String indexedFiles(final String filter) {
        Indexer indexer = agent.getIndexer();
        final AtomicInteger added = new AtomicInteger();
        final StringBuilder results = new StringBuilder();
        final AtomicInteger totalFiles = new AtomicInteger();
        long start = System.currentTimeMillis();
        final AtomicLong volume = new AtomicLong();
        final AtomicLong events = new AtomicLong();
        indexer.indexedFiles(new LogFileOps.FilterCallback() {
            @Override
            public boolean accept(LogFile logFile) {
                totalFiles.incrementAndGet();
                volume.addAndGet(logFile.getPos());
                events.addAndGet(logFile.getLineCount());
                if (added.get() >= 10000) return false;

                if (filter == null || filter.length() == 0 || StringUtil.containsIgnoreCase(logFile.toString(), filter)) {
                    added.incrementAndGet();
                    results.append(added.get() + ")" + logFile.toString()).append("\n<br>");
                }
                return false;
            }
        });
        long end = System.currentTimeMillis();
        String eventString = events.get() > 1000 * 1000 ? (events.get()/1000000.0) + "m" : events.toString();

        return "TotalFiles:" + totalFiles + " TotalVolume(G):" + FileUtil.getGIGABYTES(volume.get()) + " TotalEvents:" + eventString +
                " Elapsed(ms):" + (end - start) + "<br>" + results.toString();
    }
    public String listTailers(String filename) {
        if (filename == null) filename = "";
        try {
            List<Tailer> tailers = new ArrayList<Tailer>(this.agent.getTailers());
            Collections.sort(tailers,new Comparator<Tailer>(){
                public int compare(Tailer o1, Tailer o2) {
                    if (o1.filename().equals(o2.filename())) {
                        return Long.valueOf(o1.lastMod()).compareTo(o2.lastMod());
                    }
                    return o1.filename().compareTo(o2.filename());
                }
            });
            StringBuilder stringBuilder = new StringBuilder("Results:<br>Total:" + tailers.size() + "<br>");
            int count = 0;
            for (Tailer tailer : tailers) {
                if (tailer.filename().contains(filename)) {
                    stringBuilder.append(count++ + ") ").append(tailer.toString()).append("<br><br>\n");
                }
            }
            return stringBuilder.toString();
        } catch (Throwable t) {
            return ExceptionUtil.stringFromStack(t, -1).replaceAll("\n", "<br>");
        }
    }
    public String watchVisitorStoppedWatching(String args) {
        StringBuilder builder = new StringBuilder("Results:");
        List<String> stoppedIndexing = new ArrayList<String>();//(watchVisitor.stoppedIndexing);
        Collections.sort(stoppedIndexing);

        int i = 0;
        for (String string : stoppedIndexing) {
            builder.append(i++ + ") ").append(string).append("<br>\n");
        }
        return builder.toString();
    }

    public String watchVisitorCurrentlyHandled(String args) {

        List<CompactCharSequence> currentlyHandledFiles = new ArrayList<CompactCharSequence>(watchVisitor.currentlyHandledFiles());
        StringBuilder builder = new StringBuilder("Results: (total:" + currentlyHandledFiles.size() + ")<br>");
        //Collections.sort(currentlyHandledFiles);
        int i = 0;
        for (CompactCharSequence file : currentlyHandledFiles) {
            builder.append(i++ + ") ").append(file).append("<br>\n");
        }
        return builder.toString();
    }


    public String watchVisitorLastMod(String filter) {

        try {
            if (filter == null) filter = "";
            Map<String, Long> lastModifiedDirs = watchVisitor.lastModifiedDirs(filter);
            Set<String> keySet = lastModifiedDirs.keySet();
            StringBuilder builder = new StringBuilder("Results:");
            int count = 1;
            for (String string : keySet) {
                Long long1 = lastModifiedDirs.get(string);
                builder.append(count++).append(") ").append(string).append(" - ").append(new DateTime(long1.longValue())).append("<br>\n");
            }

            return builder.toString();
        } catch (Throwable t) {
            t.printStackTrace();
            return ExceptionUtil.stringFromStack(t, -1);
        }
    }


    public String watchVisitorQueue(String args) {
        return  watchVisitor.queue.toString();
    }
    public String watchVisitorDirs(String args) {
        Collection<WatchDirectory> values = watchVisitor.watchDirSet.values();
        int i = 0;
        StringBuilder builder = new StringBuilder("Results:");
        for (WatchDirectory watchDirectory : values) {
            String strgval = watchDirectory.toString();
            if (args != null && strgval.contains(args) || args == null) {
                builder.append(i++ + ") ").append(strgval).append("<br>\n");
            }
        }
        return builder.toString();
    }
    public String listFwderAutoDataSources(String args) {
        int i = 1;
        StringBuilder builder = new StringBuilder("Results:<br>");
        ArrayList<String> keys = new ArrayList<String>(watchVisitor.fwdWatchDirSet.keySet());
        Collections.sort(keys);
        if (args == null) args = "";
        for (String watchDirectory : keys) {
            String str = watchVisitor.fwdWatchDirSet.get(watchDirectory).toString();
            if (str.contains(args)) builder.append(i++ + ") ").append(str).append("<br>\n");
        }
        return builder.toString();
    }

    public String listFwderServerDirs(String args) {
        return watchVisitor.serverDirs.toString().replaceAll(",","<br>/n");
    }

    public String getId() {
        return id;
    }

    //Invokable UI Stuff!
    private void makeRemotable(ProxyFactory proxyFactory) {
        if (proxyFactory != null) {
            InvokableImpl invokable = new InvokableImpl(this);
            proxyFactory.registerMethodReceiver(getId(), invokable);
        }
    }
    public String getUI() {
        return "<root>" +
                "<panel>" +
                "	<title label='" + "AgentTailerService" + "'/>"+
                "	<label label=' --------------------' padding='30'/>"+
                "  <row2 spaceOneWidth='10' label='List Tailers' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='listTailers' inputLabel='' taText='             ' outputHeight='100' />\n" +
                "  <row2 spaceOneWidth='10' label='WatchVisitorStoppedWatching' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='watchVisitorStoppedWatching' inputLabel='' taText='             ' outputHeight='100' />\n" +
                "  <row2 spaceOneWidth='10' label='WatchVisitorCurrentlyHandled' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='watchVisitorCurrentlyHandled' inputLabel='' taText='             ' outputHeight='100' />\n" +
                "  <row2 spaceOneWidth='10' label='WatchVisitorDirs' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='watchVisitorDirs' inputLabel='' taText='             ' outputHeight='100' />\n" +
                " </panel>" +
                "</root>";

    }
    public String getUID() {
        return id;
    }
    public void registerServiceWithLookup(LookupSpace lookupSpace, ProxyFactory proxyFactory) {
        makeRemotable(proxyFactory);
        final ServiceInfo serviceInfo = new ServiceInfo(getId(), proxyFactory.getAddress().toString(), AgentLogServiceJMX.class.getName(), JmxHtmlServerImpl.locateHttpUrL(), "Tailer:" + VSOProperties.getResourceType(), getId(), VSOProperties.getZone(), VSOProperties.getResourceType());
        lookupSpace.registerService(serviceInfo, -1);
    }
    public String pauseVisitor(boolean pauseTrueOfFalse) {
        watchVisitor.suspend(pauseTrueOfFalse);
        return pauseTrueOfFalse + "";
    }
}
