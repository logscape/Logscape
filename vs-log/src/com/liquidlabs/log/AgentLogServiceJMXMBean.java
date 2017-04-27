package com.liquidlabs.log;

public interface AgentLogServiceJMXMBean {

    String nukeDBSchemaFile(String args);

    void causeIntentionalCrash();

    String archiveRulesFireNow(String args);

    String indexedFiles(String filename);

    String indexedFilesLineCount(final String filename);

    String discoveredFieldDBStats(String filter);

    String listTailers(String filename);

    String watchVisitorStoppedWatching(String args);

    String watchVisitorCurrentlyHandled(String args);

    String watchVisitorQueue(String args);

    String watchVisitorDirs(String args);

    String listFwderAutoDataSources(String args);
    String listFwderServerDirs(String args);

    String pauseVisitor(boolean pauseTrueOrFalse);

    String watchVisitorLastMod(String args);

    String getQueueSizes();

    boolean isDiscoveryEnabled();

    void suspendFieldDiscovery(boolean enabled);

    int getTailerLimit();

    String getVisitorQueue();

    int getIndexFileCount();

    int getCurrentSearchCount();

    int getTailerCount();

    int getCurrentHistoAggCount();

    int getTailerBacklog();

    String discoveredFieldDBOrhpaned(String filter);
}
