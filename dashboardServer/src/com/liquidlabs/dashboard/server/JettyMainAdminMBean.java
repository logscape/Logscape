package com.liquidlabs.dashboard.server;

public interface JettyMainAdminMBean {

    String dumpThreads();

    String displaySystemProperties();

    String reindex(String tag);

}
