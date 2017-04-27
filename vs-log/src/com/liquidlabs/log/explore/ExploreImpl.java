package com.liquidlabs.log.explore;

import com.liquidlabs.admin.User;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.vso.VSOProperties;

import java.util.*;

/**
 * Created by neil.avery on 25/03/2016.
 */
public class ExploreImpl implements Explore {
    private Indexer indexer;

    public ExploreImpl(Indexer indexer) {
        this.indexer = indexer;
    }

    @Override
    public Set<String> hosts(final User user) {
        final HashSet<String> hosts = new HashSet<String>();
        final String hostname = NetworkUtils.getHostname();
        indexer.indexedFiles(new LogFileOps.FilterCallback() {
            @Override
            public boolean accept(LogFile logFile) {
                String fileHost = logFile.getFileHost(hostname);
                if (user.isFileAllowed(fileHost, logFile.getFileName(), logFile.getTags())) {
                    hosts.add(fileHost);
                }
                return false;
            }
        });
        return hosts;
    }

    @Override
    public List<String> dirs(final User user, final String host) {
        final String hostname = NetworkUtils.getHostname();
        final List<String> dirs = new ArrayList<String>();
        indexer.indexedFiles(new LogFileOps.FilterCallback() {
            @Override
            public boolean accept(LogFile logFile) {
                String thisHost = logFile.getFileHost(hostname);
                if (user.isFileAllowed(thisHost, logFile.getFileName(), logFile.getTags())) {
                    if (thisHost.equals(host)) {
                        dirs.add(logFile.getFileName());
                    }
                }
                return false;
            }
        });
        return dirs;
    }


    @Override
    public String url() {
        return LogProperties.getLogHttpServerURI();
    }
}
