package com.liquidlabs.log;

import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.search.tailer.TailerEmbeddedAggSpace;
import com.liquidlabs.log.space.LogFilters;

import java.util.List;

public interface AgentLogService {
    void setFilters(LogFilters filters);

    void enqueue(Tailer tailer, int noDataSeconds, int scheduleSeconds);

    void remove(Tailer tailer);

    void start();

    void stop();

    Indexer getIndexer();

    int tailerCount();

    void stopTailing(Tailer tailer);

    boolean isRollCandidateForTailer(String file, String Tag) throws InterruptedException;

    void roll(String filename);

    List<Tailer> getTailers();


    java.util.Map<String, Integer> getPoolSizes();

    TailerEmbeddedAggSpace getTailerAggSpace();

    int getTailerLimit();
}
