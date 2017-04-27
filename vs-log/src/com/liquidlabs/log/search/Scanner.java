package com.liquidlabs.log.search;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.LogRequest;

public interface Scanner {

    int search(LogRequest request, List<HistoEvent> histos, AtomicInteger sent);

    String getFilename();


    LogFile getLogFile();

    double getPercentComplete();

    long eventsComplete();

    boolean isComplete();
}
