package com.liquidlabs.log;

import com.liquidlabs.common.collection.cache.ConcurrentLRUCache;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import org.apache.log4j.Logger;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.LogRequestHandler;

import java.util.HashMap;
import java.util.Map;

public class LogRequestHandlerImpl implements LogRequestHandler {

	private static final Logger LOGGER = Logger.getLogger(LogRequestHandlerImpl.class);
	
	private String id = getClass().getSimpleName();
	private transient java.util.concurrent.ExecutorService executor;
	private transient AggSpace localAggSpace;
    private Indexer indexer;

    public LogRequestHandlerImpl(AggSpace aggSpace, String resourceId, Indexer indexer) {
		this.localAggSpace = aggSpace;
        this.indexer = indexer;
        this.id = getClass().getName() + resourceId;
		executor = ExecutorService.newDynamicThreadPool("manager", "search-task");
	}

	public String getId() {
		return id;
	}

	
	public void replay(final LogRequest request) {
        LOGGER.warn("replay on this handler shouldn't have been called");
	}
    public void cancel(final LogRequest request) {
        localAggSpace.cancel(request.subscriber());
    }

    ConcurrentLRUCache<String, String> searchLRU = new ConcurrentLRUCache<String, String>(30, 25);

	public void search(final LogRequest request) {

        String requestKey = request.subscriber() + request.getSubmittedTime();
        if (searchLRU.get(requestKey) != null) return;
        searchLRU.put(requestKey, requestKey);

        // allow for clock drift between manager and this agent
        request.setSubmittedTime(System.currentTimeMillis());


        executor.submit(new Runnable() {
				public void run() {
					// grab a copy before the summary bucket is created
					LogRequest copy = request.copy();
					try {
						localAggSpace.search(copy, "REQ_HANDLER", null);
					} catch (Exception e) {
						LOGGER.warn(e.getMessage(), e);
					}

				}
			});
	}

	@Override
	public Map<String, Double> volumes() {
        final HashMap<String, Double> results = new HashMap<>();
        indexer.indexedFiles(new LogFileOps.FilterCallback() {
            @Override
            public boolean accept(LogFile logFile) {
                String logFileTags = logFile.getTags();
                if (results.containsKey(logFileTags)) {
                    results.put(logFileTags, results.get(logFileTags) + logFile.getPos());
                } else {
                    results.put(logFileTags, logFile.getPos() + 0.0);
                }
                return false;
            }
        });
        return  results;
    }
}
