package com.liquidlabs.log.streaming;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.log4j.Logger;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.log.Tailer;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;

public class StreamingRequestHandlerImpl implements StreamingRequestHandler {
	public enum LIVE_TYPE  {SEARCH, REPLAY };
	static final Logger LOGGER = Logger.getLogger(StreamingRequestHandlerImpl.class);
	
	private final Map<String, Tailer> tailers;
	private final AggSpace aggSpace;
	CopyOnWriteArrayList<LogRequest> requests = new CopyOnWriteArrayList<LogRequest>();
	private final String endPointURI;

	private final Indexer indexer;

	public StreamingRequestHandlerImpl(final Map<String, Tailer> tailers, AggSpace aggSpace, String endPointURI, Indexer indexer, ScheduledExecutorService scheduler) {
		this.tailers = tailers;
		this.aggSpace = aggSpace;
		this.endPointURI = endPointURI;
		this.indexer = indexer;
	}

	public void start(LogRequest request) {
        request = request.copy();
        if (!request.isStreaming()) return ;
		LOGGER.info("LOGGER Starting LiveRequest:" + request);
		requests.add(request);
		LiveHandler liveHandler = getHandler(request);
		int i  = 0;
		for (Tailer tailer : tailers.values()) {
			try {
                if (indexer.isIndexed(tailer.filename()))  {
                    LogFile tailerFile = indexer.openLogFile(tailer.filename());
                    if (tailerFile != null && request.isSearchable(tailerFile, tailerFile.getFileHost(NetworkUtils.getHostname()))) {
                        if (request.isVerbose()) LOGGER.info(" ATTACHING:" +  " with:" +  tailer.filename() + "/" + tailer.fileTag() + " s:" + request.subscriber());
                        tailer.addLiveHandler(liveHandler);
                        i++;
                    }
                }
			} catch (Throwable t) {
				LOGGER.warn("Can't live stream:" + tailer, t);
			}
		}
		
		removeOldRequests();
		LOGGER.info("Starting LiveRequest, added to TailerCount:" + i + " r:" + request.subscriber());
	}


	public void attachToTailer(Tailer tailer) {
		for (LogRequest request : requests) {
			try {
				LiveHandler liveHandler = getHandler(request);
				
				LogFile tailerFile = indexer.openLogFile(tailer.filename());
				if (isRunning(request) && request.isSearchable(tailerFile, tailerFile.getFileHost(NetworkUtils.getHostname()))) {
					if (request.isVerbose()) LOGGER.info("ATTACHING:" +  " with:" +  tailer.filename() + "/" + tailer.fileTag() + " s:" + request.subscriber());
					tailer.addLiveHandler(liveHandler);
				}
			} catch (Throwable t) {
				LOGGER.warn("Can't live stream:" + tailer, t);
			}
		}
	}

    private boolean isRunning(LogRequest request) {
        return !(request.isExpired() || request.isCancelled());
    }

    private LiveHandler getHandler(LogRequest request) {
		LiveHandler liveHandler = null;
		LIVE_TYPE type  = request.getReplay() == null ? LIVE_TYPE.SEARCH : LIVE_TYPE.REPLAY;
		if (type == LIVE_TYPE.REPLAY) {
			liveHandler = new LiveReplayHandler(indexer, request.copy(), aggSpace, this.endPointURI);
		} else {
			liveHandler = new LiveSearchHandler(indexer, request.copy(), aggSpace, this.endPointURI);
		}
		return liveHandler;
	}
	
	private void removeOldRequests() {
		for (LogRequest request : requests) {
			if (request.isCancelled() || request.isExpired()) {
				requests.remove(request);
			}
		}
	}

}
