/**
 * 
 */
package com.liquidlabs.log.streaming;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.HistoEvent;
import com.liquidlabs.log.search.Query;
import com.liquidlabs.log.space.AggSpace;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.transport.serialization.Convertor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LiveSearchHandler implements LiveHandler {
	static final Logger LOGGER = Logger.getLogger(LiveSearchHandler.class);
	public LogRequest request;
	AggSpace aggSpace;
	private LiveCommon common;
	private final String sourceURI;
	
	public LiveSearchHandler(Indexer indexer, LogRequest request, AggSpace aggSpace, String sourceURI) {
		this.request = request;
		this.aggSpace = aggSpace;
		this.sourceURI = sourceURI;
		common = new LiveCommon(request,LOGGER);
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Creating Times:" + request.getStartTimeMs() +  " To:" + request.getEndTimeMs() + " Buckets:" + request.getBucketCount());
		}
		if (LOGGER.isDebugEnabled())	LOGGER.debug("Creating" + request.subscriber());
		
	}
	public String subscriber() {
		return request.subscriber() + "_SEARCH";
	}

//	private List<HistoEvent> generalHistos;
	Map<String, List<HistoEvent>> histosForFiles = new ConcurrentHashMap<String, List<HistoEvent>>();
	
	public int handle(LogFile logfile, String path, long time, int line, String nextLine, String fileTag, FieldSet fieldSet, String[] fieldValues) {
		if (LOGGER.isDebugEnabled())	{
			if (!path.endsWith("agent.log")) LOGGER.debug("Handle" + request.subscriber() + " path:" + path + ":" + line);
		}
		if (common.bail()) {
			try {
				if(!request.isCancelled()) {
					LOGGER.info("Was Failed,Cancelled or Expired: Cancel:" + request.isCancelled() + " Expire:" + request.isExpired());
					request.cancel();
				}
			} catch (Throwable t) {
				LOGGER.warn(t.toString(),t);
			}
			return 0;
		}

		final String fileNameOnly = FileUtil.getFileNameOnly(path);
		
		if (request.isVerbose() && !path.endsWith("agent.log")) LOGGER.info("Handle: path" + path + " file:" + fileNameOnly + ":" + line);
		int qPos = 0;
		int i = 0;
		for (Query query : request.queries()) {
			MatchResult matchResult = query.isMatching(nextLine);
			
			if (matchResult.isMatch()) {
				 
				List<HistoEvent> histo = null;
					if (query.isPassedByFilters(fieldSet, fieldValues, nextLine, matchResult, line)){
						try {
							
							synchronized (this) {
								histo = histosForFiles.get(path);
								if (histo == null) {
									histo = createHisto(logfile.getFileHost(NetworkUtils.getHostname()));
									histosForFiles.put(path, histo);
								}
							}
							HistoEvent event =  histo.get(qPos);
							query.increment(event.getBucketTime(time));
							// TODO - fix me? - needed for efficiency
							long fileStartTime = event.getStartTime();
							event.add(fieldSet, fieldValues, nextLine, time, FileUtil.getFileNameOnly(path), path, line, request.isVerbose(), fileStartTime, System.currentTimeMillis(), matchResult, request.getStartTimeMs(), request.getEndTimeMs());
							aggSpace.write(event.bucket(), false, "", 0, 0);
							event.bucket().resetAll();
							common.resetFailure();
							i++;
						} catch (Exception e) {
							common.incrementFailure();
							LOGGER.warn("Failed to Stream msg h:" +histo + " r:" + request, e);
						}
					}
			}
			qPos++;
		}
		return i;
	}

	private List<HistoEvent> createHisto(String hostname) {
		List<HistoEvent> histo;
		histo = new ArrayList<HistoEvent>();
		for (Query query1 : request.queries()) {
			Query query = (Query) Convertor.clone(query1);
			histo.add(new HistoEvent(request, 0, request.getStartTimeMs(), request.getEndTimeMs(), request.getBucketCount(), hostname, sourceURI, request.subscriber(), query, aggSpace, false));
		}
		return histo;
	}
	public boolean isExpired() {
		return common.isExpired();
	}

	public String toString() {
        return getClass().getSimpleName() + " " + common.toString();
	}
}