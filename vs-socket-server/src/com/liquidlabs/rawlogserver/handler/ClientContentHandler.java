package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.rawlogserver.handler.ContentFilteringLoggingHandler.TypeMatcher;
import com.liquidlabs.rawlogserver.handler.fileQueue.FileQueuer;
import com.liquidlabs.rawlogserver.handler.fileQueue.FileQueuerFactory;
import javolution.util.FastMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles the stream of data from a single client
 * @author neil
 *
 */
public class ClientContentHandler {
	private static final String NLINE = "\n";
	private static final String MLINE_TRUE = "mline=true";
	final static Logger LOGGER = Logger.getLogger(ClientContentHandler.class);
	private static final String TIMESTAMP_TRUE = "timestamp=true";
	private static final String NL_W_SPACE = "\n ";
	private static final String NL = NLINE;
	protected static String eol = System.getProperty("line.separator");
	protected static final String SPACE = " ";
	private static final String SLASH = "/";
	
	
	private final String hostPath;
	private final List<TypeMatcher> matchers;
    private final ScheduledFuture<?> scheduledFuture;
    private FileQueuerFactory fqFactory;
    public Map<String, FileQueuer> fileQueue = new FastMap<String, FileQueuer>();

	public ClientContentHandler(String hostPath, List<TypeMatcher> matchers, ScheduledExecutorService scheduler, FileQueuerFactory fqFactory) {
		this.hostPath = hostPath;
		this.matchers = matchers;
        this.fqFactory = fqFactory;

        // ensure a scheduled flush writes data off to disk
        scheduledFuture = scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Collection<FileQueuer> queuers = fileQueue.values();
                for (FileQueuer queuer : queuers) {
                    queuer.run();
                }

            }
        }, 2, Integer.getInteger("raw.server.fq.delay.secs", 2), TimeUnit.SECONDS);
    }
	
	String[] lastParts;
	TypeMatcher lastType;

	public void handleContent(String sourceHost, String payloadLine, ContentFilteringLoggingHandler parent, String rootDir) {
		
		String now = getDateTime();
		String nowTimeStamp = DateUtil.shortDateTimeFormat3.print(DateTimeUtils.currentTimeMillis());
			try {
				TypeMatcher dataType = parent.getTypeMatcher(payloadLine);
				
				// appending to the last type
				if (dataType == null && lastType != null) {
					dataType = lastType;
					lastType.parts = lastParts;
				} else {
					// else we have a new Type - so its a new event sequence.
					// check the lastType and flush NL it if we need to...
					if (lastType != null) {
						lastType.queuer.append(lastType.getRecordDelimiter());
						lastType.queuer.flushMLine(lastType);
					}
					
					if (dataType != null && dataType.isMLine()) {
						lastType = dataType;
						lastParts = dataType.parts;
					} else {
						lastType = null;
						lastParts = null;
					}
				}
				if (dataType == null) dataType = new TypeMatcher();
				
				String remoteRootDir = mkRootDirString(rootDir, dataType.parts, hostPath);
				
				String destinationFile = String.format(parent.FORMAT_SS_LOG, remoteRootDir, dataType.type, now);
				
				if (!fileQueue.containsKey(destinationFile)) {
					fileQueue.put(destinationFile, fqFactory.create(sourceHost, destinationFile, dataType.isMLine(), dataType.parts, now));
				}

				String updatedPayload = timestampPayload(payloadLine, dataType.isTimeStamp(), nowTimeStamp);
				
				FileQueuer queuer = fileQueue.get(destinationFile);
                String token = Meter.extractToken(payloadLine);
                String tag = Meter.extractTag(payloadLine);
                queuer.setTokenAndTag(sourceHost, token, tag);
				queuer.append(updatedPayload);
				//queuers.add(queuer);
				dataType.queuer = queuer;
				
				
				
			} catch (Throwable t) {
				LOGGER.warn(lastType + " ex:" + t.toString(), t);
			}
	}
	
	protected String getDateTime() {
		return DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis());
	}
	String mkRootDirString(String rootDir, String typeInfo[], String hostPath) {
		StringBuilder sb = new StringBuilder();

//        StringBuilder sb = new StringBuilder();
		sb.append(SLASH);
//		sb.append(hostPath).append(SLASH);
		sb.append(rootDir).append(SLASH);
		for (String item : typeInfo) {
			sb.append(item);
			sb.append(SLASH);
		} 
		String replaceAll = sb.toString().replaceAll(":", "_");
		byte[] bytes = replaceAll.getBytes();
		StringBuilder result = new StringBuilder();
		for (byte b : bytes) {
			if (b == '/' || Character.isDigit(b) || Character.isLetter(b) || b == '.' || b == '-') {
				result.append((char)b);
			} else {
				result.append("_");
			}
		}
		return result.toString();
	}
	private String timestampPayload(String payload, boolean isTimeStamping, String nowDateTime) {
		if (isTimeStamping) {
			StringBuilder sb = new StringBuilder(nowDateTime);
			sb.append(SPACE);
			sb.append(payload);
//			sb.append(payload.replaceAll(NL, NL_W_SPACE).trim() + eol);
			//if (!payload.endsWith(eol) && !payload.endsWith(NLINE)) sb.append(eol);
			return sb.toString();
		}
//		if (!payload.endsWith(eol) && !payload.endsWith(NLINE))  return payload +  eol;
		return payload;
	}

	public void stop() {
        if (scheduledFuture != null) scheduledFuture.cancel(false);
		Collection<FileQueuer> values = this.fileQueue.values();
		for (FileQueuer fileQueuer : values) {
			fileQueuer.flush();
		}
		
		
	}
}
