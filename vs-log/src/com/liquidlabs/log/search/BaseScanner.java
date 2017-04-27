package com.liquidlabs.log.search;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.fields.field.LiteralField;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.search.handlers.LogScanningHandler;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.util.DateTimeExtractor;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

abstract public class BaseScanner implements Scanner {
	private static final Logger LOGGER = Logger.getLogger(BaseScanner.class);
	
	final protected Indexer indexer;
	protected LogScanningHandler next;
    private final String sourceUri;

    protected AtomicLong scannedBytes = new AtomicLong();
    protected AtomicLong scannedEvents = new AtomicLong();
    protected volatile long totalEvents;
    protected volatile int hits = 0;
	
	protected AtomicInteger sent = new AtomicInteger();


	private static String agentType = VSOProperties.getResourceType();


    private String filename;
    final protected String path;

    int hostnameHash = 0;
    private String hostname;


    private LogFile logFile;
    protected boolean finished = false;


    public BaseScanner(Indexer indexer, String path, LogScanningHandler handler, String fileTags, String sourceUri) {
		this.indexer = indexer;
		this.path = path;
		this.next = handler;
        this.sourceUri = sourceUri + path;
        this.logFile = indexer.openLogFile(path);
        this.totalEvents = this.logFile.getLineCount();
        String hostname = logFile.getFileHost(NetworkUtils.getHostname());
        this.hostnameHash = MurmurHash3.hashString(hostname, 12);

	}
	final public String getFilename() {
		return this.path;
	}

	final protected boolean scan(final LogRequest request, final FieldSet fieldSet, final String rawLineData, final int lineNumber, final Query query, final long bucketTime, final HistoEvent event, final long fileStartTime, final long fileEndTime) {

        try {

            MatchResult matchResult = query.isMatching(rawLineData);
            if (matchResult.isMatch()) {

                if (hostname == null) {
                    hostname = logFile.getFileHost(NetworkUtils.getHostname());
                }
                if (filename == null) {
                    filename = logFile.getFileNameOnly();
                }

                String[] fields = fieldSet.getFields(rawLineData, logFile.getId(), lineNumber, bucketTime);
                if (fields.length > 0 && query.isPassedByFilters(fieldSet, fields, rawLineData, matchResult, lineNumber)) {

                    if (!query.containsFields(fieldSet)) {
                        return false;

                    }
                    fieldSet.setDefaultField(FieldSet.DEF_FIELDS._size, Integer.toString(rawLineData.length() + 1));
                    LiteralField field = (LiteralField) fieldSet.getField(FieldSet.DEF_FIELDS._timestamp.name());
                    long eventTime = bucketTime;
                    if (field != null) {
                        eventTime = LogProperties.fromSecToMs((int) field.longValue());
                    }

                    next.handle(fieldSet, fields, query, event, bucketTime, lineNumber, fileStartTime, fileEndTime, eventTime, rawLineData, matchResult, request.getStartTimeMs(), request.getEndTimeMs(), request.getBucketWidthSecs() * DateUtil.SECOND);
                    return true;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error Processing File:" + this.filename + ":" + lineNumber + "\n\t [=== RAW_LINE: " + rawLineData + "===]", e);
        }
        return false;
    }
    final protected boolean isFinished(LogRequest request, long lineTime, List<HistoEvent> histos) {
        long histoBucketTime = -1;
        if (histos != null) {
            histoBucketTime = histos.get(0).getBucketTime(lineTime);
        }
        boolean hitLimitHit = (histos != null) ? request.isHitLimitDone(hostnameHash + histoBucketTime) : false;

        finished = hitLimitHit || request.isExpired() || request.isCancelled()    ;
        return finished;
    }
	
	public FieldSet getFieldSet(String fieldSetId, LogFile logFile) {
		FieldSet fieldSet = indexer.getFieldSet(fieldSetId);
		if (fieldSet == null) {
            //throw new RuntimeException("Failed to load Type:" + fieldSetId);
            LOGGER.warn("Failed to load Type:" + fieldSetId + " Reverting to 'basic'");
            fieldSet = FieldSets.getBasicFieldSet();
        }
        fieldSet = fieldSet.copy();

        String hostname = logFile.getFileHost(NetworkUtils.getHostname());
        String resource = sourceUri + "?&host=" + hostname + "&mod=" + logFile.getEndTime()+"&length=" + logFile.getPos() + "&type=" + logFile.getFieldSetId() + "&";
		fieldSet.addDefaultFields(fieldSet.getId(), hostname, logFile.getFileNameOnly(), this.path, logFile.getTags(), agentType, resource, 0, true);

        fieldSet.enhance();
        return fieldSet;
	}

    public LogFile getLogFile() {
        return this.logFile;
    }

    public boolean isFinished() {
        return finished;
    }

    public long eventsComplete() {
        return scannedEvents.get();
    }

    @Override
    public String toString() {
        return "BaseScanner{" +
                "scannedBytes=" + scannedBytes +
                ", scannedEvents=" + scannedEvents +
                ", totalEvents=" + totalEvents +
                ", hits=" + hits +
                ", sent=" + sent +
                ", filename='" + filename + '\'' +
                ", path='" + path + '\'' +
                ", logFile=" + logFile +
                ", finished=" + finished +
                '}';
    }
}
