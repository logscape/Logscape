/**
 * 
 */
package com.liquidlabs.log.search;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.search.filters.Filter;
import com.liquidlabs.log.search.functions.Function;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.orm.Id;
import com.liquidlabs.transport.serialization.Convertor;
import javolution.util.FastMap;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.model.constraints.NotNull;
import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


@SuppressWarnings("unchecked")
public class Bucket implements KryoSerializable,  net.openhft.lang.io.serialization.BytesMarshallable, Serializable {
	public static String HIT_SPLIT = "!";
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = Logger.getLogger(Bucket.class);
	
	private transient boolean hasLoggedWARN = false;
	
	@Id
	protected String id = "";
	
	private long start;
	private long end;
	private transient String filePath;

	private Set<String> fieldSetId = new ConcurrentHashSet<>();
	
	
	private int hits=0;

	private short queryPos;
	protected String pattern = "";
	private Map<String, Map> functionResults = new ConcurrentHashMap<String, Map>();
	

	protected Map<String, Function> functions = new ConcurrentHashMap<String, Function>();

	private String sourceURI = "";
	public String subscriber = "";
	private int eventsTotal;

	transient private int aggdCount=0;
	
	public Bucket() {	}
	
	public Bucket(long start, long end, List<Function> functions, int queryPos, String pattern, String sourceURI, String subscriberId, String filePath) {
		this.start = start;
		this.end = end;
		this.filePath = filePath;
		if (functions != null) {
            for (Function function : functions) {
                this.functions.put(function.toStringId(), function);
            }
        }
		this.queryPos = (short) queryPos;
		if (pattern != null) this.pattern = pattern;
		if (sourceURI != null) this.sourceURI = sourceURI;
		if (subscriber != null) this.subscriber = subscriberId;
		this.id = UID.getUUID();
	}

	public int hits() {
		return hits;
	}
	
	
	final public long getStart() {
		return start;
	}
	
	public long getEnd() {
		return end;
	}
	
	public int getQueryPos() {
		return queryPos;
	}
	
	public String getPattern() {
		return pattern;
	}
	
	public void increment() {
		hits++;
	}
	
	public String getSourceURI() {
		return sourceURI;
	}
	
	public String subscriber() {
		return subscriber;
	}
	
	public Set<String> getAggregateResultKeys(){
		if (functionResults == null) return new ConcurrentHashSet<>();
		return functionResults.keySet();
	}
	public Map<String, Map> getAggregateResults(){
		return functionResults;
	}
	
	public Map getAggregateResult(String functionStringId, boolean verbose) {
		if (functionResults == null || functionResults.size() == 0) {
			convertFuncResults(verbose);
		}
		return functionResults.get(functionStringId);
	}

	public void increment(FieldSet fieldSet, String[] fields, String filenameOnly, String filename, long time, long fileStartTime, long fileEndTime, int lineNumber, String lineData, MatchResult matchResult, boolean isSummaryRequired, long requestStartMs, long requestEndMs) {

        try {
            short exec = 0;
            for (Function function : functions.values()) {
                try {
                    if (filter(fieldSet, fields, function.filters(), lineData, matchResult, lineNumber)) {
                        function.execute(fieldSet, fields, pattern, time, matchResult, lineData, requestStartMs, lineNumber);
                        exec++;
                    }
                } catch (Throwable t) {
    // Dont log an exception cause it can kill the machine on massive searches - and fuck it all up
                    if (!hasLoggedWARN) {
//							t.printStackTrace();
                        hasLoggedWARN = true;
                        LOGGER.error(String.format("%s: sub:%s q:%s \n\tFailed to handle:%s ex:%s file:%s line:%d sub:%s ", id, subscriber, pattern, function.toString(), t.toString(), filename, lineNumber, subscriber), t);
                    }
                }
            }
            if (functions.isEmpty()) exec++;


            // hit is used to determine propogate - only pump buckets with events?
            if (exec > 0) hits++;

            this.fieldSetId.add(fieldSet.getId());

            return;

        } catch (ConcurrentModificationException ex) {
            if (!hasLoggedWARN) {
                LOGGER.error(String.format("%s: sub:%s q:%s \n\tFailed to handle:%s ex:%s file:%s line:%d sub:%s ", id, subscriber, pattern, "", ex.toString(), filename, lineNumber, subscriber), ex);
            }
        } catch (Throwable t) {
            if (!hasLoggedWARN) {
                LOGGER.error(String.format("%s: sub:%s q:%s \n\tFailed to handle:%s ex:%s file:%s line:%d sub:%s ", id, subscriber, pattern, "", t.toString(), filename, lineNumber, subscriber), t);
            }
        }
	}
    public void increment(MatchResult matchResult, Map<String, Object> fieldSet, LogRequest request) {
        for (Function function : functions.values()) {
            // TODO: sortout numeric fields.... i.e. if applyTo or groupBy are nuermic or contain "+" concat operators
            Object   applyO =  fieldSet.get(function.getApplyToField());
            Object   groupO =  fieldSet.get(function.groupByGroup());
            String apply = applyO != null ? applyO.toString() : "";
            String group = groupO != null ? groupO.toString() : "";
            function.execute(apply, group);
        }
        hits++;
    }


    final public boolean filter(FieldSet fieldSet, String[] fields, List<Filter> filters, String lineData, MatchResult matchResult, int lineNumber) {
		if (filters == null || filters.isEmpty()) {
			return true;
		}
		
		for (Filter filter : filters) {
			if (!filter.isPassed(fieldSet, fields, lineData, matchResult, lineNumber)) {
				return false;
			}
		}
		return true;
	}


	public int convertFuncResults(boolean verbose) {
		int nonBucketLevel = 0;
        if (functionResults == null) functionResults = new ConcurrentHashMap<>();
		for (Function function : functions.values()) {
			try {			
				if (!function.isBucketLevel()) nonBucketLevel++;
                Map results = function.getResults();
				functionResults.put(function.toStringId(), results);
				hits += results.size();
			}catch(NullPointerException t) {
				LOGGER.warn("LOGGER - Bucket null pointered on convert results. Zero Bucket Hits? hits = " + hits);
				LOGGER.warn("LOGGER -func:" + function + " tag:" + function.getTag());
			}
		}
//		LOGGER.warn(String.format("%s hits[%d] map[%s] %s=>%s", getId(), hits, this.myMap, format.format(new Date(start)), format.format(new Date(end))));
		if (verbose && hits > 0) {
			LOGGER.info(String.format("LOGGER - Bucket %s P[%s] hitCount:%d Results:%s", toStringTime(), pattern, hits, functionResults.keySet()));
		}
		return nonBucketLevel;
	}
	public void setFunctionResults(Map<String, Map> functionResults) {
		this.functionResults = functionResults;
	}

	public List<Function> functions() {
        if (functions != null) {
		    return new ArrayList<Function>(functions.values());
        }
        return new ArrayList<Function>();
	}

	public void increment(int hits) {
		this.hits += hits;
	}

	public String toString() {
		return getClass().getSimpleName() + "." + this.id +  " qpos:" + queryPos + " p:" + getPattern() + " sub:" + subscriber() + " hits:" + hits  +  toStringTime();
	}
	public String toString2() {
		return getClass().getSimpleName() + " qpos:" + queryPos + " p:" + getPattern() + " sub:" + subscriber() + " hits:" + hits  + " " + toStringTime();
	}

	public void incrementAggCount() {
		this.aggdCount++;
	}

	public int getAggdCount() {
		return aggdCount;
	}

	public String toStringTime() {
		DateTimeFormatter formatter = DateUtil.shortDateTimeFormat6;
		return String.format("[%s - %s]", formatter.print(start), formatter.print(end));
	}

	public String id() {
		return id;
	}

	public void incrementScanned(int amount) {
		this.eventsTotal += amount;
	}

	public int totalScanned() {
		return this.eventsTotal;
	}

	public boolean isWithinTime(long time) {
		return (time >= start && time < end);
	}

	public Set<String> getFieldSetId() {
		return fieldSetId;
	}

	public void setFieldSetId(Set<String> fieldSetId2) {
		this.fieldSetId.addAll(fieldSetId2);
	}

	public void aggregate(Bucket sourceBucket) {
		for (Function sourceBucketFunction : sourceBucket.functions()) {
			if (!this.functions.containsKey(sourceBucketFunction.toStringId())) {
				Function function = sourceBucketFunction.create();
				this.functions.put(function.toStringId(), function);
			}
		}

		this.setFieldSetId(sourceBucket.getFieldSetId());
		this.increment(sourceBucket.hits());
		this.incrementAggCount();
		this.incrementScanned(sourceBucket.totalScanned());
	}
	
//	private Set<String> hostnames;
//	private Set<String> filenames;


	public void resetCounts() {
		this.aggdCount = 0;
		this.hits = 0;
		this.eventsTotal = 0;
		
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setSourceURI(String sourceURI) {
		this.sourceURI = sourceURI;
	}

	public void cache(FastMap<String, String> cachedStringReferences) {
		this.setPattern(cache(this.getPattern(), cachedStringReferences));
		this.setSourceURI(cache(this.getSourceURI(), cachedStringReferences));
	}
	private String cache(String item, FastMap<String, String> cachedStringReferences) {
		if (item == null) return null;
		String foundIt = cachedStringReferences.get(item);
		if (foundIt != null) return foundIt; 
		cachedStringReferences.put(item, item);
		return item;
	}

	public void resetAll() {
		resetCounts();
        resetFunctions();
        resetResults();
	}

    protected void resetFunctions() {
        for (Function func : this.functions.values()) {
            func.reset();
        }
    }

    public void resetResults() {
        this.functionResults = new ConcurrentHashMap<>();
    }

    public void setTimes(long start, long end) {
		this.start = start;
		this.end = end;
	}
    public void adjust(long ms) {
        this.start += ms;
        this.end += ms;
    }

    public Bucket copy() {
        return (Bucket) com.liquidlabs.transport.serialization.Convertor.clone(this);
    }

    public void setFunctions(Map<String, Function> functions) {
        this.functions = functions;
    }

    public void write(Kryo kryo, Output out) {
        kryo.writeObject(out, this.id);
        kryo.writeObject(out, this.subscriber);
        kryo.writeObject(out, this.start);
        kryo.writeObject(out, this.end);
        kryo.writeObject(out, this.fieldSetId);
        kryo.writeObject(out, this.hits);
        kryo.writeObject(out, this.queryPos);
        kryo.writeObject(out, this.aggdCount);
        kryo.writeObject(out, this.eventsTotal);
        kryo.writeObject(out, this.sourceURI);
        kryo.writeObject(out, this.pattern);
        kryo.writeObject(out, functions);
        if (functionResults == null) this.functionResults = new ConcurrentHashMap<String, Map>();
        kryo.writeObject(out, this.functionResults);
    }


    public void read(Kryo kryo, Input in) {
        this.id = kryo.readObject(in, String.class);
        this.subscriber = kryo.readObject(in, String.class);
        this.start = kryo.readObject(in, long.class);
        this.end = kryo.readObject(in, long.class);
        this.fieldSetId = kryo.readObject(in, HashSet.class);
        this.hits = kryo.readObject(in, int.class);
        this.queryPos = kryo.readObject(in, short.class);
        this.aggdCount = kryo.readObject(in, int.class);
        this.eventsTotal = kryo.readObject(in, int.class);
        this.sourceURI = kryo.readObject(in, String.class);
        this.pattern = kryo.readObject(in, String.class);
        this.functions = kryo.readObject(in, ConcurrentHashMap.class);
        this.functionResults = kryo.readObject(in, ConcurrentHashMap.class);
    }


    @Override
    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        try {
            this.id = in.readUTF();
            this.subscriber = in.readUTF();
            this.start = in.readLong();
            this.end = in.readLong();
            this.fieldSetId = (Set<String>) readObject(in);
//            this.fieldSetId = (Set<String>) in.readObject();
            this.hits = in.readInt();
            this.queryPos = in.readShort();
            this.aggdCount = in.readInt();
            this.eventsTotal =  in.readInt();
            this.sourceURI =  in.readUTF();
            this.pattern =  in.readUTF();

//            this.functions = (Map<String, Function>) Convertor.deserialize(getBytes(in));
                    this.functions = (Map<String, Function>) readObject(in);

//            this.functionResults = (Map<String, Map>) Convertor.deserialize(getBytes(in));
//            this.functionResults = (Map<String, Map>) in.readObject();

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private Object readObject(Bytes in) throws IOException, ClassNotFoundException {
        return Convertor.deserialize(getBytes(in));
    }

    private byte[] getBytes(Bytes in) {
        byte[] raw = new byte[in.readInt()];
        in.read(raw);
        return raw;
    }

    @Override
    public void writeMarshallable(@NotNull Bytes out) {
        try {
            out.writeUTF(this.id);
            out.writeUTF(this.subscriber);
            out.writeLong(this.start);
            out.writeLong(this.end);

            writeObject(out, this.fieldSetId);
//            out.writeObject(this.fieldSetId);

            out.writeInt(this.hits);
            out.writeShort(this.queryPos);
            out.writeInt(this.aggdCount);
            out.writeInt(this.eventsTotal);
            out.writeUTF(this.sourceURI);
            out.writeUTF(this.pattern);

//            out.writeObject(functions);
            writeObject(out, functions);

//            out.writeObject(this.functionResults);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private void writeObject(Bytes out, Object obj) throws IOException {
        byte[] toWrite2 = Convertor.serialize(obj);
        out.writeInt(toWrite2.length);
        out.write(toWrite2);
    }

	public CharSequence getFilePath() {
		return filePath;
	}
}