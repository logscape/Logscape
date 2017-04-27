package com.liquidlabs.log.index;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.liquidlabs.common.MurmurHash3;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.indexer.LineStore;
import org.joda.time.DateTime;

import com.liquidlabs.log.fields.FieldSet;



// Ok, i know, this is gonna grow to be massive and needs to have overflow to disk argghh!
public class InMemoryIndexer implements Indexer{
	
	public InMemoryIndexer() {
		fieldSets.put(FieldSets.get().getId(), FieldSets.get());
		fieldSets.put(FieldSets.getBasicFieldSet().getId(), FieldSets.getBasicFieldSet());
	}

    private final Map<String, TreeMap<Long, List<Line>>> index = new ConcurrentHashMap<String, TreeMap<Long, List<Line>>>();
	private final Map<String, FieldSet> fieldSets = new ConcurrentHashMap<String, FieldSet>();
	private final Map<String, LogFile> files = new ConcurrentHashMap<String, LogFile>();

	public Map<String, TreeMap<Long, List<Line>>> getIndex() {
		return index;
	}
	public boolean fileExists(final String filename) {
		return true;
	}
	public void addFieldSet(final FieldSet fieldSet) {
		this.fieldSets.put(fieldSet.getId(), fieldSet);
	}
	public void removeFieldSet(final FieldSet data) {
	}
	public void update(final String filename, final Line line) {
		files.get(filename);
		
	}
	
	// file -> time
	// time -> line_number
	public void add(final String file, final int line, final long time, final long pos) {
		TreeMap<Long, List<Line>> lines = index.get(file);
		if (lines == null) {
			lines = new TreeMap<Long, List<Line>>();
			index.put(file, lines);
		}
		List<Line> list = lines.get(time);
		if (list == null) {
			list = new ArrayList<Line>();
			lines.put(time, list);
		}
		list.add(new Line(Math.abs(file.hashCode()), line, time, pos));
	}

	public List<Line> getLines(final String file) {
		final TreeMap<Long, List<Line>> treeMap = index.get(file);
		final List<Line> all = new ArrayList<Line>();
		for(final List<Line> lines: treeMap.values()) {
			all.addAll(lines);
		}
		return all;
	}

	@Override
	public void addFileDeletedListener(FileDeletedListener listener) {

	}

	public LogFile openLogFile(final String file, final boolean create, final String fieldSetId, final String sourceTags) {
        final LogFile logFile = new LogFile(file, Math.abs(MurmurHash3.hashString(file,12)), fieldSetId, sourceTags);
		files.put(file, logFile);
		return logFile;
	}
	public LogFile openLogFile(final String logId) {
		return files.get(logId);
	}
    public long getLineStoreSize() {
        return 100;//this.lineStore.size();
    }

    @Override
    public LogFile openLogFile(int logId) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public FieldSet getFieldSet(final String fieldSetId) {
		return fieldSets.get(fieldSetId);
	}
	public List<FieldSet> getFieldSets(Filter<FieldSet> filter) {
		return new ArrayList<FieldSet>(fieldSets.values());
	}
	public int open(final String file, final boolean createNew, final String fieldSetId, final String sourceTags) {
		final LogFile logFile = new LogFile(file,Math.abs(file.hashCode()), fieldSetId, sourceTags);
		files.put(file, logFile);
		return Math.abs(file.hashCode());
	}

    @Override
    public void updateLogFileLines(String file, List<Line> lines) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isIndexed(final String absolutePath) {
		return index.containsKey(absolutePath);
	}
	public void rolled(final String baseLogName, final int maxRolls) {
	}

//	public List<String> indexedFiles() {
//		return new ArrayList<String>(index.keySet());
//	}
//	public List<LogFile> indexedFiles(final String filter, final int limit) {
//		return null;//new ArrayList<String>(index.keySet());
//	}
//	public List<LogFile> indexedFiles(final String filter, final int limit,final FilterCallback callback) {
//		final List<LogFile> results = new ArrayList<LogFile>();
//		for (final LogFile logFile : this.files.values()) {
//			if (callback.accept(logFile)) results.add(logFile);
//		}
//		return results;
//	}
//	public List<LogFile> indexedFiles(final String filter, final int limit, final long startTimeMs, final long endTimeMs, final boolean sortByTime) {
//		return indexedFiles(new String[] { filter } , limit, startTimeMs, endTimeMs, sortByTime);
//	}
//	public List<LogFile> indexedFiles(final String[] fileFilterRegExp, final int limit,
//			final long startTimeMs, final long endTimeMs, final boolean sortByTime) {
//		final Set<String> keySet = index.keySet();
//		final ArrayList<LogFile> results = new ArrayList<LogFile>();
//		int id = 0;
//		for (final String file : keySet) {
//			results.add(new LogFile(file, id++, "",""));
//		}
//		return results;
//	}
	public List<LogFile> indexedFiles(final long startTimeMs, final long endTimeMs, final boolean sortByTime, final FilterCallback callback) {
        final List<LogFile> results = new ArrayList<LogFile>();
        for (final LogFile logFile : this.files.values()) {
            if (callback.accept(logFile)) results.add(logFile);
        }
        Collections.sort(results, new Comparator<LogFile>() {
            @Override
            public int compare(LogFile o1, LogFile o2) {
                return Long.valueOf(o2.getEndTime()).compareTo(o1.getEndTime());
            }
        });
		return results;
	}

    @Override
    public void indexedFiles(FilterCallback callback) {
        for (final LogFile logFile : this.files.values()) {
            callback.accept(logFile);
        }
    }

    public List<String> indexedFilesSortedByFirstTime() {
		return new ArrayList<String>(index.keySet());
	}

	public void remove(final String absolutePath) {
	}

	public IndexStats getLastStats() {
		return null;
	}
	public IndexStats indexStats() {
		return null;
	}

	public void add(final String file, final List<Line> lines) {
		for(final Line line : lines) {
			add(file, line.number(), line.time(), line.position());
		}
		
	}

    @Override
    public void removeFromIndex(String file) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public long firstIndexedLineFrom(final long fromTime, final long toTime, final String file) {
		return 0;
	}
	
	public long getFileId(final String filename) {
		return 0;
	}

	public String rolled(final String baseLogName, final String files) {
		return "";
	}

	public void removeFromIndex(final String absolutePath, final String pattern) {
	}

	public long cleanupMissingIndexedFiles() {
		return 0;
	}
	public void close() {
	}

	public List<Bucket> find(final String file, final long start, final long end) {
		final TreeMap<Long, List<Line>> treeMap = index.get(file);
		final ArrayList<Bucket> result = new ArrayList<Bucket>();
		if (treeMap == null) return result;
		for (final long time : treeMap.keySet()) {
			final Bucket bucket = new Bucket(Math.abs(file.hashCode()), time);
			final List<Line> list = treeMap.get(time);
			for (final Line line : list) {
				bucket.update(line);
			}
			result.add(bucket);
		}
		return result;
	}

	public List<Line> linesForNumbers(final String file, final int startLine, final int endLine) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<Line> linesForTime(final String file, final long time, final int pageSize) {
		// TODO Auto-generated method stub
		return null;
	}

	public Set<String> patterns(final String file) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<Line> refineLineTimesAgainstIndexData(final int logId, final String fieldSetId, final Set<String> patterns, final List<Line> lines) {
		// TODO Auto-generated method stub
		return null;
	}
	public List<DateTime> getStartAndEndTimes(final String filename) {
		// TODO Auto-generated method stub
		return null;
	}

	public Set<String> listStores() {
		return this.index.keySet();
	}
	public long[] getLastPosAndLastLine(final String filename) {
		return null;
	}

    @Override
    public long filePositionForLine(String file, long line) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int size() {
        return 0;
    }

	@Override
	public LineStore lineStore() {
		return null;
	}

	public void removeFromIndex(final String dirName, final String filePattern,
			final boolean recurseDirectory) {
	}

	public List<Bucket> bucketsBetween(final String logId, final int i,	final long currentTimeMillis) {
		return null;
	}

	public void setLogFileTimeFormat(final String logCanonical, final String timeFormat) {
	}

	public void iterateOverLines(final long fromTime, final LineHandler lineHandler) {
	}

	public void storeLines(final List<Line> compressedLines) {
	}

	public boolean assignFieldSetToLogFile(final String logFile, final String fieldSetId) {
		return false;
	}

	public int updated = 0;
	public void updateLogFile(final LogFile openLogFile) {
		this.updated++;
		this.files.put(openLogFile.fileName.toString(), openLogFile);
	}
	public void addActionListener(final actionListener actionListener) {
	}

	public int removeFromIndex(final List<LogFile> logFiles) {
		return 0;
	}

    @Override
    public boolean isStalling() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void sync() {

    }
    public void stallIndexingForSearch(){
    }

}
