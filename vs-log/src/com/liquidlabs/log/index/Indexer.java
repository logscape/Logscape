package com.liquidlabs.log.index;

import java.util.List;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.indexer.LineStore;


public interface Indexer extends LogFileOps {

	static final String DB_SCHEMA_ID = "/../db.schema.id";
	static String SCHEMA_VERSION="3.23-_PIIndex_SEP_09_2016";



	void stallIndexingForSearch();

    long filePositionForLine(String file, long line);

    int size();

	LineStore lineStore();

	public interface actionListener {
		void fileRemoved(int logId, String filename);
		void filesRemoved(List<LogFile> logFiles);
	}


    public void removeFromIndex(String dirName, String filePattern, boolean recurseDirectory);

	int removeFromIndex(List<LogFile> logFiles);

    boolean isStalling();

	void update(String filename, Line line);

	void add(String file, List<Line> lines);

    void removeFromIndex(String file);

	void close();
	
	List<Bucket> find(String file, long startTime, long endTime);

	List<Line> linesForNumbers(String file, int startLine, int endLine);

	List<Line> linesForTime(String file, long time, int pageSize);

	/**
	 * Remove those files that dont exist any more
	 * @return Return the amount of data removed
	 */
	long cleanupMissingIndexedFiles();

	FieldSet getFieldSet(String fieldSetId);

	void addFieldSet(FieldSet fieldSet);
	List<FieldSet> getFieldSets(Filter<FieldSet> filter);
	void removeFieldSet(FieldSet data);

	interface Filter<T> {
		boolean accept(T thing);
	}
	
	public static class AlwaysFilter implements Filter {

		@Override
		public boolean accept(Object thing) {
			return true;
		}
		
	}
	IndexStats indexStats();
	IndexStats getLastStats();
    long getLineStoreSize();


}
