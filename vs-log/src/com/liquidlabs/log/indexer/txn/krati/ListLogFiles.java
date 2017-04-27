package com.liquidlabs.log.indexer.txn.krati;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.indexer.krati.KratiFileStore;
import com.liquidlabs.transport.serialization.Convertor;
import jregex.Matcher;
import jregex.Pattern;
import jregex.WildcardPattern;
import krati.store.DataStore;
import krati.util.IndexedIterator;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.*;

public class ListLogFiles {
	

	private static final Logger LOGGER = Logger.getLogger(ListLogFiles.class);
	private static final int MaxLimit = LogProperties.getMaxFileList();

	private DataStore<byte[], byte[]> store;
	private List<LogFile> results = new ArrayList<LogFile>();
	private List<String> resultsString = new ArrayList<String>();
	private String filter[];
	private int limit  =-1;
	private long startTimeMs;
	private long endTimeMs;
	private boolean regexpMode = false;
	private String fileFilterRegExp[];
	private boolean sortByTime;

	private LogFileOps.FilterCallback callback;

	public ListLogFiles(DataStore<byte[], byte[]> store, String[] filter, int limit) {
		this.store = store;
		this.filter = filter;
		this.limit = limit;
		this.sortByTime = false;
		if (isRegExpFilter(filter)) {
			regexpMode = true;
			this.fileFilterRegExp = this.filter;
			this.endTimeMs = new DateTime().plusHours(1).getMillis();
		}
	}
	public ListLogFiles(DataStore<byte[], byte[]> store, String[] fileFiltersAndTags, int limit, long startTimeMs, long endTimeMs, boolean sortByTime, LogFileOps.FilterCallback callback) {
		this.store = store;
		this.startTimeMs = startTimeMs;
		this.endTimeMs = endTimeMs;
		this.sortByTime = sortByTime;
		this.filter = fileFiltersAndTags;
		this.limit = limit;
		this.callback = callback;
		if (isRegExpFilter(fileFiltersAndTags) ) {
			regexpMode = true;
			this.fileFilterRegExp = this.filter;
			this.endTimeMs = new DateTime().plusHours(1).getMillis();
		}
		if (limit <= 0) this.limit = MaxLimit;

	}
	private boolean isRegExpFilter(String[] filter) {
		String filterString = Arrays.toString(filter);
		return filter != null && filter.length > 0 && (filterString.contains("*"));
	}
	
	public void doWork() {
        boolean isLimited =limit > 0 && limit != Integer.MAX_VALUE;
		if (regexpMode) {
			getAllUsingRegexp();
		} else if (isLimited) {
			getAllByLimit();
		} else {
			getAll();
		}
	}
	private void getAllUsingRegexp()  {

		try {
            int limit = getHitLimit();
			List<Pattern> patterns = getIncludePatterns(fileFilterRegExp);
			List<Pattern> xPatterns = getExcludePatterns(fileFilterRegExp);
			List<String> acceptedTags = getTagsFromFileFilters(fileFilterRegExp);
			Matcher matcher = null;
			if (fileFilterRegExp.length == 1) {
				if (!fileFilterRegExp[0].contains(".*") && fileFilterRegExp[0].contains("*") ) matcher = new WildcardPattern(fileFilterRegExp[0],true).matcher();
			}

            IndexedIterator<byte[]> indexedIterator = store.keyIterator();
            while (indexedIterator.hasNext() && results.size() < limit) {
                String filename = new String(indexedIterator.next());
                if (filename.equals(KratiFileStore.FILE_SEED)) continue;
				try {
					if (isExcluded(xPatterns, filename)) continue;
					for (Pattern pattern : patterns) {
						LogFile logFile = (LogFile) Convertor.deserialize(store.get(filename.getBytes()));
						if (pattern.equals(".*") || matcher != null && matcher.matches(filename) || pattern.matches(filename) || isTagMatching(logFile.getTags(), acceptedTags) ) {
							if (logFile.isWithinTime(this.startTimeMs, this.endTimeMs)) {
								if (callback != null) {
									if (callback.accept(logFile)) results.add(logFile);
								} else results.add(logFile);
							}
						}
					}
					if (patterns.size() == 0 && matcher != null) {
						LogFile logFile =  (LogFile) Convertor.deserialize(store.get(filename.getBytes()));
						if (matcher.matches(filename) || isTagMatching(logFile.getTags(), acceptedTags) ) {
							if (logFile.isWithinTime(this.startTimeMs, this.endTimeMs)) {
								if (callback != null) {
									if (callback.accept(logFile)) results.add(logFile);
								} else results.add(logFile);
							}
						}

					}
                } catch (Exception e) {
                    LOGGER.error("e:", e);
                } finally {
				}
			}
		} finally {
		}
		fixFinalResults();
	}

    private int getHitLimit() {
        return this.limit > 0 ? this.limit : Integer.MAX_VALUE;
    }

    private boolean isTagMatching(String fileTagsToCheck, List<String> acceptedTags) {
		if (fileTagsToCheck == null) return false;
		if (acceptedTags.size() == 0) return false;
		String[] fileTagItems = fileTagsToCheck.split(",");
		for (String fileTagItem : fileTagItems) {
			if (acceptedTags.contains(fileTagItem.trim())) return true;
		}

		return false;
	}
	private List<String> getTagsFromFileFilters(String[] fileFilterRegExp2) {
		List<String> results = new ArrayList<String>();
		if (fileFilterRegExp2 == null) return results;
		for (String tagMaybe : fileFilterRegExp2) {
			if (tagMaybe.startsWith("tag:")) results.add(tagMaybe.trim().split(":")[1]);
		}
		return results;
	}
	private boolean isExcluded(List<Pattern> patterns, String filename) {
		for (Pattern xPattern : patterns) {
			if (xPattern.matches(filename)) return true;
		}
		return false;
	}
	private List<Pattern> getIncludePatterns(String[] parts) {
		ArrayList<Pattern> result = new ArrayList<Pattern>();
		for (String part : parts) {
			try {
				if (part.contains("*") && !part.contains(".*")) continue;
				if (part.startsWith("tag:")) continue;
				part = part.trim();
				if (part.startsWith("not(")) continue;
				if (part.startsWith("!")) continue;
				result.add(new Pattern(part.trim()));
			} catch (Throwable t) {
				String string = "Failed to handle:" + part;
				System.out.println(string);
				LOGGER.warn(string, t);
			}
		}
		if (parts.length  == 0) result.add(new Pattern(".*"));
		return result;
	}
	private List<Pattern> getExcludePatterns(String[] parts) {
		ArrayList<Pattern> result = new ArrayList<Pattern>();
		for (String part : parts) {
			part = part.trim();
			if (part.startsWith("not(")) {
				part = part.substring("not(".length(), part.length()-1);
				result.add(new Pattern(part));
			}
			if (part.startsWith("!")) {
				part = part.substring("!".length(), part.length());
				result.add(new Pattern(part));
			}

		}
		return result;
	}
	private void getAllByLimit() {
		int count1 = 0;
		int count2 = 0;
		int count3 = 0;
		LogFile accurateMatch = null;
		int givenLimit = limit;
		if (givenLimit == 1) limit = 100 * 1024;
		try {
			List<String> acceptedTags = getTagsFromFileFilters(filter);

            IndexedIterator<byte[]> indexedIterator = store.keyIterator();
            while (indexedIterator.hasNext() && results.size() < limit) {
                byte[] key = indexedIterator.next();
                String filename = new String(key);
                if (filename.equalsIgnoreCase(KratiFileStore.FILE_SEED)) continue;

                LogFile logFile = (LogFile) Convertor.deserialize(store.get(key));


				count1++;
				if (filter != null) {
					count2++;

					if (isFilterMatch(filename, filter) || isTagMatching(logFile.getTags(), acceptedTags)) {
						count3++;

						// we have been asked to match 1 item - so go for the first accurate match the 'equals' it properly
						if (givenLimit == 1 && filter.length == 1 && filename.equalsIgnoreCase(filter[0])) {
							accurateMatch = logFile;
							results.add(logFile);
							// short circuit
							limit = results.size();
							continue;
						}
						if (startTimeMs > 0) {
							if (logFile.isWithinTime(this.startTimeMs, this.endTimeMs)) {
								if (callback != null) {
									if (callback.accept(logFile)) results.add(logFile);
								} else results.add(logFile);
							}
						} else {
							if (callback != null) {
								if (callback.accept(logFile)) results.add(logFile);
							} else results.add(logFile);
						}
					}
				}
				else {
					if (callback != null) {
						if (callback.accept(logFile)) results.add(logFile);
					} else {
						results.add(logFile);
					}
				}
			}

        } catch (Exception e) {
            LOGGER.error(e);
        } finally {
		}
		if (givenLimit == 1 && accurateMatch != null) {
			results = new ArrayList<LogFile>();
			results.add(accurateMatch);
			resultsString.add(accurateMatch.getFileName());
		} else {
			fixFinalResults();
		}

		//LOGGER.info(String.format("ListFiles Count:%d %d %d filter:%s", count1, count2, count3, Arrays.toString(filter)));
	}
	private void fixFinalResults() {
		if (sortByTime) {
			Collections.sort(results, new Comparator<LogFile>(){
				public int compare(LogFile o1, LogFile o2) {
					return Long.valueOf(o2.getEndTime()).compareTo(o1.getEndTime());
				}
			});
		}
		if (results.size() > limit && limit != -1) results = results.subList(0, limit);
		for (LogFile logFile : results) {
			resultsString.add(logFile.getFileName());
		}
	}
	private boolean isFilterMatch(String file, String[] filters) {
		for (String filter : filters) {
			if (filter.length() == 0 || StringUtil.containsIgnoreCase(file, filter)) return true;
		}
		return false;
	}
	private void getAll() {


            int limit = getHitLimit();
            IndexedIterator<byte[]> indexedIterator = store.keyIterator();
            while (indexedIterator.hasNext() && results.size() < limit) {
                byte[] key = indexedIterator.next();
                String filename = new String(key);
                if (filename.equalsIgnoreCase(KratiFileStore.FILE_SEED)) continue;

                try {

                    LogFile logFile = (LogFile) Convertor.deserialize(store.get(key));
                    if (startTimeMs > 0) {
                        if (logFile.isWithinTime(this.startTimeMs, this.endTimeMs)) {
                            if (callback != null) {
                                if (callback.accept(logFile)) {
                                    results.add(logFile);
                                    resultsString.add(filename);
                                }
                            } else {
                                results.add(logFile);
                                resultsString.add(filename);
                            }
                        }
                    } else {
                        if (callback != null) {
                            if (callback.accept(logFile)) {
                                results.add(logFile);
                                resultsString.add(filename);
                            }
                        } else {
                            results.add(logFile);
                            resultsString.add(filename);
                        }                    }
                } catch (Exception e) {
                    LOGGER.error("Key" + filename, e);
                }
            }
	}
	
	public List<LogFile> getResults() {
		return results;
	}
	public void setFilter(LogFileOps.FilterCallback callback2) {
		this.callback = callback2;
	}
}
