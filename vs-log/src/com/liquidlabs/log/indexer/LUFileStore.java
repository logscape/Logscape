package com.liquidlabs.log.indexer;

import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * http://lucene.apache.org/core/4_6_1/
 http://lucene.apache.org/core/4_6_1/demo/src-html/org/apache/lucene/demo/IndexFiles.html
 Q: concurrent access and commit behaviour?

 */
public class LUFileStore  implements LogFileOps {
    private static final Logger LOGGER = Logger.getLogger(LUFileStore.class);

    public final static String FILE_SEED = "FILE_SEED_KEY";
    public static final double TARGET_MIN_STALE_SEC = Double.parseDouble(System.getProperty("lu.target.min.stale.sec","1.00"));

    volatile int nextFileId;

    private ExecutorService executor;
    protected LoggingEventMonitor eventMonitor = new LoggingEventMonitor();

    IndexWriter indexWriter = null;
    TrackingIndexWriter trackingIndexWriter = null;
    SearcherManager searchManager = null;
    ControlledRealTimeReopenThread indexSearcherReopenThread = null;
    private String indexPath;
    boolean applyAllDeletes = System.getProperty("lu.apply.deletes", "true").equals("true");
    private Directory index;


    public LUFileStore(String environment, ExecutorService executor) {
        this.executor = executor;


        try {

            this.indexWriter = getIndexWriter(environment + "/LUC-LF", true);
            initFileSeed();

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Failed to open File", e);
        }

    }

    public IndexWriter getIndexWriter(String indexPath, boolean create) throws IOException {
        if (indexWriter == null) {
            this.indexPath = indexPath;
            File path = new File(indexPath);
            path.mkdirs();
            this.index = FSDirectory.open(path);
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            this.indexWriter = new IndexWriter(index, iwc);
            this.trackingIndexWriter = new TrackingIndexWriter(this.indexWriter);


            this.searchManager = new SearcherManager(this.indexWriter, applyAllDeletes, null);

            this.indexSearcherReopenThread = new ControlledRealTimeReopenThread<IndexSearcher>(trackingIndexWriter, searchManager, 10.00, TARGET_MIN_STALE_SEC);

            this.indexSearcherReopenThread.setDaemon(true);
            this.indexSearcherReopenThread.start();
        }
        return indexWriter;
    }

    void initFileSeed() {

        IndexSearcher searcher = null;
        try {
            searcher = searchManager.acquire();
            Term term = new Term(FILE_SEED,  FILE_SEED);
            Query query = new TermQuery(term);
            TopDocs results = searcher.search(query, 1);
            if (results.totalHits == 0) {
                LOGGER.info("Creating-FileSeed:" + nextFileId);
                writeFileId();
            } else {
                ScoreDoc[] hits = results.scoreDocs;
                Document doc = searcher.doc(hits[0].doc);
                this.nextFileId = doc.getField("count").numericValue().intValue();
                LOGGER.info("FileSeed:" + nextFileId);
            }
        } catch (IOException ioex) {
            ioex.printStackTrace();
        } finally {
            release(searcher);
        }

    }
    void writeFileId() {

        try {
            Term term = new Term(FILE_SEED,  FILE_SEED);
            Query query = new TermQuery(term);
            Document doc = new Document();
            doc.add(new StringField(FILE_SEED,FILE_SEED, Field.Store.YES ));
            doc.add(new IntField("count",nextFileId, Field.Store.YES ));
            LUFileStore.this.indexWriter.updateDocument(term, doc);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }



    public int open(String file, boolean createNew, String fieldSetId, String sourceTags) {
        return openLogFile(file, createNew, fieldSetId, sourceTags).getId();
    }

    @Override
    public void addFileDeletedListener(FileDeletedListener listener) {

    }

    @Override
    public LogFile openLogFile(String file, boolean create, String fieldSetId, String sourceTags) {
        LogFile logFile = openLogFile(file);
        if (logFile != null) return logFile;
        if (create) {

            logFile = new LogFile(file, nextFileId++, fieldSetId, sourceTags);
            writeFileId();
            eventMonitor.raise(new Event("IndexerCreate").with("file", file)
                    .with("tag", logFile.getTags())
                    .with("id", logFile.getId())
            );


            try {
                updateLogFile(logFile);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return logFile;
        }

        return null;

    }

    /**
     * Return the systems known last modified date - WHEN we are up to date.
     * IF we are out of date - then return 0
     *
     * @param logFilename
     * @return
     */
    public long lastMod(String logFilename) {
        LogFile logFile = openLogFile(logFilename);
        if (logFile == null) return 0;
        // We know the file (have tailed it)
        // Tail pos is recent
        // It hasnt changed for 7 days

        // When the file changes - then this rule will fail
        File file = new File(logFilename);
        if (logFile != null && file.lastModified() < new DateTime().minusDays(7).getMillis() && logFile.getPos() > file.length() - 10 * 1024) return logFile.getEndTime();
        return 0;
    }

    @Override
    public LogFile openLogFile(String logFilename) {

        IndexSearcher searcher = null;
        try {
            if (searchManager == null) {
                System.out.println("LUCENE Search Manager is NULL - forcing process exit");
                LOGGER.fatal("LUCENE SearchManager is NULL - forcing process exit");
                LOGGER.fatal("LUCENE Check disk-space and work/agent.log for cause");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e){}
                System.exit(1);
            }
            searcher = searchManager.acquire();
            Term term = new Term("filename", logFilename);
            Query query = new TermQuery(term);
            TopDocs results = searcher.search(query, 1);
            ScoreDoc[] hits = results.scoreDocs;
            if (hits.length == 0) return null;
            Document doc = searcher.doc(hits[0].doc);
            return new LogFile(doc);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            release(searcher);

        }
    }
    public LogFile openLogFile(int logId) {

        IndexSearcher searcher = null;
        try {
            searcher = searchManager.acquire();

            Query q2 = NumericRangeQuery.newIntRange("id", 1, logId, logId, true, true);
            TopDocs results = searcher.search(q2, 1);
            if (results.totalHits == 0) return null;
            ScoreDoc[] hits = results.scoreDocs;
            Document doc = searcher.doc(hits[0].doc);
            return new LogFile(doc);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            release(searcher);
        }

    }
    // By default track all files we have ever watched
    public void removeFromIndex(String filename) {

        try{
            LOGGER.info("Remove:" + filename);
            Term term = new Term("filename", filename);
            Query query = new TermQuery(term);

            indexWriter.deleteDocuments(query);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }




    public void updateLogFile(LogFile logFile) {
        try {
            indexWriter.updateDocument(new Term("filename", logFile.getFileName()), logFile.toDocument());
            //sync();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed:" + e, e);
        }
    }



    public void updateLogFileLines(String file, List<Line> lines) {
        LogFile logFile = openLogFile(file);
        logFile.update(lines);
        updateLogFile(logFile);
    }

    public boolean isIndexed(String file) {

        IndexSearcher searcher = null;
        try {
            searcher = searchManager.acquire();
            Term term = new Term("filename", file);
            Query query = new TermQuery(term);
            TopDocs results = searcher.search(query, 1);
            ScoreDoc[] hits = results.scoreDocs;
            return (hits.length == 1);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            release(searcher);
        }
    }

    @Override
    public void add(String file, int line, long time, long pos) {
        LogFile logFile = openLogFile(file);
        if (logFile != null) {
            logFile.update(pos, new Line(logFile.getId(), line, time, pos));
            updateLogFile(logFile);
        }
    }

    @Override
    public String rolled(String fromFile, String toFile) {
        LogFile fromLog = openLogFile(fromFile);
        if (fromLog == null) {
            LOGGER.error("RollFailed: Source Roll LUFile not found, cannot roll:" + fromFile + " - " + toFile);
            return null;
        }
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Rolling: id:" + fromLog.getId() + ":"+fromFile + " -> " + toFile);
        LogFile rolledTo = new LogFile(toFile, fromLog);
        try {
            rolledTo.setAppendable(false);
            updateLogFile(rolledTo);
            Term term = new Term("filename", fromFile);
            Query query = new TermQuery(term);
            indexWriter.deleteDocuments(query);
            sync();
        } catch (Exception e) {
            LOGGER.error("RollError:" + fromFile,e);
        }
        return toFile;
    }


    public void indexedFiles(FilterCallback callback) {
        indexedFiles(0, System.currentTimeMillis(), false, callback);
    }
    public List<LogFile> indexedFiles(final long startTimeMs, final long endTimeMs, boolean sortByTime, final FilterCallback callback) {

        IndexSearcher searcher = null;
        try {
            searchManager.maybeRefresh();
            searcher = searchManager.acquire();

            final List<LogFile> results = new ArrayList<LogFile>();

            final List<Integer> docIds = new ArrayList<Integer>();

            Query startQuery = NumericRangeQuery.newLongRange("startMs", 0l, endTimeMs, true, true);
            Query endQuery = NumericRangeQuery.newLongRange("endMs", startTimeMs, Long.MAX_VALUE, true, true);
            BooleanQuery and = new BooleanQuery();
            and.add(startQuery, BooleanClause.Occur.MUST);
            and.add(endQuery, BooleanClause.Occur.MUST);

            searcher.search(and, new Collector() {
                private int base;
                @Override
                public void setScorer(Scorer scorer) throws IOException {
                }

                @Override
                public void collect(int doc) throws IOException {
                    IndexSearcher searcher = searchManager.acquire();
                    try {
                        doc += base;

//                        Document doc1 = searcher.doc(doc);
//                        if (doc1.getField("filename") == null) return;
//                        LogFile file = new LogFile(doc1);
                        docIds.add(doc);
                        // CANNOT callback on search head within here as it stuffs everything up by syncinh the indexWriter on an update
//                        if (file.isWithinTime(startTimeMs, endTimeMs) && callback.accept(file)) {
//                            results.add(file);
//                        }
                    } catch (Throwable t) {
                        LOGGER.warn("Ex: doc:" + doc + " base:" + base + " docId:" + (doc - base), t);
                    } finally {
                        searchManager.release(searcher);
                    }
                }

                @Override
                public void setNextReader(AtomicReaderContext context) throws IOException {
                    this.base = context.docBase;
                }

                @Override
                public boolean acceptsDocsOutOfOrder() {
                    return true;
                }
            });
//            IndexReader reader =  DirectoryReader.open(index);
//            IndexSearcher newSeacher = new IndexSearcher(reader);
            //searcher = searchManager.acquire();

            for (Integer logId : docIds) {
                try {
                    LogFile logFile = new LogFile(searcher.doc(logId));
                    if (callback.accept(logFile))  results.add(logFile);
                } catch (Throwable t) {
                    LOGGER.warn("Ex: doc:" + logId, t);

                }
            }
            //reader.close();

            if (sortByTime) {
                Collections.sort(results, new Comparator<LogFile>() {
                    @Override
                    public int compare(LogFile o1, LogFile o2) {
                        return Long.valueOf(o2.getEndTime()).compareTo(o1.getEndTime());
                    }
                });
            }

            return results;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            release(searcher);
        }
    }

    public void close() {
        try {
            this.indexWriter.close();
            this.indexSearcherReopenThread.interrupt();
            searchManager.close();
        } catch (Exception e) {
            LOGGER.error("Close error", e);
        }
    }


    public List<DateTime> getStartAndEndTimes(String filename) {
        LogFile file = openLogFile(filename);
        return file.getStartEndTimes();
    }

    public long[] getLastPosAndLastLine(String filename) {
        LogFile file = openLogFile(filename);
        return new long[] { file.getPos(), file.getLineCount()};
    }

    public boolean assignFieldSetToLogFile(String filename, String fieldSetId) {
        LogFile file = openLogFile(filename);
        file.setFieldSetId(fieldSetId);
        updateLogFile(file);
        return true;
    }


    public void sync() {
        try {
            doCommit();
        } catch (Exception e) {
            LOGGER.error("Sync error", e);
        }
    }

    synchronized private void doCommit() {
        try {
            searchManager.maybeRefresh();
            indexWriter.commit();
        } catch (AlreadyClosedException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void release(IndexSearcher indexSearcher) {
        try {
            if (indexSearcher != null) searchManager.release(indexSearcher);
        } catch (IOException ioe){
            throw new RuntimeException(ioe);
        }
    }
}
