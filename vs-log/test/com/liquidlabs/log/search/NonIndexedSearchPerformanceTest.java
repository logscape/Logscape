package com.liquidlabs.log.search;

import com.liquidlabs.common.UID;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.log.LogReader;
import com.liquidlabs.log.NullAggSpace;
import com.liquidlabs.log.NullLogSpace;
import com.liquidlabs.log.TailerImpl;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.search.handlers.SearchHistogramHandler;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.log.space.WatchDirectory;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NonIndexedSearchPerformanceTest {

    String dbName = "build/" + getClass().getSimpleName();

    private NullAggSpace aggSpace = new NullAggSpace();
    private NullLogSpace logSpace = new NullLogSpace();
    private File dbDir;
    private File logFile;
    private LogReader writer;
    private TailerImpl tailer;

    protected Indexer indexer;
    private List<HistoEvent> histo;

    private AtomicLong indexedDataSize = new AtomicLong();

    String LOG_FILE="test-data/liquidlabs-cloud.com-Aug-2010.gz";
//	String LOG_FILE="E:/weblogs/userlogs.log";
//	String LOG_FILE="/Volumes/Media/weblogs/rainbow.log";


    ////////// Number of iterations
    int limit = 1;
    int buckets = 10;

    boolean dwEnabled = false;

    private FieldSet fieldSet;


    @Before
    public void setUp() throws Exception {

        boolean doFullImportEtc = true;

        dbDir = new File(dbName);
        if (doFullImportEtc) FileUtil.deleteDir(dbDir);
        dbDir.mkdirs();
        logFile = new File(LOG_FILE);

        indexer = new KratiIndexer(dbName);

        doDWStuff();

        fieldSet = new FieldSet("(**)","data");
        fieldSet.id = UID.getUUID();
        indexer.addFieldSet(fieldSet);

        WatchDirectory watch = new WatchDirectory();
        watch.setMaxAge(99999);

        LogFile logFile2 = indexer.openLogFile(logFile.getAbsolutePath(), true, fieldSet.id, "");

        writer = new GenericIngester(logFile2, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);


        long[] countLines = FileUtil.countLines(logFile);

        System.out.println(String.format("%d v %d", logFile.length(), countLines[0]));

        if (doFullImportEtc) {
            tailer = new TailerImpl(logFile, 0, 1, writer, watch, indexer);
        } else {
// use this if you want to re-use the same DB - i.e. no import required (and turn off the FileUtil.deleteDir();
            tailer = new TailerImpl(logFile, logFile.length(), 0, writer, watch, indexer);
        }


        histo = new ArrayList<HistoEvent>();
    }

    protected void doDWStuff() {
    }

    @After
    public void tearDown() throws Exception {
        indexer.close();
    }


    @Test
    public void testPerformance() throws Exception {

        tailer.call();
        List<LogFile> indexedFiles = indexer.indexedFiles(0, System.currentTimeMillis(), false, new LogFileOps.FilterCallback() {
            public boolean accept(LogFile logFile) {
                return true;
            }
        });
        System.out.println("********** File:" + indexedFiles);

//		LogRequest request = getRequest(0, System.currentTimeMillis(), ".*^(\\S+) (\\S+)  (\\S+) (\\S+)\\|(\\S+)\\|(\\S+)\\|(\\S+).*");
        LogRequest request = getRequest(0, System.currentTimeMillis(), fieldSet.id, "data", "Apple");
        request.setVerbose(true);
        request.setBucketCount(buckets);

        // hitlimiters etc only get initialised when a copy is made
        Scanner searcher = createScanner(request, logFile, logSpace, indexer);

        // do the search
        long start = System.currentTimeMillis();
        int count = 0;
        while (count++ < limit) {

            long start2 = System.currentTimeMillis();
            int hits = searcher.search(request, histo, new AtomicInteger());
            long end2 = System.currentTimeMillis();
            System.out.println(String.format(">>>>>>>>>> COUNT:%d hits:%d elapsed:%dms", count, hits, (end2 - start2)));

//			for (HistoEvent event : histo) {
//				System.out.println(event);
//			}
//			Assert.assertTrue("hits:" + histo.get(0).hits(), histo.get(0).hits() > 10);
            List<Bucket> buckets2 = aggSpace.buckets();
            for (Bucket bucket : buckets2) {
                System.out.println("::::::::::Bucket:" + bucket.hits() + "/" + bucket.totalScanned());
            }
//
        }
        long end = System.currentTimeMillis();
        System.out.println("ELASPED ms:" + (end - start));

        for (HistoEvent h : histo) {
            System.out.println("h:" + h.hits());
        }
        Assert.assertTrue("hits:" + histo.get(0).hits(), histo.get(0).hits() > 10);
    }


    protected Scanner createScanner(LogRequest request, File file, LogSpace logSpace, Indexer indexer) throws IOException {
        setupHisto(request);
        return new FileScanner(indexer, file.getAbsolutePath(), new SearchHistogramHandler(null, new LogFile("file", 1, "basic", "tags"), request), "", "", aggSpace);
    }

    private LogRequest getRequest(long from, long to, String fieldSetId, String fieldName,  String... includeFilter) {
        LogRequest request = new LogRequest("sub", from, to);
        int i = 0;
        for (String string : includeFilter) {
            string = new SimpleQueryConvertor().convertSimpleToRegExp(string);
            Query query = new Query(i++, i, string, string, false);
            request.addQuery(query);
        }
        request.setBucketCount(4);

        setupHisto(request);
        return request;
    }

    protected void setupHisto(LogRequest request) {
        this.histo = new ArrayList<HistoEvent>();
        for (Query query : request.queries()) {
            histo.add(new HistoEvent(request, 0, 0, DateTimeUtils.currentTimeMillis(), 1, "hostname", "endPoint", request.subscriber(), query, aggSpace, false));
        }
    }

}
