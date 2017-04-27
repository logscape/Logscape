package com.liquidlabs.log.streaming;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.*;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.InMemoryIndexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.search.Bucket;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.log.space.WatchDirectory;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class StreamingRequestLiveHandlerTest {

    String testFileName = "build/StreamingLiveTest.log";
    private File testFileFile;

    InMemoryIndexer indexer = new InMemoryIndexer();
    NullAggSpace aggSpace = new NullAggSpace();
    Map<String, Tailer> tailers = new HashMap<String, Tailer>();
    private WatchDirectory watch;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


    @Before
    public void before() throws IOException {
        File tf = new File(testFileName);
        tf.delete();
        tf.createNewFile();
        testFileFile = new File(testFileName);
        watch = new WatchDirectory("tag", testFileFile.getParentFile().getAbsolutePath(), testFileFile.getName(), "","", 100, "", true,"", "", true,  true);
    }

    @Test
    public void shouldStreamLive_HISTO_EventsWhenRequestExists() throws Exception {

        FileOutputStream fos = writeTheFile();

        // 1. CREATE THE STREAMING REQUEST
        StreamingRequestHandlerImpl streamHandler = new StreamingRequestHandlerImpl(tailers, aggSpace, "endpointURI", indexer, scheduler);
        LogRequest request = new LogRequestBuilder().getLogRequest("LIVE_1", Arrays.asList("*"), "", 0, DateTimeUtils.currentTimeMillis());
        request.setVerbose(true);
        request.setStreaming(true);
        request.setReplay(null);

        streamHandler.start(request );

        // 2. DETECTED THE FILE - CREATE THE TAILER

        LogFile logFile = indexer.openLogFile(FileUtil.getPath(testFileFile), true, FieldSets.getBasicFieldSet().getId(), "tag");
        LogReader reader = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);
        TailerImpl tailer = new TailerImpl(testFileFile,0, 1, reader, watch, indexer);

        // 4. IMPORT
        tailer.call();

        // 3. ATTACH THE TAILER = it should pick up the request
        streamHandler.attachToTailer(tailer);

        int line = tailer.getLine();
        Assert.assertTrue(line > 0);

        // 5. FILE UPDATED
        writeLine(fos,10);
        writeLine(fos,11);
        fos.flush();

        System.out.println("2 Lines:" + aggSpace.replayEvents.size());
        List<Bucket> replayEvents = printBuckets();

        // 5. DETECT UPDATES AND STREAM
        tailer.call();
        printBuckets();
        Thread.sleep(100);
        Assert.assertEquals("Didnt see any live events", 2, replayEvents.size());
    }


    @Test
    public void shouldStreamLive_REPLAY_EventsWhenRequestExists() throws Exception {

        FileOutputStream fos = writeTheFile();

        // 1. CREATE THE STREAMING REQUEST
        StreamingRequestHandlerImpl streamHandler = new StreamingRequestHandlerImpl(tailers, aggSpace, "endpointURI", indexer, scheduler);
        LogRequest request = new LogRequestBuilder().getLogRequest("LIVE_2", Arrays.asList("*"), "", 0, DateTimeUtils.currentTimeMillis());
        request.setVerbose(true);
        request.setStreaming(true);
        streamHandler.start(request );

        // 2. DETECTED THE FILE - CREATE THE TAILER

        LogFile logFile = indexer.openLogFile(FileUtil.getPath(testFileFile), true, FieldSets.getBasicFieldSet().getId(), "tag");
        LogReader reader = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);
        TailerImpl tailer = new TailerImpl(testFileFile,0, 1, reader, watch, indexer);

        // 4. IMPORT
        tailer.call();

        // 3. ATTACH THE TAILER = it should pick up the request
        streamHandler.attachToTailer(tailer);

        int line = tailer.getLine();
        Assert.assertTrue(line > 0);

        // 5. FILE UPDATED
        writeLine(fos,10);
        writeLine(fos,11);
        fos.flush();

        System.out.println("2 Lines:" + aggSpace.replayEvents.size());
        List<ReplayEvent> replayEvents = printReplayEvents();

        // 5. DETECT UPDATES AND STREAM
        tailer.call();
        printReplayEvents();
        Thread.sleep(100);
        Assert.assertEquals("Didnt see any live events", 2, replayEvents.size());
    }

    private List<Bucket> printBuckets() {
        List<Bucket> events = aggSpace.buckets();
        for (Bucket event : events) {
            System.out.println("Events:" + event + " line: " +event.toString());
        }
        return events;
    }


    private List<ReplayEvent> printReplayEvents() {
        List<ReplayEvent> replayEvents = aggSpace.replayEvents;
        for (ReplayEvent replayEvent : replayEvents) {
            System.out.println("Events:" + replayEvent + " line: " +replayEvent.getLineNumber());

        }
        return replayEvents;
    }

    private FileOutputStream writeTheFile() throws FileNotFoundException,
            IOException, InterruptedException {
        FileOutputStream fos = new FileOutputStream(testFileName);
        writeLine(fos,3);
        writeLine(fos,4);
        writeLine(fos,5);
        fos.flush();
        return fos;
    }



    @Test
    public void shouldStreamLiveEvents() throws Exception {

        testFileFile = new File(testFileName);

        FileOutputStream fos = writeTheFile();

        WatchDirectory watch = new WatchDirectory("tag", testFileFile.getParentFile().getAbsolutePath(), testFileFile.getName(), "","", 100, "", false,"", "", false, true);

        LogFile logFile = indexer.openLogFile(FileUtil.getPath(testFileFile), true, FieldSets.getBasicFieldSet().getId(), "tag");
        LogReader reader = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "sourceURI", "hostname", watch);

        TailerImpl tailer = new TailerImpl(testFileFile,0, 1, reader, watch, indexer);

        // now import the first bit of data
        TailResult call = tailer.call();
        Assert.assertEquals(TailResult.DATA, call);

        int line = tailer.getLine();
        Assert.assertTrue(line > 0);

        tailers.put(tailer.filename(), tailer);


        // now create the StreamingIndexHandler - and add a Request
        StreamingRequestHandlerImpl streamHandler = new StreamingRequestHandlerImpl(tailers, aggSpace, "endpointURI", indexer, scheduler);
        LogRequest request = new LogRequestBuilder().getLogRequest("LIVE_1", Arrays.asList("*"), "", 0, DateTimeUtils.currentTimeMillis());
        request.setVerbose(true);
        request.setStreaming(true);
        streamHandler.start(request );

        // BEGIN TEST -  check the agg space is empty
        Assert.assertEquals(0, aggSpace.replayEvents.size());
        tailer.call();

        writeLine(fos,6);
        fos.flush();
        tailer.call();
        System.err.println("Got Line:" + tailer.getLine());
        writeLine(fos,7);
        fos.flush();
        tailer.call();
        System.err.println("Got Line:" + tailer.getLine());
        Thread.sleep(1000);
        Assert.assertEquals("Didnt see any live events", 2, aggSpace.replayEvents.size());
    }



    private void writeLine(FileOutputStream fos, int line) throws IOException, InterruptedException {
        fos.write((new DateTime() + " CPU:55 Line:" + line +"\n").getBytes());
        Thread.sleep(100);
    }

}
