package com.liquidlabs.log;

import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.indexer.persistit.PIIndexer;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.space.WatchDirectory;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 02/06/2014
 * Time: 15:27
 * To change this template use File | Settings | File Templates.
 */
public class FileIngesterFunctionalTest {

    // really write out > 1 minute buckets and check we get back the correct times/buckets
    String lineString = "2014-06-02 14:XX:27,106 INFO visitor-feed-152-1 (EventMonitor)CRAP";

    @Test
    public void testShouldDetectMissingGapsInData() throws Exception {


        String testDir =   "build/file-ingester-test";
        File file = new File(testDir);
        file.mkdirs();

        String filename = testDir + "/crap.log";
        FileOutputStream fos = new FileOutputStream(filename);

        /**
         * Need to write a bit first so we can process it
         */
        int switchOverLineNumber = 15;

        for (int i = 10; i < switchOverLineNumber; i++) {
            fos.write((lineString.replace("XX",i+"")+"\n").getBytes());
            fos.flush();;
        }




        Indexer indexer = new PIIndexer(file.getAbsolutePath());


        int fileId = indexer.open(filename, true, "basic", "");
        LogFile logFile = new LogFile(filename, fileId, "basic","");



        WatchDirectory watchDir = new WatchDirectory("test", testDir,"*.log", "","",100, "", false, BreakRule.Rule.Year.name(), "watchId", false, false);
        GenericIngester ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "", "host", watchDir);




        /**
         * Now - live update the contents - 2 lines per minute
         */
        WriterResult writerResult = ingester.readNext(0, 1);
        long pos = 0;
        int line = 20;
        for (int i = switchOverLineNumber; i < 60; i+=1) {
            fos.write((lineString.replace("XX",i+"")+" ONe\n").getBytes());
            fos.write((lineString.replace("XX",i+"")+" TWO\n").getBytes());
            fos.write((lineString.replace("XX",i+"")+" THREE\n").getBytes());
            fos.flush();;
            writerResult = ingester.readNext(writerResult.currentPosition, writerResult.currentLine);
        }
        fos.close();;
        List<Bucket> buckets = indexer.find(filename,  fileId,	  System.currentTimeMillis());

        System.out.println("Buckets:" + buckets.size());
        Bucket prevBucket = null;
        long lastPos = 0;

        RAF raf = RafFactory.getRaf(filename, logFile.getNewLineRule());
        int errors = 0;

        for (Bucket bucket : buckets) {
            System.out.println("Bucket:" + bucket);
            if (prevBucket != null ) {

                // BOOM! - there is a gap between buckets...
                if (bucket.startingPosition() != lastPos) {
                    System.out.println("FileScanner Seek: PosDelta:" + (bucket.startingPosition() - lastPos) + " ThisStartLine: " + bucket.firstLine() + " PrevEndLine:" + prevBucket.lastLine() + " " + logFile.getFileName()) ;
                    System.out.println("\tPRE_Bucket:" + prevBucket.toString());
                    System.out.println("\tNOW_Bucket:" + bucket.toString());
                    errors ++;


                } else {

                }
            }

            // Scan the bucket
            for (int lineNo = bucket.firstLine(); lineNo <= bucket.lastLine()  ; lineNo += raf.linesRead()) {
                String nextLine = raf.readLine(logFile.getMinLineLength());
            }
            lastPos = raf.getFilePointer();
            System.out.println("\tLastPos:" + lastPos);


            prevBucket = bucket;
        }
        // dont want to see errors!
        Assert.assertTrue(errors == 0);

    }





}
