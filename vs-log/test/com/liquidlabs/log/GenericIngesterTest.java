package com.liquidlabs.log;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.TimeZoneDiffer;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.InMemoryIndexer;
import com.liquidlabs.log.index.Line;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.reader.LocalIndexerIngestHandler;
import com.liquidlabs.log.reader.GenericIngester;
import com.liquidlabs.log.space.WatchDirectory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GenericIngesterTest {


    private NullAggSpace aggSpace ;
    private InMemoryIndexer indexer = new InMemoryIndexer();
    private AtomicLong indexedDataSize = new AtomicLong();
    private FieldSet fieldSet = FieldSets.get();
    private LogFile logFile;
    private boolean summary;
    private String sourceTags = "";

    @Before
    public void setup() {
        new File("build").mkdir();
        aggSpace = new NullAggSpace();
        logFile = new LogFile("logFile.log", 100, "fieldSet", "tag");

        indexedDataSize.set(0);
    }
    @Test
    public void shouldTzOffSetFromPropertiesFile() throws Exception {

        TimeZone aDefault = TimeZone.getDefault();
        int offset = aDefault.getOffset(new DateTime().getMillis());
        int rawOffset = aDefault.getRawOffset();

        int est = TimeZoneDiffer.getHoursDiff("EST");

        // should be 6 hours


        String serverDir = "build/XX_SERVER_/MyServer/";
        String filename = serverDir + "crap/file.log";
        LogFile logFile = new LogFile(filename, 1, "basic", "a,b,c");

        new File(filename).mkdirs();
        File propsFile = new File(serverDir, "datasource.properties");
        FileOutputStream fos = new FileOutputStream(propsFile);
        fos.write("source.timezone=CEST".getBytes());
        fos.close();

//        String displayName = TimeZone.getDefault().getID();

        short xx = GenericIngester.getTzOffset("xx", logFile);
        assertTrue(xx != 0);
    }

    @Test
    public void shouldGetFileSecs() throws Exception {
        GenericIngester ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "", "host", new WatchDirectory(99999));

        // 1500 is 75% of 2000 - so we want to get a time which is 75% of the total file time
        int fileSecs = ingester.getFileSecs(1000 * 1000, 2000 * 1000, 1500, 2000);
        System.out.println("Filesecs:" + fileSecs);
        assertEquals(750, fileSecs);
    }

    @Test
    public void testShouldTailFileAndGetGoodTimeProperlyFromFileLastMod() throws Exception {

        final File tailingFile = new File("build", "fileIngesterTailerTest.log");

        logFile = new LogFile(tailingFile.getAbsolutePath(),100,fieldSet.id,"tags");

        Thread threadWriter = new Thread() {
            @Override
            public void run() {
                try {
                    tailingFile.delete();
                    int line = 0;
                    while (true) {
                        // need to make last mod change
                        FileOutputStream fos = new FileOutputStream(tailingFile, true);

                        String string = "---------------stuff:" + new Date() + " line:" + line++ + "\n";
                        System.out.println("Write:" + string);
                        fos.write(string.getBytes());
                        fos.flush();
                        fos.close();

                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        threadWriter.setDaemon(true);
        threadWriter.start();

        // wait for the file to be written
        Thread.sleep(1000);

        indexer.open(logFile.getFileName(), true, fieldSet.getId(), sourceTags );

        GenericIngester ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "", "host", new WatchDirectory(99999));

        int lineNumber = 1;
        long sPos = 0;

        int count = 0;
        int loopCount = 0;
        // mimmic the tailer
        while (lineNumber < 10 && loopCount++ < 100) {
            long lastModified = tailingFile.lastModified();
            System.out.println(count++ + " now:" + DateUtil.shortTimeFormat.print(new DateTime()) + " lastMod:" + DateUtil.shortTimeFormat.print(lastModified));
            WriterResult readNext = ingester.readNext(sPos, lineNumber);
            System.out.println(readNext);
            sPos = readNext.currentPosition;
            lineNumber = readNext.currentLine;

            Thread.sleep(500);
            System.out.println("CurrentLine:" + lineNumber);
        }
        threadWriter.interrupt();

        List<Line> lines = indexer.getLines(tailingFile.getAbsolutePath());

        assertTrue(lines.size() > 1);

        DateTime firstLineTime = new DateTime(lines.get(0).time());
        DateTime lastLineTime = new DateTime(lines.get(lines.size() -1).time());
        DateTime now = new DateTime();
        DateTime lastMod = new DateTime(tailingFile.lastModified());
        System.out.println(String.format("Now:%s, LastMod:%s, FirstLine:%s LastLine:%s lines:%d", now.toString(), lastMod.toString(), firstLineTime.toString(), lastLineTime.toString(), lines.size()));


        assertTrue(lines.get(0).time() != lines.get(lines.size()-1).time());
    }

    @Test
    public void testShouldTailFileFileProperly() throws Exception {
        String filename = "test-data/2009081707-nodes.txt";
        LogFile logFile = new LogFile(filename, 100, fieldSet.getId(),"tags");

        indexer.open(logFile.getFileName(), true, fieldSet.getId(), sourceTags);

        fieldSet = new FieldSet();
        fieldSet.timeStampField = 1;
        // 3584 Mon Aug 17 07:00:03 BST 2009	Mon Aug 17 07:00:03 BST 2009	1	lon-fortdev-calc2/10.54.24.99	lon-fortdev-calc2_0	14225	SempraIridiumMdsRunServer	lon-fortdev-calc2	n/a	n/a
        fieldSet.expression = "^(*)\t(s19) (**)";
        fieldSet.addField("batch", "count()", true, 1, summary, "","", "", false);
        fieldSet.addField("timestamp", "count()", true, 2, summary,"","", "", false);
        fieldSet.addField("rest of line", "count()", true, 3, summary,"","", "", false);
//		fieldSet.fieldNames = new String[] { "batch","timestamp","rest of line" };
        fieldSet.timeStampField = 2;

        GenericIngester ingester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "", "host", new WatchDirectory(99999));

        RAF raf = RafFactory.getRafSingleLine(filename);

        ingester.readNext(0, 0);

        // verify we have valid line times on the index
        TreeMap<Long, List<Line>> intexed = indexer.getIndex().get(filename);
        for (Long time : intexed.keySet()) {
            List<Line> list = intexed.get(time);
            for (Line line2 : list) {
                System.out.println(" I:" + line2 +  " time:" + DateTimeFormat.longDateTime().print(line2.time()));
            }
        }
        raf.close();
    }

    @Test
    public void testShouldImportFileProperly() throws Exception {
        String filename = "test-data/2009081707-nodes.txt";
        LogFile logFile = new LogFile(filename,100,fieldSet.getId(), "tags");
        indexer.open(logFile.getFileName(), true, fieldSet.getId(), sourceTags);
        GenericIngester fileIngester = new GenericIngester(logFile, indexer, new LocalIndexerIngestHandler(indexer), "", "host", new WatchDirectory(99999));

        WriterResult readNext = fileIngester.readNext(0, 1);

        System.out.println("Result:" + readNext);

        List<Line> lines = indexer.getLines(filename);

        long countLines = FileUtil.countLines(new File(filename))[1];

        assertEquals(countLines, lines.size());
        DateTimeFormatter formatter = DateUtil.shortDateTimeFormat4;
        String lineTime = formatter.print(lines.get(1).time());
        assertTrue(formatter.print(lines.get(1).time()), lineTime.contains("17-Aug-2009 07:00:03") || lineTime.contains("17-Aug-2009 06:00:03"));

        String lastLineTime = formatter.print(lines.get(lines.size()-1).time());
        assertTrue(formatter.print(lines.get(lines.size()-1).time()), lastLineTime.contains("17-Aug-2009 07:59:01") || lastLineTime.contains("17-Aug-2009 06:59:01") );
    }
}
