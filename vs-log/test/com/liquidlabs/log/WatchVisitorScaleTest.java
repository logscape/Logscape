package com.liquidlabs.log;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.space.WatchDirectory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class WatchVisitorScaleTest {

    protected static boolean isTailing = false;

    public String dir;

    List<String> tailing = new ArrayList<String>();

    private boolean isDW;

    private String breakRule;

    @Before
    public void setup() {
        isTailing = false;
        dir = FileUtil.getPath(new File("build/" + getClass().getSimpleName()));
        FileUtil.deleteDir(new File(dir));
        FileUtil.mkdir(dir);
        System.setProperty("watch.visitor.cache.hours", "0");
    }

    @Test
    public void shouldVisitDirectoryAndAddAllFile() throws Exception {

        Map<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);
            }

            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorScaleTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, (ConcurrentHashMap<String, WatchDirectory>) watchDirSet);
        WatchDirectory watchDirectory = new WatchDirectory("", dir, ".*.log", null, "", 120, "", false, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);

        int limit = 100;
        for (int i = 0; i < limit; i++) {
            FileOutputStream fos = new FileOutputStream(dir + "/" + "/watch-scale-visit" + i + ".log");
            fos.write("crap 1\ncrap 2\ncrap 3\ncrap 4\n".getBytes());
            fos.close();
        }

        long start = System.currentTimeMillis();

//        visitor.scanDirForLastMod(watchDirectory);
        watchDirectory.getTracker().scanDirForLastMod();

        List<File> scan1 = visitor.scanWatchDir(watchDirectory);
        assertEquals(limit, scan1.size());

        long end = System.currentTimeMillis();
        System.out.println("1 Elapsed:" + (end - start));
        start = end;


        List<File> scan2 = visitor.scanWatchDir(watchDirectory);
        assertEquals(0, scan2.size());

        end = System.currentTimeMillis();

        System.out.println("2 Elapsed:" + (end - start));


        // now added it to the tailing list
        isTailing = true;

        List<File> scan3 = visitor.scanWatchDir(watchDirectory);
        assertEquals(0, scan3.size());

    }

    //    @Test
    public void shouldDoSquat() throws Exception {
        System.setProperty("log.tailer.high.queue.size", "30000");
        System.setProperty("log.tailer.low.queue.size", "30000");

        Map<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();

        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);
            }

            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorScaleTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);



        WatchDirectory watchDirectory = new WatchDirectory("", dir, ".*.log", null, "", 120, "", false, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);

        int limit = 10000;
        for (int i = 0; i < limit; i++) {
            writeOne(i, dir);
            for (int j = 0; j < 1; j++) {
                final File myDir = new File(dir, String.valueOf(j));
                myDir.mkdir();
                writeOne(i, myDir.getAbsolutePath());
                for (int k = 0; k < 1; k++) {
                    final File theDir = new File(myDir, String.valueOf(k));
                    theDir.mkdir();
                    writeOne(i, theDir.getAbsolutePath());
                }
            }
        }

        System.out.println("Starting...");
        long start = System.currentTimeMillis();
        for (int h = 0; h < 1000; h++) {
            long s1 = System.currentTimeMillis();
            visitor.searchForMatchingFiles();
            long stop = System.currentTimeMillis() - s1;
            System.out.println("Took: " + stop);
            Thread.sleep(5000);
        }
        long took = System.currentTimeMillis() - start;
        System.out.println(took);

    }




    private void writeOne(int i, String dir1) throws IOException {
        FileOutputStream fos = new FileOutputStream(dir1 + "/" + "/watch-scale-visit" + i + ".log");
        fos.write("crap 1\ncrap 2\ncrap 3\ncrap 4\n".getBytes());
        fos.close();
    }
}
