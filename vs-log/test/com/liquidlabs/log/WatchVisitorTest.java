package com.liquidlabs.log;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.space.WatchDirectory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

public class WatchVisitorTest {

    protected static boolean isTailing = false;

    public String dir;

    List<String> tailing = new ArrayList<String>();

    private boolean isDW;

    private String breakRule;

    @Before
    public void setup() {
        System.setProperty("track.live.ms", "500");
        isTailing = false;
        dir = FileUtil.getPath(new File("build/" + getClass().getSimpleName()));
        FileUtil.deleteDir(new File(dir));
        FileUtil.mkdir(dir);
        System.setProperty("watch.visitor.cache.hours", "0");
    }

    @Test
    public void shouldRelativePathFor_SERVER_() throws Exception {

        String dsPath = ".,./work,./work/*";
        String logServer = "./work/LogServer_SERVER_";
        WatchDirectory wd = new WatchDirectory("logscape-logs", dsPath, "*.log*,!audit,!event,!stats,!schedule", "", "", 99, "", false, "", "logscape-logs", false, false);
        WatchDirectory sdir = new WatchDirectory("server-logs", logServer, "DO_NOT_DELETE", "", "", 99, "", false, "", "server-logs", false, false);


        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        watchDirSet.put(sdir.id(), sdir);
        watchDirSet.put(wd.id(), wd);


        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);;
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);

        // should add server dir and watch dirs
        visitor.buildLastModMap();
        Thread.sleep(500);
        Set<Map.Entry<String,WatchDirectory>> entries = visitor.fwdWatchDirSet.entrySet();
        for (Map.Entry<String, WatchDirectory> entry : entries) {
            System.out.println("FWDR" + entry.getKey() + " / " + entry.getValue().toString().replace(",", "\n\t"));
        }


        Thread.sleep(500);
    }

    @Test
    public void shouldGrabHost_SERVER_() throws Exception {

        String path = "/home/logscape/logscape/work/LogServer_SERVER_";
        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file, int item, int total) {
                tailing.add(file);;
            }

            @Override
            public void startTailing(WatchDirectory watch, String file) {

            }

            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);
        String host = LogProperties.getHostFromPath(path);
        assertNotNull(host);
    }


    @Test
    public void shouldGrabHostStuff() throws Exception {
        //String path = "/home/logscape/logscape/work/LogServer_SERVER_/CRAP-HOST/Volumes/Media/LicenseServerTrial/logs/license.log";
        String path = "/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/work/LogServer_SERVER_/CRAP-HOST/C/LOGS/Delta/ems";
        path = FileUtil.makePathNative(path);
        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);;
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);
        String host = LogProperties.getHostFromPath(path);
        assertNotNull(host);
        assertEquals("CRAP-HOST",host);

    }

    @Test
    public void shouldVisitDirectoryAndAddFileThenAddAgainIFItWasDeleted() throws Exception {

        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);;
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);
        WatchDirectory watchDirectory = new WatchDirectory("", dir, ".*.log", null, "", 120, "", isDW, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);

        String testFile = dir + "/" + "visit1.log";
        FileOutputStream fos = new FileOutputStream(testFile);
        fos.write("crap".getBytes());
        fos.close();

        visitor.buildLastModMap();//assembleLastModMap(new File(dir), watchDirectory);
        List<File> scan1 = visitor.scanWatchDir(watchDirectory);
        assertEquals(1,scan1.size());

        // now added it to the tailing list
        isTailing = true;

        List<File> scan3 = visitor.scanWatchDir(watchDirectory);
        assertEquals(0,scan3.size());

//        System.out.println("1111 LastMod:" + new DateTime(new File(dir).lastModified()));

        Thread.sleep(1000);

        // rewrite the file
        new File(testFile).delete();
        fos = new FileOutputStream(testFile);
        fos.write("crap".getBytes());
        fos.close();
        Thread.sleep(1000);

//        System.out.println("2222 LastMod:" + new DateTime(new File(dir).lastModified()));

        // now it should be re-added because the lastmod of the dir has changed
        // plus the file is not being tailed and
        isTailing = false;


        List<File> scan4 = visitor.scanWatchDir(watchDirectory);
        assertEquals(1,scan4.size());
    }

    @Test // DodgyTest?: I think this is probably broken. Though I have no F*ckin idea as to how it is supposed to behave
    public void shouldWorkWithRelativePathUnixApp() throws Exception {
        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }
        }, watchDirSet);

        // File mask is explicit  - use a wildcard on the directory
        String mainDir = "./build/" + getClass().getSimpleName() + "/work/UnixApp-1.0";
        WatchDirectory watchDirectory = new WatchDirectory("TAG", mainDir + "/*", "*.log", null, "", 120, "", false, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);

        File parent = new File(mainDir + "/13Jan31/");
        parent.mkdirs();
        FileOutputStream fos = new FileOutputStream(new File(parent,"CPU.log"));
        fos.write("crap\ncrap2\n".getBytes());
        fos.close();

        watchDirectory.makeDirsAbsolute();
        visitor.buildLastModMap();//assembleLastModMap(parent, watchDirectory);

        List<File> scan1 = visitor.scanWatchDir(watchDirectory);
        assertEquals(1,scan1.size());

    }
    @Test
    public void shouldWorkWithRelativePath() throws Exception {
        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);;
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }
        }, watchDirSet);

        String dd = "./build/" + getClass().getSimpleName();
        // File mask is explicit
        WatchDirectory watchDirectory = new WatchDirectory("", dd, "visitRel.log", null, "", 120, "", isDW, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);

        FileOutputStream fos = new FileOutputStream(dir + "/" + "visitRel.log");
        fos.write("crap\ncrap2\n".getBytes());
        fos.close();

        watchDirectory.makeDirsAbsolute();
        visitor.buildLastModMap();//assembleLastModMap(new File(dir), watchDirectory);
        Map<String, Long> lastModifiedDirs = visitor.lastModifiedDirs("");
        for (String dirr : lastModifiedDirs.keySet()) {
            System.out.println(dirr);
        }


        List<File> scan1 = visitor.scanWatchDir(watchDirectory);
        assertEquals(1,scan1.size());

    }

    @Test
    public void shouldVisitDirectoryAndAddExplicitFile() throws Exception {

        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);;
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }
        }, watchDirSet);

        // File mask is explicit
        WatchDirectory watchDirectory = new WatchDirectory("", dir, "visit1.log", null, "", 120, "", isDW, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);



        FileOutputStream fos = new FileOutputStream(dir + "/" + "visit1.log");
        fos.write("crap\ncrap2\n".getBytes());
        fos.close();

        visitor.buildLastModMap();//assembleLastModMap(new File(dir), watchDirectory);

        List<File> scan1 = visitor.scanWatchDir(watchDirectory);
        assertEquals(1,scan1.size());

        // now added it to the tailing list
        isTailing = true;

        List<File> scan3 = visitor.scanWatchDir(watchDirectory);
        assertEquals(0,scan3.size());

    }


    @Test
    public void shouldVisitForwarderDirectoryAndAddFile() throws Exception {

        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);;
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);


        WatchDirectory watchDirectory = new WatchDirectory("", dir, ".*.log", null, "", 120, "", isDW, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);



        FileOutputStream fos = new FileOutputStream(dir + "/" + "visit99.log");
        fos.write("crap".getBytes());
        fos.close();

        visitor.buildLastModMap();//assembleLastModMap(new File(dir), watchDirectory);

        List<File> scan1 = visitor.scanWatchDir(watchDirectory);
        assertEquals(1,scan1.size());

        Thread.sleep(3000);

        List<File> scan2 = visitor.scanWatchDir(watchDirectory);
        assertEquals(0,scan2.size());

        // now added it to the tailing list
        isTailing = true;

        List<File> scan3 = visitor.scanWatchDir(watchDirectory);
        assertEquals(0,scan3.size());

    }
    @Test
    public void shouldAddForwarderWatchDir() throws Exception {

        Map<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                tailing.add(file);;
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return WatchVisitorTest.isTailing;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, (ConcurrentHashMap<String, WatchDirectory>) watchDirSet);
        String dir = this.dir + "_VISITOR_FWDR_TEST_";
        new File(dir + "/LogServer_SERVER_").mkdirs();

        // add the LogServer_SERVER_ dir
        WatchDirectory serverDS = new WatchDirectory("SERVER", dir + "/LogServer_SERVER_" , "XXX", null, "", 120, "", isDW, breakRule, null, false, true);
        watchDirSet.put(serverDS.id(), serverDS);

        // add normal Datasource
        WatchDirectory regularDS = new WatchDirectory("App", dir + "/MyApp-1.*/*" , "XXX", null, "", 120, "", isDW, breakRule, null, false, true);
        watchDirSet.put(regularDS.id(), regularDS);

        // for it to be added in
        visitor.checkForServerDirChange();
//        visitor.buildLastModMap();
//        visitor.getFullSetOfWatchDirs();
//        visitor.buildLastModMap();
//        visitor.getFullSetOfWatchDirs();



        // check to see that the fwrder dir was added correctly
        // expect the default server-dir to be there (work/LogServer_SERVER_), plus the new forwarded DataSource
        assertEquals(2, visitor.fwdWatchDirSet.size());



    }
    @Test
    public void shouldVisitDirectoryAndAddFile() throws Exception {

        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        tailing.clear();

        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                if (!tailing.contains(file)){
                    System.out.println(Thread.currentThread().getName() + " Added:" + file);
                    tailing.add(file);
                } else {
                    System.out.println("Already Added:" + file);
                }
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return tailing.contains(fullFilePath);
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);

        String baseDir = "./build/visit-loop";
        WatchDirectory watchDirectory = new WatchDirectory("", baseDir, "*xxx*", null, "", 120, "", isDW, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);

        File dir1 = new File(baseDir);
        FileUtil.deleteDir(dir1);
        dir1.mkdirs();


        System.out.println("Started:" + tailing.size());
        for (int i = 1; i < 10; i++) {
            System.out.println(">>>>>" + i);
            writeFile(baseDir + "/file" + i + "-xxx.log");
            visitor.searchForMatchingFiles();
            Thread.sleep(500);
            System.out.println("Scan:" + tailing.size());
            if (tailing.size() != i) {
                visitor.searchForMatchingFiles();
                System.err.println("Mismatching:::::::::::: Tailing:" + tailing.size());
                Thread.sleep(500);
            }
            assertEquals(i, tailing.size());
            Thread.sleep(500);
        }
    }

    @Test
    public void shouldVisitNestedDirectoryAndAddFile() throws Exception {

        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        tailing.clear();

        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                if (!tailing.contains(file)){
                    System.out.println(Thread.currentThread().getName() + " Added:" + file);
                    tailing.add(file);
                } else {
                    System.out.println("Already Added:" + file);
                }
            }
            public boolean isTailingFile(String fullFilePath, File file) {
                return tailing.contains(fullFilePath);
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);

        String baseDir = "./build/visit-loop-multi";
        FileUtil.deleteDir(new File(baseDir));
        new File(baseDir).mkdirs();
        WatchDirectory watchDirectory = new WatchDirectory("", baseDir + "/*", "*xxx*", null, "", 120, "", isDW, breakRule, null, false, true);
        watchDirSet.put(watchDirectory.id(), watchDirectory);

        File dir1 = new File(baseDir);
//        FileUtil.deleteDir(dir1);
//        dir1.mkdirs();


        System.out.println("Started:" + tailing.size());
        for (int i = 1; i < 10; i++) {
            System.out.println(">>>>>" + i);
            writeFile(baseDir + "/" + i + "/file" + i + "-xxx.log");
            visitor.searchForMatchingFiles();
            Thread.sleep(1000);
            System.out.println("Scan:" + tailing.size());
            if (tailing.size() != i) {
                visitor.searchForMatchingFiles();
                System.err.println("Mismatching:::::::::::: Tailing:" + tailing.size());
                Thread.sleep(500);
            }
            Thread.sleep(1000);

            assertEquals(i, tailing.size());
            Thread.sleep(500);
        }
    }


    private void writeFile(String filename) {
        try {
            new File(filename).getParentFile().mkdirs();
            FileOutputStream fos = new FileOutputStream(new File(filename));
            fos.write("one\ntwo\nthree\n".getBytes());
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();;
        }
    }

    @Test
    public void shouldIgnoreThisExt() throws Exception {

        NumberFormat format = new DecimalFormat("#0");
        String format2 = format.format(1222222222222229999.22);
        System.out.println(format2);

        ConcurrentHashMap<String, WatchDirectory> watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();
        //		startLogService();
        WatchVisitor visitor = new WatchVisitor(null, watchDirSet);
//		2010-04-16 16:29:30,234 WARN tailer-sched-141-2 (log.AgentLogServiceImpl)	fullFilePath:E:\workspace\master\build\logscape\work\webapp\WEB-INF\lib\commons-codec-1.3.jar r:false set:[.dll, .exe, .jar, .lib, .so, .cab, .zip]

        assertTrue(visitor.isIgnoringThisExtension("E:\\workspace\\master\\build\\logscape\\work\\webapp\\WEB-INF\\lib\\commons-codec-1.3.jar"));
        assertTrue(visitor.isIgnoringThisExtension("c:\\somefile\\crap.dll"));
        assertFalse(visitor.isIgnoringThisExtension("c:\\somefile\\stuff.log"));
    }



}
