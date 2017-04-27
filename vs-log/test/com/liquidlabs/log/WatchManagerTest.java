package com.liquidlabs.log;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.index.InMemoryIndexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.space.WatchDirectory;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

public class WatchManagerTest {
    private InMemoryIndexer indexer;
    private ArrayList<Tailer> tailers;
    Map<String, WatchDirectory> watchDirSet;
    private WatchManager manager;
    private boolean isDW;
    private String breakRule;
    private String tags = "tags";
    private boolean grokItEnabled = false;

    ArrayList<File> startedWatching = new ArrayList<File>();
    ArrayList<LogFile> startedWatchingLF = new ArrayList<LogFile>();
    ArrayList<LogFile> deleteLogFile = new ArrayList<LogFile>();
    private boolean systemFieldEnabled;
    LogFile testFile;

    @Before
    public void setup() {

        System.setProperty("track.live.ms", "500");

        indexer = new InMemoryIndexer();

        tailers = new ArrayList<Tailer>();
        watchDirSet = new ConcurrentHashMap<String, WatchDirectory>();

        manager = new WatchManager(new WatchManager.Callback() {
            public List<Tailer> tailers() {
                return tailers;
            }

            public void failedToAddWatch(WatchDirectory newWatchDirectory, Throwable reason) {
                final StringWriter stack = new StringWriter();
                reason.printStackTrace(new PrintWriter(stack));
                fail("Failed to add watch: " + newWatchDirectory + " " + stack);
            }

            public void deleteLogFile(List<LogFile> logFiles, boolean forceRemove) {
                startedWatchingLF.removeAll(logFiles);
                deleteLogFile.addAll(logFiles);
            }

        }, indexer, 1);

        WatchVisitor visitor = new WatchVisitor(new WatchVisitor.Callback() {
            public void startTailing(WatchDirectory watch, String file) {
                System.out.println(new DateTime() + " *************** StartWatching:" + file);
                startedWatching.add(new File(file));
                testFile = new LogFile(new File(file).getAbsolutePath(),1,"",watch.getTags());
                startedWatchingLF.add(testFile);
            }

            @Override
            public boolean isTailingFile(String fullFilePath, File file) {
                return false;
            }

            @Override
            public boolean isRollCandidateForTailer(String fullFilePath, Set<String> avoidTheseFiles, String tags) throws InterruptedException {
                return false;
            }

        }, watchDirSet);
        manager.setVisitor(visitor);
    }

    @Test
    public void shouldntDeleteNewFiles() throws Exception{
        String path = new File(".").getCanonicalPath();
        String root = new File("build/wmgr-TAG/").getAbsolutePath();

        writeFileContent(new File(root), "oldFile.old");

        WatchDirectory watch1 = new WatchDirectory("test-1", path + "/build/wmgr-TAG/", "*", null, null, 5, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        watchDirSet.clear();
        watchDirSet.put("1", watch1);


        class MockedLogFile extends LogFile{
            public MockedLogFile(String logFile, int logId, String fieldSetId, String tags){
                super(logFile, logId, fieldSetId, tags);
            }

            @Override
            public long getEndTime(){
                Date curDate = new Date();
                return curDate.getTime() - 5; //brand new
            }
        }

        MockedLogFile mlf = new MockedLogFile(new File(root + "/oldFile.old").getAbsolutePath(), 1,"", "test-1");

        assertFalse(manager.shouldDeleteLogFile(mlf, watchDirSet));

    }

    @Test
    public void shouldDeleteOldFile() throws Exception {
        String path = new File(".").getCanonicalPath();
        String root = new File("build/wmgr-TAG/").getAbsolutePath();

        writeFileContent(new File(root), "oldFile.old");

        WatchDirectory watch1 = new WatchDirectory("test-1", path + "/build/wmgr-TAG/", "*", null, null, 5, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        watchDirSet.clear();
        watchDirSet.put("1", watch1);


        class MockedLogFile extends LogFile{
            public MockedLogFile(String logFile, int logId, String fieldSetId, String tags){
                super(logFile, logId, fieldSetId, tags);
            }

            @Override
            public long getEndTime(){
                Date curDate = new Date();
                return curDate.getTime() - (60000 *10080) ; //1 week old
            }
        }

        MockedLogFile mlf = new MockedLogFile(new File(root + "/oldFile.old").getAbsolutePath(), 1,"", "test-1");

        assertTrue(manager.shouldDeleteLogFile(mlf, watchDirSet));


    }

    /**
     * Bug where Syslog_SERVER_ data was coming in with the logscape-logs tag even though it has !_SERVER_ on the dirs
     * @throws Exception
     */
    @Test
    public void shouldMatchCorrectSourceByTAG() throws Exception {  // this test doesnt work
        String path = new File(".").getCanonicalPath();

        String root = new File("build/wmgr-TAG/").getAbsolutePath();
        writeFileContent(new File(root), "agent.ddd");
        writeFileContent(new File(root + "/SysLog_SERVER_/host"), "user.ddd");
        WatchDirectory watch1 = new WatchDirectory("logscape-logs", path + ",!_SERVER", "*.ddd", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        WatchDirectory watch2 = new WatchDirectory("syslog", path + "/build/wmgr-TAG/SysLog_SERVER_", "*.ddd", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        watchDirSet.clear();
        manager.addWatch(watch1);
        manager.addWatch(watch2);
        Thread.sleep(50);
        for (LogFile logFile : startedWatchingLF) {
            if (logFile.getFileName().contains("user.ddd")) {
                assertEquals("syslog", logFile.getTags());
            }
        }



    }

    @Test
    public void shouldDeleteIfAllAgree() throws Exception{
        String path = new File(".").getCanonicalPath();
        String root = new File("build/wmgr-TAG/").getAbsolutePath();

        writeFileContent(new File(root), "oldFile.old");

        WatchDirectory watch1 = new WatchDirectory("test-1", path + "/build/wmgr-TAG/", "*", null, null, 5, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        WatchDirectory watch2 = new WatchDirectory("test-1", path + "/build/wmgr-TAG/", "*", null, null, 5, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        watchDirSet.clear();
        watchDirSet.put("1", watch1);
        watchDirSet.put("2", watch2);


        class MockedLogFile extends LogFile{
            public MockedLogFile(String logFile, int logId, String fieldSetId, String tags){
                super(logFile, logId, fieldSetId, tags);
            }

            @Override
            public long getEndTime(){
                Date curDate = new Date();
                return curDate.getTime() - (60000 *10080) ; //1 week old
            }
        }

        MockedLogFile mlf = new MockedLogFile(new File(root + "/oldFile.old").getAbsolutePath(), 1,"", "test-1");

        assertTrue(manager.shouldDeleteLogFile(mlf, watchDirSet));


    }

    @Test
    public void shouldntDeleteIfWatchDirectoriesDisagree() throws Exception{
        String path = new File(".").getCanonicalPath();
        String root = new File("build/wmgr-TAG/").getAbsolutePath();

        writeFileContent(new File(root), "oldFile.old");

        WatchDirectory watch1 = new WatchDirectory("test-1", path + "/build/wmgr-TAG/", "*", null, null, 5, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        WatchDirectory watch2 = new WatchDirectory("test-1", path + "/build/wmgr-TAG/", "*", null, null, 999, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        watchDirSet.clear();
        watchDirSet.put("1", watch1);
        watchDirSet.put("2", watch2);


        class MockedLogFile extends LogFile{
            public MockedLogFile(String logFile, int logId, String fieldSetId, String tags){
                super(logFile, logId, fieldSetId, tags);
            }

            @Override
            public long getEndTime(){
                Date curDate = new Date();
                return curDate.getTime() - (60000 *10080) ; //1 week old
            }
        }

        MockedLogFile mlf = new MockedLogFile(new File(root + "/oldFile.old").getAbsolutePath(), 1,"", "test-1");

        assertFalse(manager.shouldDeleteLogFile(mlf, watchDirSet));

    }

    @Test
    public void shouldObeyMultipleFileFiltersWithExcludes() throws Exception {
        String root = new File("build/wmgr-mfilt-excl/").getAbsolutePath();
        File dir = new File(root + "/test/log/");
        int deleteDir = FileUtil.deleteDir(dir);
        dir.mkdirs();
        String filename = "myfile.log";
        File file1 = writeFileContent(dir, filename);

        String filename2 = "other.log";
        File file2 = writeFileContent(dir, filename2);

        // test
        WatchDirectory watch1 = new WatchDirectory(tags, dir.getAbsolutePath(), "*my*,!*other*", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);

        manager.addWatch(watch1);
        assertEquals("Didnt start watching the file:1", 1, this.startedWatching.size());
        assertEquals("Delete was wrong", 0, this.deleteLogFile.size());

        // add a wider scope
        WatchDirectory watch2 = new WatchDirectory(tags, dir.getAbsolutePath(), "*my*", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);

        manager.addWatch(watch2);
        assertEquals("Didnt start watching the file:1", 2, this.startedWatching.size());
        assertEquals("Delete was wrong", 0, this.deleteLogFile.size());

        // remove the first one - should not generate any events
        manager.removeWatch(watch1);
        assertEquals("Didnt start watching the file:1", 2, this.startedWatching.size());
        assertEquals("Delete was wrong", 0, this.deleteLogFile.size());
    }


    @Test
    public void shouldObeyMultipleFileFilters() throws Exception {
        String root = new File("build/wmgr-mfilters/").getAbsolutePath();
        File dir = new File(root + "test/log/");
        dir.mkdirs();
        String filename = "myFile.log";
        File file1 = writeFileContent(dir, filename);

        String filename2 = "otherFile.log";
        File file2 = writeFileContent(dir, filename2);

        String filename3 = "notThis.log";
        File file3 = writeFileContent(dir, filename3);

        // test
        WatchDirectory watch = new WatchDirectory(tags, dir.getAbsolutePath(), "*my*,*other*", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);

        manager.addWatch(watch);
        sleepForQueueTimeout();
        assertEquals("Didnt start watching the file:" + 2, 2, this.startedWatching.size());
    }

    @Test
    public void shouldRenameFileTagsWhenWatchTagRenamed() throws Exception {
        String root = new File("build/wmgr-ren-tag/").getAbsolutePath();
        File dir = new File(root + "test/log/");
        dir.mkdirs();
        String filename = "myFile.log";

        File file = writeFileContent(dir, filename);

        addLogFileToIndexer(new LogFile(file.getAbsolutePath(),100, "fieldSetId", "tags"));

        // test
        WatchDirectory watch = new WatchDirectory(tags, dir.getAbsolutePath(), "*log", null, "", -1, "", isDW, breakRule, "1", grokItEnabled, systemFieldEnabled);

        manager.addWatch(watch);
        sleepForQueueTimeout();
        assertEquals("Didnt start watching the file:" + 1, 1, this.startedWatching.size());
        assertEquals("Didnt start watching the file:" + 1, 0, this.deleteLogFile.size());


        // TEST rename file tag
        WatchDirectory watch2 = new WatchDirectory(tags, dir.getAbsolutePath(), "*log", null, "", -1, "", isDW, breakRule, "1", grokItEnabled, systemFieldEnabled);
        watch2.setTags("new-tag");
        indexer.updated = 0;
        manager.updateWatch(watch2, true);
        sleepForQueueTimeout();
        assertEquals("Should not have deleted the file:" + 1, 0, this.deleteLogFile.size());
        assertEquals("Should have updated something", 1, indexer.updated);
        assertEquals("new-tag", indexer.openLogFile(file.getAbsolutePath()).getTags());



    }

    @Test
    public void shouldAddRemoveAddRemoveAddFile() throws Exception {
        String root = new File("build/wmgr-1/").getAbsolutePath();
        File dir = new File(root + "test/log/");
        dir.mkdirs();
        String filename = "myFile.log";

        File file = writeFileContent(dir, filename);

        addLogFileToIndexer(new LogFile(file.getAbsolutePath(),100, "fieldSetId", "tags"));

        // test

        WatchDirectory watch = new WatchDirectory(tags, dir.getAbsolutePath(), "*log", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);

        for (int i = 1; i < 10; i++) {
            System.out.println("\n\n" + new Date() + "============= Iteration:" + i);
            manager.addWatch(watch);
            sleepForQueueTimeout();
            assertEquals("Didnt start watching the file:" + i, i, this.startedWatching.size());

            int removedFileCount = manager.removeWatch(watch);
            assertEquals("didnt remove any files:" + i, 1, removedFileCount);
            sleepForQueueTimeout();
        }
    }

    private void sleepForQueueTimeout() throws InterruptedException {
        Thread.sleep(2000);
    }

    @Test
    public void shouldRemoveWatchedExplicitFileThatWas_DELETED() throws Exception {
        String root = new File("build/wmgr-DEL/").getAbsolutePath();
        File dir = new File(root + "test/log/");
        dir.mkdirs();
        String filename = "myFile.log_DEL_17-10-10-0300";
        File file = writeFileContent(dir, filename);
        addLogFileToIndexer(new LogFile(file.getAbsolutePath(),100, "fieldSetId", "tags"));


        // test - explicit filename

        WatchDirectory watch = new WatchDirectory(tags, dir.getAbsolutePath(), "myFile.log*", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        manager.addWatch(watch);
        assertEquals("Didnt start watching the file 1", 1, this.startedWatching.size());

        int removedFileCount = manager.removeWatch(watch);

        assertEquals("didnt remove any files", 1, removedFileCount);
    }

    @Test
    public void shouldRemoveWatchedExplicitFile() throws Exception {
        String root = new File("build/wmgr-4/").getAbsolutePath();
        File dir = new File(root + "test/log/");
        dir.mkdirs();
        String filename = "myFile.log";

        File file = writeFileContent(dir, filename);
        addLogFileToIndexer(new LogFile(file.getAbsolutePath(),100, "fieldSetId", "tags"));


        // test - explicit filename

        WatchDirectory watch = new WatchDirectory(tags,dir.getAbsolutePath(), "myFile.log", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        manager.addWatch(watch);
        sleepForQueueTimeout();
        assertEquals("Didnt start watching the file 1", 1, this.startedWatching.size());

        int removedFileCount = manager.removeWatch(watch);

        assertEquals("didnt remove any files", 1, removedFileCount);
    }


    @Test
    public void shouldRemoveWatchedFileWhereThereAreMultipleWatchFilesAndMultipleWatches() throws Exception {
        // expects

        String root = new File("build/wmgr-3/").getAbsolutePath();
        File dir = new File(root + "test/log1/");
        dir.mkdirs();
        File dir2 = new File(root + "test/log1/");
        dir2.mkdirs();

        // expects
        List<LogFile> files = java.util.Arrays.asList(
                new LogFile(writeFileContent(dir, "myFile1.log").getAbsolutePath(),100, "fieldSetId", "tags"),
                new LogFile(writeFileContent(dir2, "myFile2.log").getAbsolutePath(),100, "fieldSetId", "tags")
        );
        for (LogFile logFile : files) {
            addLogFileToIndexer(logFile);
        }

        // test
        WatchDirectory watch = new WatchDirectory(tags,dir.getAbsolutePath(), "myFile1.log", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        manager.addWatch(watch);
        sleepForQueueTimeout();
        assertEquals("Didnt start watching the file 1", 1, this.startedWatching.size());

        WatchDirectory watch2 = new WatchDirectory(tags,dir.getAbsolutePath(), "myFile2.log", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        manager.addWatch(watch2);
        sleepForQueueTimeout();
        assertEquals("Didnt start watching the file 2", 2, this.startedWatching.size());

        int removedFileCount = manager.removeWatch(watch);

        assertEquals("didnt remove any files", 1, removedFileCount);

    }
    @Test
    public void shouldRemoveWatchedFileWhereThereAreMultipleWatchDirs() throws Exception {

        String root = new File("build/wmgr-2/").getAbsolutePath();
        File dir = new File(root + "test/log1/");
        dir.mkdirs();
        File dir2 = new File(root + "test/log2/");
        dir2.mkdirs();

        // expects
        List<LogFile> files = java.util.Arrays.asList(
                new LogFile(writeFileContent(dir, "myFile.log").getAbsolutePath(),100, "fieldSetId", "tags"),
                new LogFile(writeFileContent(dir2, "myFile.log").getAbsolutePath(),100, "fieldSetId", "tags")
        );
        for (LogFile logFile : files) {
            addLogFileToIndexer(logFile);
        }

        // test
        WatchDirectory watch = new WatchDirectory(tags,dir.getAbsolutePath(), ".*log", null, null, -1, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        manager.addWatch(watch);
        sleepForQueueTimeout();
        assertEquals("Didnt start watching the file 1", 1, this.startedWatching.size());

        WatchDirectory watch2 = new WatchDirectory(tags,dir2.getAbsolutePath(), ".*log", null, null, -1, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        manager.addWatch(watch2);
        sleepForQueueTimeout();

        assertEquals("Didnt start watching the file 2", 2, this.startedWatching.size());

        int removedFileCount = manager.removeWatch(watch);

        assertEquals("didnt remove any files", 1, removedFileCount);

    }

    /**
     * Dont get this test. Overlapping datasources. Not sure how it ever worked.
     * @throws Exception
     */
//	@Test DodgyTest
    public void shouldRemoveFileWhenItBecomesExcludedAndNotOtherFiles() throws Exception {
        String root = new File("build/wmgr-MWatch/").getAbsolutePath();
        File dir = new File(root + "test/log/");
        dir.mkdirs();
        String filenameI = "keepMe.log";
        File fileI = writeFileContent(dir, filenameI);
        addLogFileToIndexer(new LogFile(fileI.getAbsolutePath(),100, "fieldSetId", "tags"));

        String filenameX = "excludeMe.log";
        File fileX = writeFileContent(dir, filenameX);
        addLogFileToIndexer(new LogFile(fileX.getAbsolutePath(),100, "fieldSetId", "tags"));

        // now watch everything
        WatchDirectory watch = new WatchDirectory(tags, dir.getAbsolutePath(), "*log", null, "", -1, "", isDW, breakRule, "abc", grokItEnabled, systemFieldEnabled);
        manager.addWatch(watch);
        assertEquals("Didnt start watching the file", 2, this.startedWatching.size());

        // now Im trying to modify the watch by changing the exclude mask...
        WatchDirectory watchX = new WatchDirectory(tags, dir.getAbsolutePath(), "*log,!*exclude*", null, "", -1, "", isDW, breakRule,"abcd", grokItEnabled, systemFieldEnabled);
        manager.addWatch(watchX);

        // still got 2 items
        assertEquals("Didnt start watching the file:" + this.startedWatching, 2, new HashSet(this.startedWatching).size());
        assertEquals("Should not have deleted anything", 0, this.deleteLogFile.size());

        // now remove the first watch - and we should get 1 delete event
        int removed = manager.removeWatch(watch);

        assertEquals("Should have deleted only 1 file", 1, removed);
    }

    @Test
    public void shouldAddThenRemoveWatchedFile() throws Exception {
        String root = new File("build/wmgr-1/").getAbsolutePath();
        File dir = new File(root + "test/log/");
        dir.mkdirs();
        String filename = "myFile.log";

        File file = writeFileContent(dir, filename);

        addLogFileToIndexer(new LogFile(file.getAbsolutePath(),100, "fieldSetId", "tags"));

        // test

        WatchDirectory watch = new WatchDirectory(tags, dir.getAbsolutePath(), "*log", null, null, 120, "", isDW, breakRule, null, grokItEnabled, systemFieldEnabled);
        manager.addWatch(watch);
        sleepForQueueTimeout();
        assertEquals("Didnt start watching the file", 1, this.startedWatching.size());

        int removedFileCount = manager.removeWatch(watch);

        assertEquals("didnt remove any files", 1, removedFileCount);

    }

    //    @Test need to create a dir with different permissions
    public void shouldAddThisWithOutBlowingUp() {
        final WatchDirectory syslog = new WatchDirectory("syslog", "/var/log/**", "syslog,system.log,!.bz2,!.gz", null, null, 120, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        manager.addWatch(syslog);
    }


    private File writeFileContent(File dir, String filename)
            throws FileNotFoundException, IOException {
        dir.mkdirs();
        File file = new File(dir.getAbsolutePath() + "/" + filename);
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("line1\nline2".getBytes());
        fos.close();
        return file;
    }
    private LogFile addLogFileToIndexer(LogFile logFile) {
        return indexer.openLogFile(logFile.getFileName(), true, "basic", tags);
    }
}
