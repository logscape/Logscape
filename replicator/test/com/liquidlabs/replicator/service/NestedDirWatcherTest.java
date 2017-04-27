package com.liquidlabs.replicator.service;

import com.liquidlabs.common.file.FileUtil;
import junit.framework.TestCase;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;

public class NestedDirWatcherTest extends TestCase implements DirEventHandler {

    private DirWatcher dirWatcher;
    ArrayList<File> files = new ArrayList<File>();
    ArrayList<File> modified = new ArrayList<File>();
    private int deleteCount;
    private int modifiedCount;
    private File parent;

    @Override
    protected void setUp() throws Exception {
        System.out.println("\n\n\n\n=============================== START:" + getName());
        dirWatcher = new DirWatcher(null, new File("build"));
        dirWatcher.registerEventHandler(this);
        parent = new File("build", getName());
        FileUtil.deleteDir(parent);
        parent.mkdir();
    }

    @Override
    protected void tearDown() throws Exception {
        FileUtil.deleteDir(parent);
    }

    public void testShouldDetectNestedModifiedFileOnceOnly() throws Exception {
        dirWatcher.run();
        int beforeCreateFileInSubdir = files.size();

        File child = new File(parent, "YYYYY");
        PrintWriter writer = new PrintWriter(child);
        writer.println("doing stuff!!!!");
        writer.flush();

        dirWatcher.run();
        assertEquals(beforeCreateFileInSubdir + 1, files.size());

        Thread.sleep(1500);
        writer.println("doing Other Stuff!!!!");
        writer.flush();
        writer.close();

        verifyModified();
        Thread.sleep(1500);
        verifyModified();
    }

    private void verifyModified() throws InterruptedException {
        dirWatcher.run();
        assertEquals(1, modified.size());
        assertEquals(1, modifiedCount);
    }


    public void testShouldCreateEventWhenNestedCreated() throws Exception {
        dirWatcher.run();
        int firstCount = files.size();

        File child = new File(parent, "XXXXXX");
        child.createNewFile();
        child.deleteOnExit();

        dirWatcher.run();
        assertEquals(firstCount + 1, files.size());
        assertTrue(files.toString().contains(child.getName()));
    }


    public void testShouldDetectNestedDeleteEvent() throws Exception {
        dirWatcher.run();
        int firstCount = files.size();

        File child = new File(parent, "XXXXXX");
        child.createNewFile();
        child.deleteOnExit();


        dirWatcher.run();
        assertEquals(firstCount + 1, files.size());

        // now delete the file
        child.delete();

        dirWatcher.run();

        assertEquals(1, deleteCount);


    }


    public void created(File file) {
        this.files.add(file);

    }

    public void deleted(File file) {
        this.files.remove(file);
        this.modified.remove(file);
        System.out.println(" ************ DeletedCount = " + deleteCount);
        deleteCount++;
    }

    public void modified(File file) {
        this.files.add(file);
        this.modified.add(file);
        modifiedCount++;

    }

    public void stop() {
    }

}
