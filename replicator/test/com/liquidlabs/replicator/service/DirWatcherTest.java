package com.liquidlabs.replicator.service;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;

public class DirWatcherTest extends TestCase implements DirEventHandler {

    private DirWatcher dirWatcher;
    ArrayList<File> files = new ArrayList<File>();
    ArrayList<File> modified = new ArrayList<File>();
    private int deleteCount;

    @Override
    protected void setUp() throws Exception {

        System.out.println("\n\n\n\n=============================== START:" + getName());
        dirWatcher = new DirWatcher(null, new File("build"));
        dirWatcher.registerEventHandler(this);
    }

    @Override
    protected void tearDown() throws Exception {
    }

    public void testShouldDetectModifiedFileOnceOnly() throws Exception {
        File noModFile = new File("build", "NOMOD_" + getName());
        noModFile.createNewFile();
        noModFile.deleteOnExit();

        File file = new File("build", getName());
        file.createNewFile();
        file.deleteOnExit();

        Thread.sleep(1500);
        dirWatcher.run();

        Thread.sleep(1500);
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("doing stuff!!!!".getBytes());
        fos.flush();
        fos.close();
        dirWatcher.run();
        assertEquals(1, modified.size());

        // make sure we dont get it 2x
        Thread.sleep(1500);
        dirWatcher.run();
        assertEquals(1, modified.size());
    }

    public void testShouldIgnoreDotFile() throws Exception {
        dirWatcher.run();
        int firstCount = files.size();

        File file = new File("build", "." + getName());
        file.createNewFile();
        file.deleteOnExit();

        dirWatcher.run();

        assertEquals(firstCount, files.size());
    }
    
    public void testShouldIgnoreBakFiles() throws Exception {
        dirWatcher.run();
        int firstCount = files.size();

        File file = new File("build", "foo.bak" + getName());
        file.createNewFile();
        file.deleteOnExit();

        dirWatcher.run();

        assertEquals(firstCount, files.size());
    }

    public void testCreateEventWhenCreated() throws Exception {
        dirWatcher.run();
        int firstCount = files.size();

        File file = new File("build", getName());
        file.createNewFile();
        file.deleteOnExit();

        dirWatcher.run();

        assertEquals(firstCount + 1, files.size());
        assertTrue(files.toString().contains(getName()));


    }

    public void testShouldDeleteEventDeleted() throws Exception {
        File file = new File("build", getName());
        file.createNewFile();
        file.deleteOnExit();

        dirWatcher.run();
        int firstCount = files.size();

        file.delete();
        dirWatcher.run();

        assertEquals(firstCount - 1, files.size());

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

    }

    public void stop() {
    }

}
