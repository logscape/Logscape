package com.liquidlabs.log.space;

import static org.junit.Assert.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Map;

public class WatchDirectoryTrackerTest {

    private String breakRule = "";
    private String tags = "";
    private String fileTag = "";
    private boolean grokItEnabled = false;
    private boolean systemFieldEnabled;

    @Test
    public void testIsWatchingFwdedHost() throws Exception {
        // ./work/WindowsApp-1.0/*,./work/LogServer_SERVER_/**/WindowsApp-1.0/*
        // >*.out,*.err
        String root = "./build/TrackWindowsApp-1.0/";
        new File(root).mkdirs();
        PrintWriter fos = new PrintWriter(new File(root +  "/file1.log"));
        fos.write("one\ntwo\nthree\n");
        fos.close();

        FileOutputStream fos2 = new FileOutputStream(new File(root +  "/file2.log"));
        fos2.write("one\ntwo\nthree\n".getBytes());
        fos2.close();


        WatchDirectory wd = new WatchDirectory("test1", root +  "*", "*.out,*.err,*.log", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        wd.makeDirsAbsolute();

        WatchDirectoryTracker tracker = wd.getTracker();
        Map<String,Long> stringLongMap = tracker.lastModifiedDirs();
//        System.err.println("Modd:" + stringLongMap);
        assertEquals(1, stringLongMap.size());


    }

}
