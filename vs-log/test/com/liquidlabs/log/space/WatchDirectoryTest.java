package com.liquidlabs.log.space;

import com.liquidlabs.admin.DataGroup;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.roll.NullFileSorter;
import com.liquidlabs.log.util.DateTimeExtractor;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import org.apache.commons.io.FilenameUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class WatchDirectoryTest {

    private boolean isDW = false;
    private String breakRule = "";
    private String tags = "";
    private String fileTag = "";
    private boolean grokItEnabled = false;
    private boolean systemFieldEnabled;

    @Test
    public void testIsHostsMatching() throws Exception {
        WatchDirectory wd = new WatchDirectory("test1", "/", "*log*", null, "", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        //match everything
        assertTrue(wd.isHostMatch(""));
        wd.setHosts("matches");

        // simple
        assertTrue(wd.isHostMatch("matches"));
        assertFalse(wd.isHostMatch("nooo"));

        // multiple
        wd.setHosts("a,b,c,d,e");
        assertTrue(wd.isHostMatch("c"));
        assertFalse(wd.isHostMatch("f"));

        // substring
        wd.setHosts("match");
        assertTrue(wd.isHostMatch("this_is_a_match"));
    }

    @Test
    public void shouldMatchWildCard() throws Exception {
        WatchDirectory wd = new WatchDirectory("subStringTest", "work/LogServer_SERVER_/*EX6*/C/Roller/logs", "*", null, "", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        wd.setHosts("EX6");
        assertTrue(wd.isHostMatch("EX672"));
    }
    @Test
    public void shouldMatchAegon() throws Exception {
        WatchDirectory wd = new WatchDirectory("test1", "/", "*log*", null, "", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        wd.setHosts("uklnxpl0125.nl.aegon.com,uklnxul0125.nl.aegon.com");
        assertTrue(wd.isHostMatch("uklnxpl0125.nl.aegon.com"));

    }

    @Test
    public void shouldntMatchDifferentHosts() throws Exception{
        WatchDirectory wd = new WatchDirectory("test1", "/", "*log*", null, "", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        wd.setHosts("EX672");
        assertFalse(wd.isHostMatch("alteredcarbon.local"));
    }

    @Test
    public void testIsMatchingDataGroup() throws Exception {
        DataGroup simple = new DataGroup("simple", "tag:yes", "tag:no", "", true, "");
        WatchDirectory wd = new WatchDirectory("test1", "/", "*log*", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);

        assertFalse("Complete mismatch", wd.isDataGroupMatch(simple));

        wd.setTags("yes");
        assertTrue("Complete Include", wd.isDataGroupMatch(simple));

        wd.setTags("no");
        assertFalse("Complete Exclude", wd.isDataGroupMatch(simple));

    }


    @Test
    public void testIsWatchingFwdedHost() throws Exception {
        String path = "/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/work/LogServer_SERVER_/CRAP-VM/C/LOGS/Delta/ems/EmsDataTruncated.1.log.gz";
        path = FileUtil.makePathNative(path);
        WatchDirectory wd = new WatchDirectory("test1", path, "*log*", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        wd.hosts = "CRAP";
        assertTrue("Extracted Host:" + LogProperties.getHostFromPath(path), wd.matchesHost(LogProperties.getHostFromPath(path)));
    }

    @Test
    public void testIsWatchingFwded() throws Exception {
        String path ="C:\\LOGS\\Delta\\Level_*\\Jvm_*,/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/C:\\LOGS\\Delta\\Level_*\\Jvm_*,_ABS_GENERATED_,/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/work/LogServer_SERVER_/neil-vm/C/LOGS/Delta/Level_*/Jvm_*,/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/work/LogServer_SERVER_/neil-vm/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/C/LOGS/Delta/Level_*/Jvm_*,/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/work/LogServer_SERVER_/neil-vm/_ABS_GENERATED_ ";
        String file = "/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/work/LogServer_SERVER_/neil-vm/C/LOGS/Delta/Level_1/Jvm_1/SeatV6.log.10";
        WatchDirectory wd = new WatchDirectory("test1", path, "*log*", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        System.out.println("Path:" + wd.getDirName());
        assertTrue(wd.isWatching(file, "test1"));

    }


    @Test
    public void testIsWatchingFwded1() throws Exception {

        String path ="/opt/stuff/crap";
        WatchDirectory wd = new WatchDirectory("test1", path, "*log*", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);

        String file = "/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/work/LogServer_SERVER_/neil-vm/opt/stuff/crap/SeatV6.log.10";
        assertTrue(wd.isWatching(file, "test1"));

    }

    @Test
    public void testIsWatchingWindowsFile() throws Exception {
        String path1= "C:\\LOGS\\Delta\\Jvm_*";
        String path = "C:\\LOGS\\Delta\\Jvm_*,_ABS_GENERATED";
        String filename = "C:\\LOGS\\Delta\\Jvm_1\\FlightDetailsService.log.1";
        WatchDirectory wd = new WatchDirectory("test1", path, "*log*", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        assertTrue(wd.isWatching(filename, "test1"));
    }


    @Test
    public void testWatchingSysLogIsObeyed() throws Exception {
        String filename = "/var/log/windowserver.log";
        WatchDirectory wd = new WatchDirectory("NOO", ".,!App,!SocketServer,!LogServer,/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape,!/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/App,!/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/SocketServer,!/WORK/LOGSCAPE/TRUNK/LogScape/master/build/logscape/LogServer,_ABS_GENERATED_", "*.log*", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        assertFalse(wd.isWatching(filename, "test1"));
    }


    @Test
    public void testWatchingThisIsGood() throws Exception {
        String filename = "/Volumes/Media/LabManagerCopy/logscape/work/schedule/13Feb01-schedule-prod-support.log";
        filename = FileUtil.makePathNative(filename);
        if (!File.separator.equals("/")){
            String ppp = new File(".").getAbsolutePath();
            String drive = ppp.substring(0,1);

            filename = drive + ":\\" + filename;

            String norm = FilenameUtils.normalize(filename);
            String sss = FileUtil.makePathNative(filename);



        }
        WatchDirectory wd = new WatchDirectory("test1", "/Volumes/Media/LabManagerCopy/logscape/work/**", "*", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        wd.makeDirsAbsolute();

        long firstTimeMs = new DateTimeExtractor(wd.getTimeFormat()).getDateTime(FileUtil.getLine(new File(filename),1)).getMillis();

        assertFalse(wd.isTooOld(firstTimeMs));
        assertTrue(wd.isWatching(filename, "test1"));//&& !wd.isTooOld(logFile.getEndTime());
    }

    @Test
    public void shouldMakeDirsAbsolute() throws Exception {
        String before = ".,work";
        WatchDirectory dir = new WatchDirectory("test1", before, "*", null,"", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        String beforePath = dir.getDirName();
        dir.makeDirsAbsolute();
        String afterPath = dir.getDirName();
        dir.makeDirsAbsolute();
        String afterPath2 = dir.getDirName();
        assertTrue(dir.getDirName().contains(before));
        assertEquals("Should not have mutated the path", afterPath, afterPath2);


    }

    @Test
    public void shouldNotBeEquals() throws Exception {
        WatchDirectory watchDir1 = new WatchDirectory("test1", "D:\\work\\temp\\two", "*", null, NetworkUtils.getHostname() + ", lonrs0445", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        WatchDirectory watchDir2 = new WatchDirectory("test1", "D:\\work\\temp\\two", "*,!webapp", null, NetworkUtils.getHostname() + ", lonrs0445", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        assertFalse(watchDir1.equals(watchDir2));

    }

    @Test
    public void shouldMatchHost() throws Exception {
        WatchDirectory watchDir = new WatchDirectory("test1", "D:\\work\\temp\\two", "*", null, NetworkUtils.getHostname() + ", lonrs0445", 999, "", false, breakRule, null, grokItEnabled, systemFieldEnabled);
        assertTrue(watchDir.matchesHost(NetworkUtils.getHostname().toUpperCase()));
    }

    @Test
    public void shouldDeleteOldFileAndNotNewOne() throws Exception {

        WatchDirectory watchDirectory = new WatchDirectory(tags, ".","*,*,*WATCH","","", 7, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);

        File file = new File("build", "WATCH_ME");
        file.createNewFile();

        LogFile logFile = new LogFile("buid/WATCH_ME", 0, "FieldSet", "tag");

        logFile.setEndTime(DateTimeUtils.currentTimeMillis());

        assertFalse(watchDirectory.canDelete(logFile));

        logFile.setEndTime(new DateTime().minusDays(8).getMillis());

        assertTrue(watchDirectory.canDelete(logFile));

    }

    @Test
    public void shouldNotWatchFileWhenAnIncludeWatchesButHasEXcludeFilter() throws Exception {
        File parent = new File("build", "OSSEC");
        parent.mkdirs();
        File file = new File(parent,"data.log");
        file.createNewFile();
        try {

            String fullFilePath = file.getAbsolutePath();
            System.out.println("FullFilePAth:" + fullFilePath);
            WatchDirectory watchDirectory2 = new WatchDirectory(tags, "build,!OSSEC","*,*OSSEC*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
            watchDirectory2.makeDirsAbsolute();
            assertFalse("Should NOT Watch:" + fullFilePath, watchDirectory2.isWatching(fullFilePath, fileTag));

            WatchDirectory watchDirectory = new WatchDirectory(tags, ".,!OSSEC","*,*OSSEC*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
            watchDirectory2.makeDirsAbsolute();
            assertFalse("ShouldBeFalse:" + fullFilePath, watchDirectory.isWatching(fullFilePath, fileTag));




        } finally {
            file.delete();
        }
    }


    @Test
    public void shouldWatchFileWhereItsBeenIgnoredAlready() throws Exception {
        File parent = new File("build", "OSSEC-alert");
        parent.mkdirs();
        File file = new File(parent,"data.log");
        file.createNewFile();
        try {

            String fullFilePath = file.getAbsolutePath();
            System.out.println("FullFilePAth:" + fullFilePath);
            WatchDirectory watchDirectory = new WatchDirectory(tags, "build/OSSEC-alert","*,*,!data","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
            assertFalse("Should not be watching", watchDirectory.isWatching(fullFilePath, fileTag));

            WatchDirectory watchDirectory2 = new WatchDirectory(tags, "build/OSSEC-alert","*data*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
            assertTrue(watchDirectory2.isWatching(fullFilePath, fileTag));

        } finally {
            file.delete();
        }
    }

    @Test
    public void shouldEscapeSlashCharsWhenBeingUsedForEOLN() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, ".","*.csv,*.log,!agent","","", 1, "", isDW, "Explicit:\\r\\n", "", grokItEnabled, systemFieldEnabled);
        assertEquals("Explicit:\r\n",watchDirectory.getBreakRule());

    }

    @Test
    public void shouldNotWatchThisFile() throws Exception {

        File file = new File("build","ffl-me.log");
        file.createNewFile();
        String fullPath = new File("build").getAbsolutePath();

        WatchDirectory exclude = new WatchDirectory(tags, fullPath,"*.log,!ffl*.log","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertFalse("Should not watch:" + file.getAbsolutePath(), exclude.isWatching(file.getAbsolutePath(), fileTag));

        WatchDirectory exclude2 = new WatchDirectory(tags, fullPath,"*.log,!*ffl*.log","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertFalse("Should not watch:" + file.getAbsolutePath(), exclude2.isWatching(file.getAbsolutePath(), fileTag));

        WatchDirectory include = new WatchDirectory(tags, fullPath,"*.log","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertTrue("Should Watch:" + file.getAbsolutePath(), include.isWatching(file.getAbsolutePath(), fileTag));
    }

    @Test
    public void shouldNotWatchThisAppWhereItMightBeIncluded() throws Exception {

        String dirrr = "./work";
        String substring = dirrr.substring(2);
        System.out.println(substring);

        WatchDirectory dir = new WatchDirectory(tags, "./work/UnixApp-1.0/*,./work/LogServer_Server/**/UnixApp-1.0/*","*.out,*.err","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        dir.makeDirsAbsolute();;
        String path = new File("").getAbsolutePath();
        assertTrue("Should watch:", dir.isWatching(path + "/work/UnixApp-1.0/13Jan29/UNIX_CPU_wHOST_wTSTAMP.out", "xxx"));
        //WatchDirectory dir = new WatchDirectory(tags, "UnixApp,!App","*.out,*.err","","", true, 1, isDW, breakRule, "");
        assertFalse("Should watch:", dir.isWatching(new File("./work/UnixApp-1.0/XXXX.out").getPath(), "xxx"));
        assertTrue("Should watch:", dir.isWatching(new File("./work/LogServer_Server/host/opt/logscape/work/UnixApp-1.0/13July12/XXXX.out").getPath(), "xxx"));
        assertFalse("Should watch:", dir.isWatching(new File("./work/LogServer_Server/host/opt/logscape/work/WindowsApp-1.0/13July12/XXXX.out").getPath(), "xxx"));
    }

    @Test
    public void shouldNotWatchThisDIR() throws Exception {

        WatchDirectory dir = new WatchDirectory(tags, "./work,!App,!webapp","*.evt","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertTrue(dir.isWatching(new File("./work/hello.evt").getPath(), fileTag));
        // THIS IS A BUG - We need this and the test above to also passsss
        assertFalse(dir.isWatching(new File("./work/webapp/subDir/file.evt").getPath(), fileTag));
        assertFalse("Should watch:", dir.isWatching(new File("/Volumes/Media/LabManagerCopy/home/logscape/logscape/work/UnixApp-1.0/13Jan29/UNIX_CPU_wHOST_wTSTAMP.out").getPath(), "xxx"));
    }
    @Test
    public void shouldnotWatchPathOnSubDir() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "./work/UnixApp,!webapp,!*otherDir*","*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue(watchDirectory.isWatching(new File("./work/UnixApp/file.log").getPath(), fileTag));
        assertFalse(watchDirectory.isWatching(new File("./work/webapp/subDir/file.log").getPath(), fileTag));
        assertFalse(watchDirectory.isWatching(new File("./work/LogServer_SERVER_/host/webapp/file.log").getPath(), fileTag));
    }
    @Test
    public void shouldNotWatchFile() throws Exception {
        String fullFilePath = FileUtil.getPath(new File("test-data","agent.log"));
        WatchDirectory watchDirectory = new WatchDirectory(tags, ".","*.csv,*.log,!agent","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertFalse(watchDirectory.isWatching(fullFilePath, fileTag));

        fullFilePath = FileUtil.getPath(new File("test-data","agent.log"));
        watchDirectory = new WatchDirectory(tags, "test-data","*.csv,!xlog,*agent*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertTrue(watchDirectory.isWatching(fullFilePath, fileTag));

        fullFilePath = FileUtil.getPath(new File("test-data","agent.log"));
        watchDirectory = new WatchDirectory(tags, "test-data","*,!dqx","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertTrue(watchDirectory.isWatching(fullFilePath, fileTag));
    }

    @Test
    public void shouldWatchMultipleEntries() throws Exception {
        String fullFilePath = FileUtil.getPath(new File("test-data","agent.log"));
        WatchDirectory watchDirectory = new WatchDirectory(tags, "test-data","*.csv,*.log","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertTrue(watchDirectory.isWatching(fullFilePath, fileTag));
        assertTrue(watchDirectory.isWatching(fullFilePath, fileTag));
    }

    @Test
    public void shouldNotWatchTooOld() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "E:\\work\\rbs","sum-test.log.*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        assertTrue(watchDirectory.isTooOld(new DateTime().minusDays(2).getMillis()));
        assertFalse(watchDirectory.isTooOld(new DateTime().getMillis()));
    }

    @Test
    public void shouldWatchSimple() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "/work/rbs","sum-test.log","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue("Test Works alone on windows - needs checking on OSX!", watchDirectory.isWatching(new File("/work/rbs/sum-test.log").getPath(), fileTag));
        //assertFalse(watchDirectory.isWatching(new File("/work/rbs/nosum-test.log").getPath(), fileTag));
    }


    @Test
    public void shouldWatchWhenExplicitFile() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "C:/DIR/stuff","*file.log","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue(watchDirectory.isWatching(new File("C:/DIR/stuff/file.log").getPath(), fileTag));
    }

    @Test
    public void shouldWatchWhenExplicitFileAndNoRecurseSpecified() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "C:/DIR/stuff","*file.log*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue(watchDirectory.isWatching(new File("C:/DIR/stuff/file.log").getPath(), fileTag));
        assertTrue(watchDirectory.isWatching(new File("C:/DIR/stuff/file.log.1").getPath(), fileTag));
        assertFalse(watchDirectory.isWatching(new File("C:/DIR/stuff/file.lAg.1").getPath(), fileTag));
    }

    @Test
    public void shouldWatchWhenFileWithROLLAndNoRecurseSpecified() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "C:/DIR/stuff","file.log*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue(watchDirectory.isWatching(new File("C:/DIR/stuff/file.log").getPath(), fileTag));
        assertTrue(watchDirectory.isWatching(new File("C:/DIR/stuff/file.log.1").getPath(), fileTag));
        assertFalse(watchDirectory.isWatching(new File("C:/DIR/stuff/file.lAg.1").getPath(), fileTag));
    }

    @Test
    public void shouldWatchRelativePath() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "./**","*.log*","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue(watchDirectory.isWatching(new File("./DIR/stuff/file.log").getPath(), fileTag));
        assertFalse(watchDirectory.isWatching(new File("./DIR/stuff/file.txt").getPath(), fileTag));
    }
    @Test
    public void shouldWatchFilenameExact() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "/var/log","secure","","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue(watchDirectory.fileNameMatches("secure"));
        assertTrue(watchDirectory.isWatching("/var/log/secure",""));
    }

    @Test
    public void shouldDetermineWatching() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "./DIR/stuff",".*file.*","time","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue(watchDirectory.isWatching("./DIR/stuff/file", fileTag));
        assertFalse(watchDirectory.isWatching("./DIR/stuff/fFFF", fileTag));
    }


    @Test
    public void shouldDetermineWatching2() throws Exception {
        WatchDirectory watchDirectory = new WatchDirectory(tags, "./DIR*/stuff",".*file.*","time","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        System.out.println("Watch:" + watchDirectory);
        assertTrue(watchDirectory.isWatching("./DIR/stuff/file", fileTag));
        assertTrue(watchDirectory.isWatching("./DIR2/stuff/file", fileTag));
        assertFalse(watchDirectory.isWatching("./DIR/stuff/fFFF", fileTag));
        assertFalse(watchDirectory.isWatching("./DIR2/stuff/fFFF", fileTag));
    }


    @Test
    public void testShouldBeAbleToUseSchema() throws Exception {
        ObjectTranslator objectTranslator = new ObjectTranslator();
        WatchDirectory watchDirectory = new WatchDirectory(tags, "DIR","filePattern","time","", 1, "", isDW, breakRule, "", grokItEnabled, systemFieldEnabled);
        watchDirectory.setFileSorter(new NullFileSorter());
        String stringFromObject = objectTranslator.getStringFromObject(watchDirectory);
        WatchDirectory objectFromFormat = objectTranslator.getObjectFromFormat(WatchDirectory.class, stringFromObject);

        assertEquals(watchDirectory, objectFromFormat);
    }
}
