package com.liquidlabs.common.file;

import com.liquidlabs.common.collection.Arrays;
import jregex.util.io.PathPattern;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;


public class FileUtilTest {
    @Test
    public void testShouldMatchPathPAndEXcludeThis() throws Exception {
        String path = "**,!App-";
        new File("FileUtilTest/isIncludes/file.log").mkdirs();
        new File("FileUtilTest/isExcludedApp-1.0/file.log").mkdirs();
//        assertTrue(FileUtil.isPathMatch(false, path, "FileUtilTest/isIncludes"));
        assertFalse(FileUtil.isPathMatch(false, path, "FileUtilTest/isExcludedApp-1.0"));
    }

    @Test
    public void testShouldMatchPathP() throws Exception {
        String cleanMyPath = "D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-FWD\\logscape\\work\\LogServer_SERVER_/*/**/logscape/work,D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-FWD\\logscape\\work\\LogServer_SERVER_/*/**/logscape/work/*,D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-FWD\\logscape\\work\\LogServer_SERVER_/*/D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-FWD\\logscape,D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-FWD\\logscape\\work\\LogServer_SERVER_/*/D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-FWD\\logscape\\work,D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-FWD\\logscape\\work\\LogServer_SERVER_/*/D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-FWD\\logscape\\work\\*,";
        String replace = FileUtil.cleanPath(cleanMyPath).replace(",", ",\n");
    }

    @Test
    public void testShouldMatchPath() throws Exception {
        String path = ".,./work,./work/*,D:\\work\\LOGSCAPE\\Logscape_250\\master\\build\\logscape,D:\\work\\LOGSCAPE\\Logscape_250\\master\\build\\logscape\\work,D:\\work\\LOGSCAPE\\Logscape_250\\master\\build\\logscape\\work\\*,_ABS_GENERATED_";
        String dir = "D:\\work\\LOGSCAPE\\Logscape_250\\master\\build\\logscape\\work\\";
        assertTrue(FileUtil.isPathMatch(true, path, dir));
    }

    @Test
    public void testShouldMaintainDropDotsOnWindwos() throws Exception {
        String path = "D\\work\\LOGSCAPE\\Logscape_250\\master\\build-DL\\logscape\\work\\.\\LogServer_SERVER_";
        String pp = FileUtil.cleanPath(path);
        assertTrue(pp.indexOf("\\.\\") == -1);
    }

    @Test
    public void testShouldMaintainDriveLetterOnWindows() throws Exception {
        String path = "D:\\work\\LOGSCAPE\\Logscape_250\\master\\build-DL\\logscape\\work\\.\\LogServer_SERVER_\\stuff\\D:\\xxx";
        String pp = FileUtil.cleanupPathAndMakeNative(path);
        assertTrue(pp.substring(0,1).equals("D"));
        assertTrue(pp.substring(1,2).equals("/"));
    }



    @Test
    public void testRemoveDIR() throws Exception {
        String path = "/WORK/logs/LabLogs/TEMP_DEL/KV_INDEX";
        FileUtil.deleteDir(new File(path));
    }



    @Test
    public void testShouldWorkwithWindowsRelative() throws Exception {
        String path = ".\\work\\DotNetApp-1.0\\*,/home/logscape/drops/logscape/\\work\\DotNetApp-1.0\\*";
        String dir = "/home/logscape/drops/logscape/work/DotNetApp-1.0/13July12";
        assertTrue(FileUtil.isPathMatch(true, path, dir));
    }


    @Test
    public void testShouldWorkwithWindows() throws Exception {
        String path = "./work/WindowsApp-1.0/*";
        String dir = new File("").getAbsolutePath() +  "\\work\\WindowsApp-1.0\\13Aug24";
        assertTrue(FileUtil.isPathMatch(true, path, dir));
    }

    @Test
    public void testEEEWORKS() throws Exception {
        PathPattern pp=new PathPattern("/var/l*g/*log*");
        File[] files = pp.files();
        for (File file : files) {
            System.out.println(file.getAbsolutePath());
        }

    }
    @Test
    public void testShouldNotMatchPathNestedType() throws Exception {
        String path = ".,./work";
        String dir = "/home/logscape/logscape/work/schedule/";
        assertFalse(FileUtil.isPathMatch(false, path, dir));
    }

    @Test
    public void testShouldMatchFullFwddPath() throws Exception {
        String path = "/home/logscape/logscape/work/SysLogServer_SERVER_/10.156.250.1/opt/logs/stuff";
        String dir ="/opt/logs/stuff";
        assertTrue(FileUtil.isPathMatch(false, path, dir));
    }

    @Test
    public void shouldMatchWildCardedHost() throws Exception {
        String path = "/home/logscape/logscape/work/SysLogServer_SERVER_/*EX6*/opt/logs/stuff";
        String dir = "/home/logscape/logscape/work/SysLogServer_SERVER_/EX672/opt/logs/stuff";
        assertTrue(FileUtil.isPathMatch(false, path, dir));
    }

    @Test
    public void testShouldMatchFWDWindows() throws Exception {
        String path = "C:\\logscapework\\LogServer_SERVER_\\10.156.250.1\\C\\opt\\logs\\stuff";
        String dir ="C:\\opt\\logs\\stuff";
        assertTrue(FileUtil.isPathMatch(false, path, dir));
    }

    @Test
    public void testShouldMatchMixedFwddPath() throws Exception {
        String path = "/home/logscape/logscape/work/SysLogServer_SERVER_/vm-1/C/opt/logs/stuff";
        String dir ="C:\\opt\\logs\\stuff";
        assertTrue(FileUtil.isPathMatch(true, path, dir));
    }
    @Test
    public void testShouldMatchMixedFwddPathWildWIN() throws Exception {
        String path = "/home/logscape/logscape/work/LogServer_SERVER_/vm-1/c/opt/logs/stuff/2019*";
        assertTrue(FileUtil.isPathMatch(true, path, "c:\\opt\\logs\\stuff\\2019-ME"));
    }

    @Test
    public void testShouldMatchFwddPathWildWIN1() throws Exception {
        String path = "D:\\work\\LOGSCAPE\\TRUNK\\master\\build-dir\\logscape\\work\\LogServer_SERVER_\\*\\D\\opt\\logs";
        PathPattern pp = new PathPattern(path);
        Enumeration enumeration = pp.enumerateFiles();
        while (enumeration.hasMoreElements()) {
            File file = (File) enumeration.nextElement();
            System.out.println(file.getAbsolutePath());

        }

    }




    @Test
    public void testShouldMatchRelativeWildcardPath() throws Exception {
        String parent = new File(".").getAbsolutePath();
        parent = parent.substring(0, parent.length()-2);

        String path = "./work/**/10.156.250.1/*";
        String dir = parent + "/work/crap/crap/10.156.250.1/13Jul31";
//        String path = ".,./work/*,./work/*/*,!App,!_SERVER_,/WORK/LOGSCAPE/TRUNK/TEMP/logscape,/WORK/LOGSCAPE/TRUNK/TEMP/logscape/work/*,/WORK/LOGSCAPE/TRUNK/TEMP/logscape/work/*/*,!/WORK/LOGSCAPE/TRUNK/TEMP/logscape/App,!/WORK/LOGSCAPE/TRUNK/TEMP/logscape/_SERVER_,_ABS_GENERATED_";
//        String dir = "/WORK/logs/RBS/ODC/";
        assertTrue(FileUtil.isPathMatch(true, path, dir));
    }

    @Test
    public void testRelativePathWORKS() throws Exception {
        String dir = "C:\\LOGS\\Delta\\Jvm_1";
        assertFalse(FileUtil.isPathMatch(false, ".", dir));
    }


    @Test
    public void testWindowsWildcardWORKS() throws Exception {
        String dir = "C:\\LOGS\\Delta\\Jvm_1";
        assertTrue(FileUtil.isPathMatch(false, "C:\\LOGS\\Delta\\Jvm_*", dir));
    }


    @Test
    public void testPathWildcardWORKS() throws Exception {
        String dir = "/TRUNK_1/LogScape_2/";

        assertFalse(FileUtil.isPathMatch(false, "/TRUNK_2/LogScape_3/", dir));

        assertTrue(FileUtil.isPathMatch(false, "/TRUNK_*/LogScape_*/", dir));

        String dirDrive = "C:\\TRUNK_1\\LogScape_2\\";

        assertTrue(FileUtil.isPathMatch(true, "C:\\TRUNK_*\\LogScape_*\\", dirDrive ));
    }

    @Test
    public void testPathExcludedWORKS() throws Exception {
        String dir = "/WORK/LOGSCAPE/TRUNK/LogScape/vs-log/build/wmgr-TAG/SysLog_SERVER_/host";
//        WatchDirectory wd = new WatchDirectory("NOO", ".,!_SERVER_", "*", null,"", true, 999, false, breakRule, null);
        String dirFilter = "/WORK/LOGSCAPE/TRUNK/LogScape/vs-log,!_SERVER,!/WORK/LOGSCAPE/TRUNK/LogScape/vs-log/_SERVER,_ABS_GENERATED_";
        //WatchDirectory wd2 = new WatchDirectory("NOO", dirFilter, "*", null,"", true, 999, false, breakRule, null);
        assertFalse(FileUtil.isPathMatch(true, dirFilter, dir));
    }
    @Test
    public void testGetPos() throws Exception {
        long[] lineAndSeekFromEnd = FileUtil.getLineAndPosSeekFromEnd(new File("D:\\work\\logs\\weblogs\\liquidlabs-cloud.com-Aug-2010"), 100);
        System.out.println("ppp:" + lineAndSeekFromEnd);
    }


    @Test
    public void testIgnoreStar() throws Exception {
        String s = FileUtil.replaceStar("./work/WindowsApp-1.0*");
        assertEquals("./work/WindowsApp-1.0",s);
    }

    @Test
    public void shouldDetectDirecoryUsingDOT() throws Exception {
        File file = new File("build/TEST-DIR");
        file.mkdirs();
        File file2 = new File(file,"isFile.log");
        file2.createNewFile();
        assertTrue(FileUtil.isDirectory(file));
        assertFalse(FileUtil.isDirectory(file2));


    }
//
//	@Test
//	public void shouldGetFileAndParent() throws Exception {
//		String filename =  File.separator + "var" + File.separator + "stuff" + File.separator + "file.log";
//		assertEquals(new File(filename).getName(), FileUtil.getFileNameOnly(filename));
//		assertEquals(new File(filename).getParent(), FileUtil.getParentFile(filename));
//
//		String winFile = "c:\\work\\stuff.log";
//		assertEquals("stuff.log", FileUtil.getFilenameOnlyWin(winFile));
//
//		String unixFile = "/var/boo/stuff.log";
//		assertEquals("stuff.log", FileUtil.getFilenameOnlyUnix(unixFile));
//
//	}

    @Test
    public void shouldCopySubDirectory() throws Exception {
        new File("build/crap1/crap2").delete();
        boolean mkdirs = new File("build/crap1/crap2").mkdirs();
        assertTrue(mkdirs);

        FileUtil.deleteDir(new File("build/subDirCopyTest"));
        FileUtil.copyDir("test-data/subDirCopyTest", "build/subDirCopyTest");
        assertTrue(new File("build/subDirCopyTest").exists());
        assertTrue(new File("build/subDirCopyTest/dir1/file1.txt").exists());
        assertTrue(new File("build/subDirCopyTest/dir2/file2.txt").exists());
        assertTrue(new File("build/subDirCopyTest/file3.txt").exists());
    }

    @Test
    public void shouldAllowValidDotInDirPath() throws Exception {
        String filenname = "E:\\workspace_0.9\\master\\build\\logscape\\.\\work\\visit99.log";
        String path = FileUtil.getPath(new File(filenname));
        assertTrue(path.contains("0.9"));

    }

    @Test
    public void shouldFixPaths() throws Exception {
        String filenname = "/workspace/master/build/./logscape/./work/ntevents-app-10Jul09.evt";
        String path = FileUtil.getPath(new File(filenname));
        System.out.println("P" + path);
        assertEquals(2, path.split("\\.").length);

    }
    @Test
    public void shouldGetAbsolutPath() throws Exception {

        File file = new File("./build/stuff.log");
        String path = FileUtil.getPath(file);
        System.out.println("Path:" + path);
        assertEquals(path.length()-4, path.lastIndexOf("."));


    }

    @Test
    public void shouldReadLineCorrectly() throws Exception {
        String file = "build/fileutil-readline.txt";
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("line 1\n".getBytes());
        fos.write("line 2\n".getBytes());
        fos.write("line 3\n".getBytes());
        fos.close();
        assertEquals("line 1", FileUtil.getLine(new File(file), 1));
        assertEquals("line 2", FileUtil.getLine(new File(file), 2));
        assertEquals("line 3", FileUtil.getLine(new File(file), 3));

    }
//	@Test
//	public void shouldgetFilenameOnly() throws Exception {
//
//		assertEquals("stuff.log", FileUtil.getFilenameOnlyUnix("/opt/app/stuff.log"));
//		assertEquals("stuff.log", FileUtil.getFilenameOnlyWin("c:\\temp\\stuff.log"));
//
//	}

    @Test
    public void shouldMkNonExistentParentDir() throws Exception {
        String parent = "build/fileUtilTest/parent";
        String file = parent +  "/testFile.log";
        FileUtil.deleteDir(new File(parent));

        File mkdir = FileUtil.mkdir(new File(file).getParent());
        System.out.println("Made:" + mkdir);
        assertTrue(new File(parent).exists());

        // to be sure to be sure - write something
        FileOutputStream fos = new FileOutputStream(file);
        fos.write("crap".getBytes());
        fos.close();

    }

    @Test
    public void testShouldCountFileLines() throws Exception {
        int count = 100 * 1000;
        long start = DateTimeUtils.currentTimeMillis();
        writeFile(new File("build"), "countLines.txt", count);
//		long countLines2 = FileUtil.countLines(new File("build", "countLines.txt"))[1];
        long countLines2 = FileUtil.countLinesNEW2(new File("build", "countLines.txt"));
//		long[] countLines2 = FileUtil.countLines(new File("test-data", "lineCount.txt"));

        long end = DateTimeUtils.currentTimeMillis();
        System.out.println(getName() + " Elapsed:" + (end - start) + " lines:" + countLines2);
        assertEquals(count, countLines2);
        // 33723 - old
        // 31906 - new (9% quicker)
        // 29556 - new2
    }



    @Test
    public void testMakePathNativeIsGoodForWin() throws Exception {
        String path = "\\";
        path = path.replaceAll("\\/", "\\\\");
        path = path.replaceAll("\\\\\\\\", "\\\\");

        String[] split = Arrays.split("\\" + path, "c:\\stuff\\doit\\file.txt");
        assertEquals(4, split.length);

    }


    @Test
    public void testMkSingleFile() throws Exception {
        File file = FileUtil.mkdir("build/stuff1");
        assertNotNull(file);
        assertTrue(file.exists());
        assertTrue(file.isDirectory());
        file.delete();
    }
    @Test
    public void testMkSingleNestedFile() throws Exception {
        File file = FileUtil.mkdir("build/one/stuff2");
        assertNotNull(file);
        assertTrue(file.exists());
        assertTrue(file.isDirectory());
        file.delete();
        new File("build/one").delete();
    }


    @Test
    public void testShouldCopyFile() throws Exception {
        System.out.println("one:" + new DateTime(1239889689156L) );
        System.out.println("two:" + new DateTime(1239889689671L) );

        //	startTime=1239889689156 endTime=1239889689671

        FileOutputStream fos = new FileOutputStream("build/crap1.txt");
        for (int i = 0; i < 100 * 10 * 100; i++) {
            fos.write("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n".getBytes());
            fos.write(Integer.toString(i).getBytes());
        }
        fos.close();
        long start = DateTimeUtils.currentTimeMillis();

        File to = new File("build/crap2.txt");
        FileUtil.copyFile(new File("build/crap1.txt"), to);

        long end = DateTimeUtils.currentTimeMillis();

        System.out.println("Elapsed:" + (end - start));

        assertTrue(to.exists());
        assertTrue(to.length() > 100);;
    }

    @Test
    public void testDoesntBlowUpWhenDeletingNothing() throws Exception {
        int deleted = FileUtil.deleteDirUsingAge(1, TimeUnit.MINUTES, new File("build/" + getName()));
        assertEquals(0, deleted);
    }

    @Test
    public void testDeletesFileDirMinuteOld() throws Exception {
        File file = FileUtil.mkdir("build/" + getName());
        file.mkdirs();
        for (int i = 0; i < 10; i++) {
            String filename = "crap.txt";
            writeFile(file, i + filename, 1);
        }
        File dir = new File(file.getAbsolutePath());
        int deleted = FileUtil.deleteDirUsingAge(1000, TimeUnit.MILLISECONDS, dir);
        assertEquals(0, deleted);
        Thread.sleep(500);

        deleted = FileUtil.deleteDirUsingAge(1, TimeUnit.MILLISECONDS, new File(file.getAbsolutePath()));
        assertEquals(11, deleted);

        assertFalse(new File("build/" + getName()).exists());

    }
    private void writeFile(File file, String filename, int lines) throws FileNotFoundException, IOException {
        File file2 = new File(file, filename);
        FileOutputStream fos = new FileOutputStream(file2);
        for (int i = 0; i < lines; i++) {
            fos.write("stufffffffffffffffffffffffff\n".getBytes());
        }
        fos.close();
    }
    private String getName() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return FileUtilTest.class.getName() + DateTimeUtils.currentTimeMillis();
    }
}
