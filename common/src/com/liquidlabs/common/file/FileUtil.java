package com.liquidlabs.common.file;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.collection.cache.ConcurrentLRUCache;
import com.liquidlabs.common.compression.GzipCodec;
import com.liquidlabs.common.file.raf.ByteBufferRAF;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.common.util.OSUtils;
import jregex.WildcardPattern;
import jregex.util.io.PathPattern;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import java.io.*;
import java.io.FileFilter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

public class FileUtil {

    private static final Logger LOGGER = Logger.getLogger(FileUtil.class);
    public static final String SERVER_DIR_SEPARATOR = "_SERVER_";

    public static int MEGABYTES = 1024 * 1024;
    public static int GB = MEGABYTES * 1024;
    private static final char EOL = '\n';
    private static final String DOT = ".";
    public static String EOLN = System.getProperty("line.separator");

    public static String getFileNameOnly(String filenameAndPath) {
        return FilenameUtil.getName(filenameAndPath);
    }
    public static int getGzipExpandedBytes(File file) {
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(raf.length() - 4);
            int b4 = raf.read();
            int b3 = raf.read();
            int b2 = raf.read();
            int b1 = raf.read();
            raf.close();
            return (b1 << 24) | (b2 << 16) + (b3 << 8) + b4;

        } catch (Throwable t) {
            return -1;
        }
    }
    public static String getNamePartOnly(String file) {
        return FilenameUtil.getBaseName(file);
    }


    public static String getParentFile(String filename) {
        String filenameOnly = getFileNameOnly(filename);
        return filename.substring(0, filename.lastIndexOf(filenameOnly) - 1);
    }


    /**
     *
     * @param from
     * @param to
     * @return true when successfully rolled
     */
    // overcomes JDK bug where rename can sometimes fail due to
    // finalizers being stuffed
    public static boolean renameTo(File from, File to) throws IOException {

        if (to.getName().endsWith(".gz")) {
            GZIPOutputStream fos = new GZIPOutputStream(new FileOutputStream(to));
            FileInputStream fis = new FileInputStream(from);
            byte[] buffer = new byte[1024 * 1024];
            while (fis.available() != 0) {
                int amount = fis.read(buffer);
                if (amount > 0) fos.write(buffer, 0, amount);
            }
            fos.close();
            fis.close();;
            from.delete();
            return true;

        }

        int count = 0;
        boolean success = from.renameTo(to);
        try {
            while (!success && count++ < 20) {
                success = from.renameTo(to);
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
        }

        if (!success) {
            success = copyFile(from ,to);
            truncate(from);
        }

        return success;
    }

    /**
     * @return filePosition and lineCount
     */
    public static long[] countLines(File log) throws IOException {
        RAF raf = RafFactory.getRaf(log.getAbsolutePath(), "");
        try {
            int count = 0;
            while (raf.readLine() != null) {
                count++;
            }
            return new long[] { raf.length(), count };
        } finally {
            if (raf != null) raf.close();
        }
    }
    public static long[] countLinesSingle(File log) throws IOException {
        RAF raf = RafFactory.getRafSingleLine(log.getAbsolutePath(), "");
        try {
            int count = 0;
            while (raf.readLine() != null) {
                count++;
            }
            return new long[] { raf.length(), count };
        } finally {
            if (raf != null) raf.close();
        }
    }

    public static long countLinesNEW(File log) throws IOException {
        LineNumberReader reader = new LineNumberReader(new FileReader(log));
        while (reader.readLine() != null) {}
        reader.close();
        return reader.getLineNumber();
    }
    public static long countLinesNEW2(File log) throws IOException {
        BufferedInputStream is = new BufferedInputStream(new FileInputStream(log));
        byte[] c = new byte[4 * 1024];
        long count = 0;
        int readChars = 0;
        while ((readChars = is.read(c)) != -1) {
            for (int i = 0; i < readChars; i++) {
                if (c[i] == EOL) count++;
            }
        }
        is.close();
        return count;
    }

    public static File mkdir(String path) {
        if (path == null) {
            RuntimeException rte = new RuntimeException("FileUtil.mkdir given null path");
            rte.printStackTrace();
            return null;
        }
        if (new File(path).exists()) return new File(path);
        String seperator = File.separator;
        if (seperator.equals("\\")) seperator = "\\" + File.separator;
        String[] dir = Arrays.split(seperator, makePathNative(path));
        if (dir == null) throw new RuntimeException("Failed to createPath:" + path);
        StringBuilder sb = new StringBuilder();
        for (String string : dir) {
            sb.append(string);
            File file = new File(sb.toString());
            if (!file.exists()) file.mkdir();

            sb.append(File.separator);
        }
        return new File(path);
    }
    public static boolean isCompressedFile(String name) {
        return name.endsWith(".gz") || name.endsWith(".gzip") || name.endsWith(".bz") || name.endsWith(".bz2") || name.endsWith(".snap") || name.endsWith(".lz4");
    }

    public static String makePathNative(String path) {
        // nix
        if (File.separator.equals("/")) {
            if (path.contains("\\")) path = path.replaceAll("\\\\", File.separator);
        } else {
            if (path.contains("/")) {
                path = path.replaceAll("\\/", "\\\\");
                path = path.replaceAll("\\\\\\\\", "\\\\");
                if (path.startsWith("\\") && path.contains(":")) path = path.substring(1);
            }
            if (path.contains("\\\\")) path = StringUtils.replace(path, "\\\\", File.separator);
        }
        return path;
    }


    public static int deleteDirWithRetry(File dir) {
        int retry = 0;
        int maxRetries = 300;
        int result = 0;
        try {

            while (retry++ < maxRetries) {
                if ((result = deleteDir(dir)) > 0) {
                    // success
                    retry = maxRetries;
                }
                else  {
                    // failed to delete some files - wait and try again
                    Thread.sleep(1000);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }
    public static String getLineUsingSimple(File testfile, int lineNum) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(testfile));
        try {

            String line;
            int lineNumRead = 1;
            while ((line = br.readLine()) != null) {
                if (lineNumRead == lineNum) return line;
                lineNumRead++;
            }
        } finally {
            br.close();

        }
        return null;
    }
    public static int deleteDir(File dir) {
        int count = 0;
        int failed = 0;

        LOGGER.debug("Delete:" + dir);
        if (dir != null && dir.exists()) {
            File[] files = dir.listFiles();
            if (files!= null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        LOGGER.debug(count + " DeleteDir => " + file.getName());
                        count += FileUtil.deleteDir(file);
                    }
                    else {

                        boolean delete = file.delete();

                        if (delete) {
                            LOGGER.debug(count + " DeleteFile:" + file.getName());
                            count++;
                        } else {
                            LOGGER.debug(count + " DeleteFile:" + file.getName() + " LOCKED");
                            failed++;
                        }
                    }
                }
            }
            count++;
            LOGGER.debug(count+ " DeleteDIR:" + dir);
            dir.delete();
        }
        if (failed != 0) {
            LOGGER.debug("Failed!: " + failed);
            return failed * -1;
        }
        return count;
    }

    public static int deleteDirUsingAge(int maxAge, TimeUnit timeUnit, File... files) {
        DateTime maxTime = new DateTime().minusMillis((int)timeUnit.toMillis(maxAge));
        int count = 0;
        for (File file : files) {
            long lastMod = getAgeOfFile(file);
            if (lastMod < maxTime.getMillis()) {
                count += deleteDir(file);
            }
        }
        return count;
    }
    public static long getAgeOfFile(File file) {
        if (!file.exists()) return DateTimeUtils.currentTimeMillis();
        long lastMod = file.lastModified();
        if (file.isDirectory()) {
            for (File cFile : file.listFiles()) {
                if (cFile.isDirectory()) lastMod = Math.max(lastMod, getAgeOfFile(cFile));
                else lastMod = Math.max(lastMod, cFile.lastModified());
            }
        }
        return lastMod;
    }

    public static boolean copyFile(File from, File to) {
        try {
            // no-op
            if (from.getCanonicalPath().equals(to.getCanonicalPath())) return false;
        } catch (IOException e1) {
        };

        if (!from.exists()) return false;
        File parentFile = to.getParentFile();
        if (parentFile != null) parentFile.mkdirs();
        FileOutputStream fos = null;
        FileInputStream fis = null;
        try {
            fos = new FileOutputStream(to);
            fis = new FileInputStream(from);
            byte[] buffer = new byte[256 * 1024];
            while (fis.available() != 0) {
                int amount = fis.read(buffer);
                if (amount > 0) fos.write(buffer, 0, amount);
            }
            return true;
        } catch (Exception e) {
            LOGGER.warn("File copy failed:" + e);
            throw new RuntimeException(e);
        } finally {
            if (fos != null)
                try {
                    fos.close();
                } catch (IOException e) {
                }
            if (fis != null)
                try {
                    fis.close();
                } catch (IOException e) {
                }
        }
    }
    public static void truncate(File file) {
        if (!file.exists()) return;
        java.io.RandomAccessFile raf = null;
        try {
            raf = new java.io.RandomAccessFile(file,"rw");
            raf.setLength(0);
        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        } finally {
            if (raf != null)
                try {
                    raf.close();
                } catch (IOException e) {
                }
        }
    }

    public static List<File> listFilesRecursively(File file) {
        ArrayList<File> result = new ArrayList<File>();
        if (!file.isDirectory()) {
            result.add(file);
            return result;
        }
        File[] listFiles = file.listFiles();
        if (listFiles == null) return result;
        for (File file2 : listFiles) {
            if (file2.isDirectory()) result.addAll(listFilesRecursively(file2));
            else result.add(file2);
        }
        return result;
    }

    final public static String getPath(File file) {
        if (file == null) throw new RuntimeException("File not found");
        return FilenameUtils.normalize(file.getAbsolutePath());
    }
    final public static String getPath(String file) {
        if (file == null) throw new RuntimeException("File not found");
        return FilenameUtil.doNormalize(file,true,false);
    }


    public static List<String> readLines(String fullFilePath, int amount) throws IOException {
        return readLines(fullFilePath, 10, "");
    }
    private static int maxLineLength = Integer.getInteger("file.readlines.max.line.length.k", 4);
    private static int maxLineCharsLength = Integer.getInteger("file.readlines.max.line.chars.k", 64);

    public static List<String> readLines(String fullFilePath, int amount, String breakRule) throws IOException {
        List<String> lines = new ArrayList<String>();
        if (new File(fullFilePath).isDirectory()) return lines;
        RAF raf = RafFactory.getRaf(fullFilePath, breakRule);
        raf.setBreakRule(breakRule);
        try {
            String line = "";
            int pos = 0;
            // can blow the heap on files with very long lines
            int currentChars = 0;
            while (pos++ < amount && (line = raf.readLine()) != null && currentChars < maxLineCharsLength * 1024) {
                // massive long link can kill the tailer
                if (line.length() > maxLineLength * 1024) line = line.substring(0, maxLineLength * 1024);
                lines.add(line);
                currentChars += line.length();
            }
        } finally {
            raf.close();
        }
        return lines;
    }
    public static List<String> readLines(String fullFilePath, int from, int amount, String breakRule) throws IOException {
        // prevent anything stupid from happening
        if (from < 1) from = 1;
        if (amount > 4000) amount = 4000;
        List<String> lines = new ArrayList<String>();
        if (new File(fullFilePath).isDirectory()) return lines;
        RAF raf = RafFactory.getRaf(fullFilePath, breakRule);
        raf.setBreakRule(breakRule);
        try {
            String line = "";
            int cLine = 1;
            int currentChars = 0;
            while (lines.size() < amount && (line = raf.readLine()) != null && currentChars < maxLineCharsLength * 1024) {
                if (cLine > from) lines.add(line);
                cLine++;
                currentChars += line.length();
            }
        } finally {
            raf.close();
        }
        return lines;
    }
    public static String getLine(File logfile, int lineNumber) {
        if (FileUtil.isCompressedFile(logfile.getName()) || lineNumber != 1) {
            RAF raf = null;
            try {

                raf = RafFactory.getRafSingleLine(FileUtil.getPath(logfile), ByteBufferRAF.class.getSimpleName());
                String line = null;
                for (int i = 0; i < lineNumber; i++) {
                    line = raf.readLine();
                }
                if (line == null) return null;
                return line.trim();
            } catch (FileNotFoundException e) {
                System.out.println(new Date() + " FileNotFound:" + e.toString());
            } catch (IOException e) {
                System.out.println(new Date() + " IOException:" + e.toString());
            } finally {
                try {
                    if (raf != null) raf.close();
                } catch (IOException e) {
                }
            }
        } else {
            // just want the first line of the file, dont want to blow up OOM though!
            try {
                FileInputStream fis = new FileInputStream(logfile);
                long max = 128*1024;
                long length = fis.available()  > max ? max : fis.available();
                byte[] lines = new byte[(int) length];
                fis.read(lines);
                fis.close();
                String mline = new String(lines);
                int eold = mline.indexOf("\n");
                int eold2 = mline.indexOf("\\m");
                int nlChar = eold2 != -1 ? eold2 : eold;
                if (nlChar == -1) nlChar = (int) length;
                return mline.substring(0, nlChar);
            } catch (Exception e) {

            }
        }
        return null;
    }

    public static String readAsString(String file) {
        try {
            FileInputStream fis = new FileInputStream(file);
            int available = fis.available();
            if (available > FileUtil.MEGABYTES * 2) {
                System.err.println(new DateTime() + " WARN FileUtil.readAsString() TRUNCATING:" + file);
            }
            byte[] content = new byte[available];
            fis.read(content);
            fis.close();
            return new String(content);
        } catch (Exception e) {
            String absolutePath = new File(file).getAbsolutePath();
            System.out.println(e.toString() + " FILE:" + absolutePath);
        }
        return null;
    }
    public static void copyDir(String srcDirName, String destDirName) {
        copyDir(srcDirName, destDirName, new FileFilter(){
            public boolean accept(File pathname) {
                return true;
            }
        }, -1);
    }
    public static void copyDir(String srcDirName, String destDirName, final java.io.FileFilter filter, final long lastMod) {
        File srcDir = new File(srcDirName);
        if (!srcDir.exists()) return;
        File destDir = new File(destDirName);
        if (!destDir.exists()) destDir.mkdir();

        File[] files = srcDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.lastModified() < lastMod) return false;
                return filter.accept(pathname);
            }
        });
        for (File file : files) {
            if (!file.isDirectory()) {
                copyFile(file, new File(destDir, file.getName()));
            } else {
                copyDir(file.getAbsolutePath(), new File(destDir, file.getName()).getAbsolutePath());
            }
        }
    }

    public static List<File> loadSortedDirectories(File reportDir, DateTime lessThanEqualToDay) {
        List<File> sortedDirs = new ArrayList<File>();
        File[] listFiles = reportDir.listFiles();
        if (listFiles == null) return sortedDirs;
        for (File file : listFiles) {
            if (file.isDirectory()) {
                long lastModified = file.lastModified();
                DateTime dateTime = new DateTime(lastModified);
                if (dateTime.getDayOfYear() <= lessThanEqualToDay.getDayOfYear() || dateTime.getYear() < lessThanEqualToDay.getYear()){
                    sortedDirs.add(file);
                }

            }
        }
        Collections.sort(sortedDirs, new Comparator<File>() {
            public int compare(File o1, File o2) {
                return Long.compare(o2.lastModified(),o1.lastModified());
            }
        });
        return sortedDirs;

    }
    public static void extractTGZ(String path, File file) {
        try {
//			GZIPInputStream fis = new GZIPInputStream(new FileInputStream(file));
            InputStream fis = new GzipCodec().createInputStream(new FileInputStream(file));
            TarInputStream tin = new TarInputStream(fis);
            TarEntry tarEntry = tin.getNextEntry();
            while (tarEntry != null) {
                try {
                    File destPath = new File(path, tarEntry.getName());
                    if (tarEntry.isDirectory()) destPath.mkdir();
                    else {
                        destPath.getParentFile().mkdirs();
                        FileOutputStream fos = new FileOutputStream(destPath);
                        tin.copyEntryContents(fos);
                        fos.close();
                        destPath.setLastModified(tarEntry.getModTime().getTime());
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                tarEntry = tin.getNextEntry();
            }
            tin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void extractGzip(String path, File file, String filename) {
        try {
            InputStream fis = new GzipCodec().createInputStream(new FileInputStream(file));
            FileOutputStream fos = new FileOutputStream(new File(path, filename.replaceAll(".gzip", "").replaceAll(".gz", "")));
            byte[] buffer = new byte[128 * 1024];
            while (fis.available() != 0) {
                int amount = fis.read(buffer);
                if (amount > 0) fos.write(buffer, 0, amount);
            }
            fis.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void extractBZ2(String path, File file, String filename) {
        try {
            FileInputStream fis = new FileInputStream(file);
            // read the 2 file header bytes
            fis.read();
            fis.read();
            CBZip2InputStream bis = new CBZip2InputStream(fis);
            FileOutputStream fos = new FileOutputStream(new File(path, filename.replaceAll(".bz2", "")));
            byte[] buffer = new byte[128 * 1024];
            while (bis.available() != 0) {
                int amount = bis.read(buffer);
                if (amount > 0) fos.write(buffer, 0, amount);
            }
            bis.close();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static double getMEGABYTES(double length) {
        return length / (1024.0 * 1024.0);
    }
    public static double getGIGABYTES(double length) {
        return getMEGABYTES(length) / 1024.0;
    }



    /**


     if (pathMasks == null) pathMasks = this.filePathMask.split(COMMA);
     for (String pathMask : pathMasks) {
     if (pathMask.contains("*")) {
     boolean matches = new WildcardPattern(pathMask, WildcardPattern.IGNORE_CASE).matches(path);
     // * was not appended
     if (!matches) pathMask += "*";
     matches = new WildcardPattern(pathMask, WildcardPattern.IGNORE_CASE).matches(path);
     if (matches) return true;
     } else {
     String canonicalPath = FileUtil.getPath(new File(pathMask));
     if (path.contains(canonicalPath)) return true;
     canonicalPath = FileUtil.getPath(pathMask);
     if (path.contains(canonicalPath)) return true;

     }
     }
     return false;


     *
     * @param makeNative
     * @param path
     * @param directory
     * @return
     */
    private static ConcurrentLRUCache<String, PathPattern> pathPatterns = new ConcurrentLRUCache<String, PathPattern>(50, 10);
    public static boolean isPathMatch(boolean makeNative, String givenPathExpr, String givenRealPath) {
        if (givenPathExpr.equalsIgnoreCase(givenRealPath)) return true;
        String pathExpr = givenPathExpr;
        String realPath = givenRealPath;
        if (makeNative) {
            pathExpr = FileUtil.cleanupPathAndMakeNative(pathExpr);
            String cwd = new File("").getAbsolutePath();
            // only replace ./ stuff instances on the pathExpr
            pathExpr = pathExpr.replace("." + File.separator, cwd + File.separator);
            // double check
            pathExpr = FileUtil.cleanupPathAndMakeNative(pathExpr);
            realPath = FileUtil.cleanupPathAndMakeNative(realPath);
            if (realPath.startsWith(".")) realPath = realPath.replace("." + File.separator, cwd + File.separator);
        }

        String[] pathExprParts = StringUtil.splitFast(pathExpr, ',');
        boolean matched = false;
        boolean excluded = false;
        if (realPath.startsWith(".")) realPath = realPath.substring(1);
        for (String pathExprPart : pathExprParts) {
            if (pathExprPart.length() == 0 || pathExprPart.startsWith(".")) continue;
            boolean isExcludeExpr = pathExprPart.startsWith("!");
            if (isExcludeExpr) {
                pathExprPart = pathExprPart.substring(1);
                if (realPath.contains(pathExprPart)) excluded = true;
            }
            // relative pathExpr with wildcard later - need to prepend with wildcard
//            if (pathExprPart.startsWith(".") && pathExprPart.contains("*")) {
//                pathExprPart = "**" + pathExprPart.substring(1);// new File("").getAbsolutePath() + pathExprPart.substring(1);
//            }
            boolean thisMatch = false;
            if (pathExprPart.contains(SERVER_DIR_SEPARATOR)) {
                pathExprPart = trimServer(pathExprPart);
                // if we are looking for a realPath match with in _SERVER_ pathExpr then we need to drop the windows drive seperator
                if (realPath.contains(":")) {
                    realPath = realPath.replace(":","");
                    realPath = File.separator + realPath;
                }
            }
            if (pathExprPart.contains("*")) {
                PathPattern pathPattern = pathPatterns.get(pathExprPart);
                if (pathPattern == null) {
                    pathPattern = new PathPattern(pathExprPart, WildcardPattern.IGNORE_CASE);
                    pathPatterns.put(pathExprPart, pathPattern);
                }
                synchronized (pathPattern) {
                    thisMatch = pathPattern.matches(realPath);
                }
            } else {
                thisMatch = pathExpr.endsWith(realPath) || realPath.endsWith(pathExprPart) || realPath.endsWith(pathExprPart + File.separator);
                if (!thisMatch && realPath.contains(":")) {
                    String dirNoCol = realPath.replace(":","");
                    thisMatch =  pathExpr.endsWith(dirNoCol) || dirNoCol.endsWith(pathExprPart) || dirNoCol.endsWith(pathExprPart + File.separator);
                }
                //StringUtil.containsIgnoreCase(realPath, pathExprPart);
                //thisMatch = pathExprPart.equalsIgnoreCase(realPath);
            }
            if (thisMatch) {
                // if we matched on an exclude then bail out
                if (isExcludeExpr) excluded = true;
                else matched = true;
            }
        }
        if (excluded) return false;
        if (matched && excluded) return false;
        return matched;
    }

    private static String trimServer(String pathPart) {
        String result = null;
        int serverIndex = pathPart.indexOf(SERVER_DIR_SEPARATOR) + SERVER_DIR_SEPARATOR.length()+1;

        // CRAP xxx - _SERVER_/host/path - strip the header
        if (pathPart.contains("/")){
            result = pathPart.substring(pathPart.indexOf("/",pathPart.indexOf("/", serverIndex))+1);
        } else {
            result = pathPart.substring(pathPart.indexOf("\\",pathPart.indexOf("\\", serverIndex))+1);

        }
        return result;
    }

    public static boolean isDirectory(File pathname) {
//		String name = pathname.getName();
//		if (name.length() > 7 && name.charAt( name.length() - 4) == '.') return false;
        return pathname.isDirectory();
    }

    public static String getPath2(File dir) {
        String absolutePath = dir.getAbsolutePath();
        absolutePath = cleanPath(absolutePath);
        return absolutePath;
    }
    public static String cleanupPathAndMakeNative(String path) {
        path = makePathNative(path);
        return cleanPath(path);
    }
    public static String cleanPath(String path) {
        StringBuilder results = new StringBuilder();
        String[] pathParts = path.split(",");

        Set<String> pathsSet = new HashSet<String>();
        for (String pathPart : pathParts) {
            String part = cleanDirectory(pathPart);
            if (!pathsSet.contains(part)) {
                if (results.length() > 0) results.append(",");
                results.append(part);
                pathsSet.add(part);
            }
        }
        return results.toString();
    }
    public static String cleanDirectory(String path) {

        if (OSUtils.isWindows()) {
            if (path.lastIndexOf(":") > 3) {
                String head = path.substring(0,3);
                String tail = path.substring(3).replace(":","");
                path = head + tail;
            }
        } else if (path.contains(":")) path = path.replace(":","");
        if (path.endsWith(".")) path = path.substring(0, path.length()-1);
        if (path.endsWith("\\")) path = path.substring(0, path.length()-1);
        if (path.endsWith("/")) path = path.substring(0, path.length()-1);
        if (path.contains("//")) path = path.replace("//","/");
        if (path.contains("/./")) path =  path.replace("/./", "/");
        if (path.contains("\\.\\")) path =  path.replace("\\.\\", "\\");
        if (!path.contains("..") && path.startsWith("./") && !path.contains("*")) path = path.replace("./", "");
        if (path.length() > 1 && path.endsWith(".")) path = path.substring(0, path.length()-1);
        return path;
    }

    public static String replaceStar(String dir) {
        return  dir.replaceAll("\\*","");
    }

    public static long getLinePosForLine(File file, long line) {
        if(line == 0 || line == 1) return 0;
        RAF raf = null;
        try {
            raf = RafFactory.getRafSingleLine(file.getAbsolutePath());
            long count = 1;
            while (raf.readLine() != null) {
                count++;
                if (count == line) return raf.getFilePointer();
            }
        } catch (Throwable t) {
        } finally {
            if (raf != null) try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }


    public static long[] getLineAndPosSeekFromEnd(File file, int linesFromEnd) {

        RAF raf = null;
        try {
            raf = RafFactory.getRafSingleLine(file.getAbsolutePath());
            int count = 0;
            Queue<long[]> memo = new LinkedList<long[]>();
            while (raf.readLine() != null) {
                count++;
                memo.add(new long[] { count, raf.getFilePointer() });
                if (memo.size() > linesFromEnd) memo.remove();

            }
            if (!memo.isEmpty()) {
                return memo.remove();
            }
            return new long[] { raf.length(), count };
        } catch (Throwable t) {
        } finally {
            if (raf != null) try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new long[0];
    }

    public static long[] getLineAndPosSeek(File file, long seekPos) {
        RAF raf = null;
        try {
            raf = RafFactory.getRafSingleLine(file.getAbsolutePath(), "");
            int count = 0;
            while (raf.readLine() != null && raf.getFilePointer() < seekPos) {
                count++;
            }
            return new long[] { count, raf.getFilePointer() };
        } catch (Throwable t) {
        } finally {
            if (raf != null) try {
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new long[0];
    }

    public static String getParent(String fileNameWithPath) {
        return FilenameUtil.getFullPath(fileNameWithPath);
    }

    public static String readLine(File file, int lineNo) {
        String line = "";
        try {
            RAF raf = RafFactory.getRafSingleLine(file.getAbsolutePath());

            int seekLine = 0;
            while ( (line = raf.readLine()) != null && seekLine++ < lineNo-1) {

            }
            raf.close();

        } catch (IOException e) {
        }
        return line;

    }

}
