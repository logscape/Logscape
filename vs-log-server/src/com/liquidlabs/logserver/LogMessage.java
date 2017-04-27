package com.liquidlabs.logserver;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import org.apache.log4j.Logger;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

public class LogMessage {
    private final static Logger LOGGER = Logger.getLogger(LogMessage.class);
    public static final Integer ThrottleOffLevel = Integer.getInteger("throttle.off.level", 20);
    public static final Integer RollDays = Integer.getInteger("live.roll.days", 3);
    transient private final TreeMap<Long, String> messages =  new TreeMap<Long, String>();

    String timeZone;
    String host;
    String canonicalFile;
    long timestamp;
    int startLine;
    private long filePos;
    int currentLine;
    List<String> message = new ArrayList<String>();
    private static int bufferSize = Integer.getInteger("fwdr.buffer.size", 100);
    private transient int charCount = 0;

    public LogMessage() {
    }
    public LogMessage(String host, String canonicalFile, long timestamp, int startLine, long filePos) {
        this.host = host;
        this.canonicalFile = canonicalFile;
        this.timestamp = timestamp;
        this.startLine = startLine;
        this.filePos = filePos;
        this.timeZone = java.util.TimeZone.getDefault().getID();
    }
    public LogMessage addMessage(String line) {
        messages.put(System.nanoTime(), line);
        this.currentLine++;
        return this;
    }
    public LogMessage addMessage(long lineTime, String line, long filePosition) {
        while (messages.containsKey(lineTime)) lineTime++;
        this.messages.put(lineTime, line);
        this.filePos = filePosition;
        this.charCount += line.length();
        this.currentLine++;
        return this;
    }
    public void flush(LogServer logServer, boolean force, long pos) {
        if (this.messages.size() > 0 && (this.messages.size() > bufferSize || force || charCount > 4 * 1024)) {
            this.message.addAll(messages.values());
            this.filePos = pos;
            //System.out.println("Client SENDing:" + "pos:" + this.filePos + " " + message);
            int handle = logServer.handle(host + canonicalFile, this);
            if (handle > ThrottleOffLevel) {
                throttleOffWhenTooBusy(logServer.toString(), handle);
            }

            //System.out.println("NewPos:" + pos);
            this.startLine = currentLine;
            this.message = new ArrayList<String>();
            this.messages.clear();
            this.filePos = -1;
            this.charCount = 0;
        } else {
            if (this.filePos == -1) this.filePos = pos;
        }

    }
    private void throttleOffWhenTooBusy(String serverString, int handle) {
        try {
            LOGGER.info(serverString + " Throttling off:" + handle + " handle:" + canonicalFile);
            Thread.sleep(handle + 50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, ReentrantLock> writerLockMap = new ConcurrentHashMap<>();
    public void write(String root) {

        String fileKey = root + canonicalFile;
        ReentrantLock lock = writerLockMap.get(fileKey);
        if (lock == null) {
            synchronized (writerLockMap) {
                if (!writerLockMap.containsKey(fileKey))
                    writerLockMap.put(fileKey, new ReentrantLock());
            }
            lock = writerLockMap.get(fileKey);
        }
        try {
            try {
                boolean b = lock.tryLock(1, TimeUnit.SECONDS);
                if (!b) {
                    LOGGER.warn("Failed to get Lock:" + root + canonicalFile);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }


            File file = null;
            try {
                file = new File(root, fixFilenames(canonicalFile));
                // filePos == -1 when sent from collector
                if (this.filePos != -1) {
                    if (file.exists() && file.length() > this.filePos) {
                        if (System.currentTimeMillis() - file.lastModified() > RollDays * 60L * 60L * 24L * 1000L) {
                            LOGGER.info("Found file greater than " + RollDays + " days, deleted and started afresh.");
                            if (!file.delete()) {
                                LOGGER.error("File Locked - deleting old file failed: " + file.getCanonicalPath() + " but failed");
                                return;
                            }
                        }
                        if (this.filePos < 4 * 1024) {
                            String msg = "Dropping msg:" + file.getAbsolutePath() + " line:" + this.currentLine +
                                    " FileLength:" + file.length() + " GivenFilePos:" + this.filePos + " msgs:" + this.message.size();
                            LOGGER.warn(msg);
                            if (this.message.size() >= 1) LOGGER.warn(" FWDR_MSG:" + shorten(this.message.get(0)));
                            return;
                        }
                    }
                    if (file.exists() && file.length() > this.filePos) {
                        LOGGER.error("Detected FileMessageOutOfSequence Dropping fwdrPos:" + this.filePos + " IndexStorePos:" + file.length() + " File:" + file.getCanonicalPath());
                        if (this.message.size() > 0) LOGGER.error(" FWDR_MSG: " + shorten(this.message.get(0)));
                        return;
                    }
                }
                if (!file.exists()) {
                    LOGGER.info("Created Fwd File:" + file.getAbsolutePath());
                    FileUtil.mkdir(file.getParentFile().getPath());
                }

                writeTimeZone(file.getAbsolutePath(), this.timeZone);
                PrintWriter writer = new PrintWriter(new BufferedOutputStream(getOutputStream(file)));
                
                for (String msg : message) {
                    writer.println(msg);
                }
                writer.close();
            } catch (Exception e) {
                String ff = file != null ? file.getAbsolutePath() : "Null-File";
                LOGGER.info("Created Fwd File Failed" + ff, e);
                throw new RuntimeException(e);
            }
        } finally {
            if (lock.isLocked()) lock.unlock();
        }
    }

    private String shorten(String s) {
        if (s.length() < 1024) return s;
        return s.substring(0, 1023);
    }

    void writeTimeZone(String file, String timeZone) {
        String serverDirName = file.substring(0, file.indexOf(this.host) + this.host.length());
        File serverDir = new File(serverDirName);
        if (!serverDir.exists()) {
            serverDir.mkdirs();
        }
        File props = new File(serverDir, "datasource.properties");
        if (!props.exists() || props.exists() && props.lastModified() < System.currentTimeMillis() - DateUtil.DAY) {
            try {
                FileOutputStream fos = new FileOutputStream(props);
                fos.write(("source.timezone=" + timeZone).getBytes());
                fos.close();
            } catch (Throwable t) {
                t.printStackTrace();;
            }
        }

    }

    private OutputStream getOutputStream(File file) throws Exception {
        file.getParentFile().mkdirs();
        if (file.getName().endsWith(".gz")) {
            return new GZIPOutputStream(new FileOutputStream(file, true));
        } else if (file.getName().endsWith(".snap")) {
            return new SnappyFramedOutputStream(new FileOutputStream(file, true));
        }
        return new FileOutputStream(file, true);
    }

    static boolean isDeletingForwarded = Boolean.parseBoolean(System.getProperty("indexserver.allow.delete", "true"));
    public boolean delete(String root) {
        File file = new File(root, fixFilenames(canonicalFile));

        if (!isDeletingForwarded) {
            String timestamp = DateUtil.shortDateTimeFormat5.print(System.currentTimeMillis());
            File preservedFile = new File(file.getParent(), timestamp + "_" + file.getName());
            LOGGER.info("preserveDeleted:" + host + " to:" + file.getName() + " =>" + preservedFile.getPath());

            return file.renameTo(preservedFile);
        } else {
            LOGGER.info("deleted:" + host + " to:" + file.getPath());

            boolean deleted = false;
            int attempts = 0;
            if (!file.exists()) {
                LOGGER.warn("Failed to delete *non-existent* file:" + file.getAbsolutePath());
            } else {
                while (file.exists() && !deleted && attempts++ < 10) {
                    deleted = file.delete();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
                if (!deleted) {
                    LOGGER.warn("Failed to delete file:" + file.getAbsolutePath());
                }
            }
            return deleted;
        }

    }
    public void roll(String root, String toFilename) {
        LOGGER.info("Rolling:" + canonicalFile + " => " + toFilename);
        File file = new File(root, fixFilenames(canonicalFile));
        File toFile = new File(root, fixFilenames(toFilename));
        try {
            FileUtil.renameTo(file, toFile);
        } catch (IOException e) {
            LOGGER.warn("Failed to rename:" + root + " > " + toFilename + " ex:" + e.toString());
        }
    }

    public File getFile(String root) {
        return new File(root, fixFilenames(canonicalFile));
    }

    public int getLineCount(String root)  {
        File file = new File(root, fixFilenames(canonicalFile));
        if (!file.exists()) {
            return 0;
        }
        try {
            return (int) FileUtil.countLines(file)[1];
        } catch (IOException e) {
            LOGGER.warn("Failed to getLineCount", e);
            return 0;
        }
    }
    private String fixFilenames(String canonicalFile2) {
        String replaced = canonicalFile2.replaceAll("\\:\\\\", "/");
        return replaced.replaceAll("\\\\", "/");
    }
    public int getLineCount(LogServer logServer) {
        return logServer.getStartLine(host + canonicalFile, host, canonicalFile);
    }
    public long getStartPosition(LogServer logServer) {
        return logServer.getStartPos(host + canonicalFile, this);
    }

    public LogMessage addMessage(String[] message) {
        for (String m : message) {
            this.addMessage(m);
        }
        return this;
    }
}
