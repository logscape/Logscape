package com.liquidlabs.replicator.download;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.replicator.ReplicatorProperties;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.PieceInfo;
import com.liquidlabs.replicator.service.ReplicationServiceImpl;
import com.liquidlabs.transport.proxy.ProxyFactory;


public class Downloader implements DownloadTaskListener {

    private static final int DOWNLOAD_RETRY_PAUSE = ReplicatorProperties.getDownloadRetryPause();
    private static final Logger LOGGER = Logger.getLogger(Downloader.class);
    private final ExecutorService executor;
    private final String hash;
    private final int pieces;
    private HashGenerator hasher = new HashGenerator();
    private final BlockingQueue<Meta> queue = new ArrayBlockingQueue<Meta>(1000);
    private final Map<String, Integer> hashFailures = new ConcurrentHashMap<String, Integer>();
    private Map<Integer, Future<DownloadResult>> results;
    private FileChannel channel;
    private final ProxyFactory proxyFactory;
    DateTime started = new DateTime();

    public Downloader(ProxyFactory proxyFactory, ExecutorService executor, String hash, int pieces) {
        this.proxyFactory = proxyFactory;
        this.executor = executor;
        this.hash = hash;
        this.pieces = pieces;
    }

    public File retrieve(List<Meta> hosts, String path, String filename) throws IOException, NoSuchAlgorithmException {

        int retryCount = 0;
        int limit = 10;
        Exception ex = null;
        while (retryCount++ < limit) {
            try {
                LOGGER.info("Clearing DL Q");
                queue.clear();
                hasher = new HashGenerator();
                hashFailures.clear();
                return retrieveTheFile(hosts, path, filename);
            } catch (Exception t){
                try {
                    LOGGER.warn(String.format("Attempt[%d of %d] %s - %s", retryCount, limit, ReplicationServiceImpl.TAG, t.toString()));
                    ex = t;
                    Thread.sleep(ReplicatorProperties.getDownloadRetryPause() * 2);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException(ex);
    }

    private File retrieveTheFile(List<Meta> metas, String path, String filename) throws IOException, NoSuchAlgorithmException, HashNoMatchException {
        Collections.shuffle(metas);
        LOGGER.info(String.format("File:%s Downloading with Metas %d", filename, metas.size()));
        queue.addAll(metas);

        if (metas.size() == 0) {
            throw new RuntimeException(String.format("Hosts is empty - cannot retrieve %s/%s", path, filename));
        }
        if (isAlreadyDownloaded(metas.get(0).getHash(), path, filename)) {
            LOGGER.info(String.format("Existing file already has same hash, ignoring download of %s/%s", path, filename));
            return new File(path, filename);
        }
        if (filename.endsWith(".bak")) {
            LOGGER.info(String.format("Ignoring BAK files %s/%s", path, filename));
            return new File(path, filename);

        }
        File tempFile = new File(path, "." + filename);
        File newFile = new File(path, filename);
        try {

            FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
            channel = fileOutputStream.getChannel();

            submitDownloadTasks(filename, channel, getURIsFromMetas(metas));

            boolean success = waitForResults();
            channel.close();
            fileOutputStream.close();
            if (!success) {
                throw new RuntimeException(String.format("%s - Experienced file[%s/%s] Download errors", ReplicationServiceImpl.TAG, path, filename));
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            String createHash = hasher.createHash(newFile.getName(), tempFile);
            if (!createHash.equals(hash)) {
                tempFile.delete();
                throw new HashNoMatchException(String.format("%s - Problem Downloading file [%s/%s]. Hash[%s]!=[%s]", ReplicationServiceImpl.TAG, path, filename, createHash, hash));
            }

            if (newFile.exists() && newFile.length() > 0) {
                File bakFile = new File(path, newFile.getName()+ ".bak");
                if (bakFile.exists()) bakFile.delete();
                LOGGER.info("Renaming existing file to:" + bakFile.getName());
                FileUtil.renameTo(newFile, bakFile);
            }
            if (newFile.exists()) newFile.delete();
            FileUtil.renameTo(tempFile, newFile);

            if (newFile.getName().equals("boot.jar")) {
                try {
                    LOGGER.info("LS_EVENT:Replacing BOOT.JAR");
                    FileUtil.copyFile(newFile, new File("boot.jar"));
                } catch (Throwable t) {
                    LOGGER.info("Download failed", t);
                }
            }
        } finally {
            tempFile.delete();
        }
        return newFile;
    }

    private List<String> getURIsFromMetas(List<Meta> hosts) {
        Set<String> hostsWithMetas = new HashSet<String>();
        for (Meta meta : hosts) {
            hostsWithMetas.add("stcp://" + meta.getHostname() + ":" + meta.getPort());
        }
        ArrayList<String> hostsList = new ArrayList<String>(hostsWithMetas);
        LOGGER.info("Download URIs:" + hostsList);
        return hostsList;
    }

    public boolean isAlreadyDownloaded(String hash, String path, String filename) {
        try {
            File file = new File(path, filename);
            String createHash = hasher.createHash(file.getName(), file);
            return (hash.equals(createHash));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean waitForResults() {
        int waitCount = 0;
        int success = 0;
        while (success != pieces && waitCount++ < 100) {
            Map<Integer, Future<DownloadResult>> current = new HashMap<Integer,Future<DownloadResult>>(results);
            for (Map.Entry<Integer, Future<DownloadResult>> entry : current.entrySet()) {
                try {
                    DownloadResult downloadResult = entry.getValue().get();
                    if (downloadResult.success || !downloadResult.retryRequired) {
                        results.remove(entry.getKey());
                        success++;
                    }
                } catch (InterruptedException e) {
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage(), e);
                }

            }
            // if we still have 'results' then we arent finished yet
            if (!results.isEmpty()) {
                try {
                    LOGGER.info("Pausing to retry: Success:" + success + " Pieces:" + pieces + " wait:" + waitCount);
                    Thread.sleep(ReplicatorProperties.getDownloadRetryPause());
                } catch (InterruptedException e) {}
            }
        }
        return success == pieces;
    }

    private Map<Integer, Future<DownloadResult>> submitDownloadTasks(String filename, FileChannel channel, List<String> uris) {
        results = new HashMap<Integer, Future<DownloadResult>>();
        LOGGER.info(filename + ": Total Pieces to download:" + pieces + " URIs:" + uris + " QSize:" + queue.size());
        for (int i = 0; i<pieces; i++) {
            try {
                Meta take = queue.poll(10, TimeUnit.SECONDS);
                if (take == null) throw new RuntimeException(String.format("Invalid Piece Info, expected[%d] queueIsEmpty", pieces));
                if (take.getPieces().length-1 < i) {
                    throw new RuntimeException(String.format("Invalid Piece Info, expected[%d] given[%d]", pieces, take.getPieces().length));
                }
                PieceInfo pieceInfo = take.getPieces()[i];
                LOGGER.info(String.format("Downloading Piece[%d] total MetaPieces[%d]", i, take.getPieces().length));
                results.put(i, executor.submit(createDownloadTask(take, pieceInfo, channel, uris)));
            } catch (InterruptedException e) {
                i--;
            }
        }
        return results;
    }

    private Callable<DownloadResult> createDownloadTask(final Meta meta, final PieceInfo pieceInfo, final FileChannel channel, List<String> uris) {
        DownloadTask downloadTask = new DownloadTask(proxyFactory, pieceInfo, channel, hasher, uris);
        downloadTask.use(meta);
        downloadTask.addDownloadTaskListenener(this);
        return downloadTask;
    }

    public void downloadFailed(PieceInfo pieceInfo, Meta meta, Throwable e, List<String> uris) {
        waitForABit();
        if (withinFailureLimit(meta.getAddress() + ":" + pieceInfo.getPieceNumber())) {
            queue.add(meta);
        }
        LOGGER.error(String.format("Rescheduling due to failure [%s] when downloading piece %s, file[%s/%s] from %s",e.getMessage(), pieceInfo.getHash() + "-" + pieceInfo.getPieceNumber(), meta.getAddress(), meta.getPath(), meta.getFileName()), e);
        reschedule(pieceInfo, uris);
    }

    private void waitForABit() {
        try {
            Thread.sleep(DOWNLOAD_RETRY_PAUSE);
        } catch (InterruptedException e1) {
        }
    }

    public void hashFailed(PieceInfo pieceInfo, Meta meta, List<String> uris) {
        if (withinFailureLimit(meta.getAddress() + ":" + pieceInfo.getPieceNumber())) {
            queue.add(meta);
        }
        reschedule(pieceInfo, uris);
    }

    private synchronized boolean withinFailureLimit(String key) {
        Integer failures = hashFailures.get(key);
        if (failures == null) {
            failures = 0;
        }
        hashFailures.put(key, ++failures);
        return failures < 120;
    }

    private void reschedule(PieceInfo pieceInfo, List<String> uris) {
        try {
            Thread.sleep(ReplicatorProperties.getDownloadRescheduleTime() + (long)(Math.random() * ReplicatorProperties.getDownloadRescheduleTime())); // give it a break.
            Meta meta = queue.take();
            results.put(pieceInfo.getPieceNumber(), executor.submit(createDownloadTask(meta, pieceInfo, channel, uris)));
            LOGGER.info(String.format("%s - Piece %d rescheduled on %s results.size=[%s]", ReplicationServiceImpl.TAG, pieceInfo.getPieceNumber(), meta.getAddress(), results.size()));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void taskComplete(PieceInfo pieceInfo, Meta meta) {
        queue.add(meta);
        // pieceListener.pieceComplete(meta, pieceInfo);
    }

    public void addMeta(Meta metaInfo) {
        LOGGER.debug(String.format("Adding %s to list of downloaders for file [%s/%s], hash %s", metaInfo.getAddress(), metaInfo.getPath(), metaInfo.getFileName(), metaInfo.getHash()));
        queue.add(metaInfo);
    }

    public DateTime getStarted() {
        return started;
    }


}
