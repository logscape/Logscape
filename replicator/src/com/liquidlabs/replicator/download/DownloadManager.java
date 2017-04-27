package com.liquidlabs.replicator.download;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.common.FileHelper;
import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.replicator.SysBouncer;
import com.liquidlabs.replicator.data.Upload;
import com.liquidlabs.replicator.service.MetaUpdateHandler;
import com.liquidlabs.replicator.service.MetaUpdateListener;
import com.liquidlabs.replicator.service.ReplicationService;
import com.liquidlabs.replicator.service.ReplicationServiceImpl;
import com.liquidlabs.space.lease.LeaseRenewer;
import com.liquidlabs.space.lease.Registrator;
import com.liquidlabs.space.lease.Renewer;
import com.liquidlabs.transport.proxy.ProxyFactory;

public class DownloadManager {
    private static final Logger LOGGER = Logger.getLogger(DownloadManager.class);
    private final ReplicationService replicationService;
    private final Map<String, Downloader> downloaders = new ConcurrentHashMap<String, Downloader>();
    private final Map<String, String> leaseKeys = new ConcurrentHashMap<String, String>();
    private ExecutorService executor;
    private ExecutorService listenerThread;
    private final Set<String> downloaded = new CopyOnWriteArraySet<String>();
    private final LeaseRenewer leaseRenewalService;
    private final ProxyFactory proxyFactory;

    java.util.LinkedHashMap<String, Long> downloadedFiles = new java.util.LinkedHashMap<String, Long>() {
        private static final long serialVersionUID = 1L;

        protected boolean removeEldestEntry(java.util.Map.Entry<String, Long> eldest) {
            return (this.size() > 100);
        }
    };
    private final String resourceId;

    public DownloadManager(ReplicationService replicationService, LeaseRenewer leaseRenewalService, ProxyFactory proxyFactory, SysBouncer bouncer, String resourceId) {
        this.replicationService = replicationService;
        this.leaseRenewalService = leaseRenewalService;
        this.proxyFactory = proxyFactory;
        this.resourceId = resourceId;
        executor = proxyFactory.getExecutor();
        listenerThread = com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("worker","dl-listener");
    }

    public synchronized boolean alreadyDownloading(String hashKey) {
        if (downloaders.containsKey(hashKey) || downloaded.contains(hashKey)) {
            return true;
        }
        return false;
    }

    public File download(String filename, String path, String hash, int pieces, DownloadListener downloadListener, int seeders) {

        if (filename.startsWith("."))
            return null;

        Downloader downloader = new Downloader(proxyFactory, executor, hash, pieces);
        String hashKey = getHashKey(path, filename, hash);

        try {
            cleanupOldDownloaders(hashKey, filename);
            if (alreadyDownloading(hashKey)) {
                LOGGER.info(String.format("Already download[ing/ed] filename: %s/%s, with hash %s", path, filename, hash));
                return null; // poo
            } else {
                LOGGER.info(String.format("%s - [%s] downloading. Pieces[%d] Seeders[%d] Exists:" + new File(filename).exists(), ReplicationServiceImpl.TAG, filename, pieces, seeders));
            }
            downloaders.put(hashKey, downloader);

            if (downloader.isAlreadyDownloaded(hash, path, filename)) {
                LOGGER.info(String.format("Has already download[ing/ed] filename: %s/%s, with hash %s", path, filename, hash));
                downloaded.add(hashKey);
                return null;
            }
            File myFile = null;
            String lease = null;
            try {
                lease = registerUpdateListener(hash, downloader, path, filename);
                myFile = download(filename, hash, path, downloader);
                downloaded.add(hashKey);
            } catch (Throwable t) {
                LOGGER.warn(String.format("%s - download of %s failed due to %s", ReplicationServiceImpl.TAG, filename, t.getMessage()), t);
                downloaded.remove(hashKey);
                throw new RuntimeException(t);
            }
            if (lease != null) {
                leaseRenewalService.cancelLeaseRenewal(lease);
                leaseKeys.remove(hashKey);
            }
            downloadedFiles.put(myFile.getPath(), DateTimeUtils.currentTimeMillis());
            notify(downloadListener, myFile, hash);
            return myFile;
        } finally {
            downloaders.remove(hashKey);
        }
    }

    private void cleanupOldDownloaders(String hashKey, String filename) {
        // remove old downloader
        if (downloaders.containsKey(hashKey)) {
            Downloader downloader = downloaders.get(hashKey);
            DateTime now = new DateTime();
            DateTime started = downloader.getStarted();
            // if been downloading for 15 minutes, then something was wrong!
            if (now.getMinuteOfDay() - started.getMinuteOfDay() > 15) {
                downloaders.remove(hashKey);
            }
        }
        // remove downloaded if the file doesnt exist
        if (!new File(filename).exists()) {
            this.downloaded.remove(hashKey);
        }
    }

    public String getHashKey(String path, String filename, String hash) {
        return path + filename + hash;
    }

    private void notify(final DownloadListener downloadListener, final File myFile, final String hash) {
        if (myFile != null) {
            listenerThread.execute(new Runnable() {
                public void run() {
                    downloadListener.downloadComplete(myFile, hash);
                }
            });
        }
    }

    private File download(String filename, String hash, String path, Downloader downloader) throws IOException, NoSuchAlgorithmException {
        long start = System.currentTimeMillis();
        FileHelper.mkAndList(path);

        File myFile = downloader.retrieve(replicationService.retrieveCurrentForHash(hash, path, filename), path, filename);
        long stop = System.currentTimeMillis();
        long length = myFile.length();
        if (length == 0)
            throw new RuntimeException("Got 0 length download for file:" + filename);
        LOGGER.info(String.format("%s - %s completed download to %s. File size = %dKB, Download Time = %d seconds", ReplicationServiceImpl.TAG, filename, myFile.getAbsolutePath(), length / 1024,
                (stop - start) / 1000));
        return myFile;
    }

    private String registerUpdateListener(final String hash, Downloader downloader, final String path, final String filename) {
        final MetaUpdateListener metaUpdateHandler = new MetaUpdateHandler(downloader, resourceId);

        final int leasePeriod = 5 * 60;
        final int leaseRenewPeriod = 3 * 60;
        String lease = replicationService.registerMetaUpdateListener(metaUpdateHandler, hash, leasePeriod, metaUpdateHandler.getId());

        Registrator registrator = new Registrator() {
            public String info() {
                return "RegisterMetaUpdateListener";
            }

            public String register() throws Exception {
                String leaseKey = replicationService.registerMetaUpdateListener(metaUpdateHandler, hash, leasePeriod, metaUpdateHandler.getId());
                leaseKeys.put(getHashKey(path, filename, hash), leaseKey);
                return leaseKey;
            }

            public void registrationFailed(int failedCount) {
            }
        };
        Renewer renewer = new Renewer(replicationService, registrator, lease, leasePeriod, "RegisterMetaUpdateListener", LOGGER);

        leaseRenewalService.add(renewer, leaseRenewPeriod, lease);

        leaseKeys.put(getHashKey(path, filename, hash), lease);
        return lease;
    }

    public void initialize(String saveDir, DownloadListener downloadListener) {

        removePatialDownloads(saveDir);
        addFilesAlreadyDownloaded(saveDir);
        downloadFileIfNeedTo(downloadListener);
    }

    private void removePatialDownloads(String saveDir) {

        try {
            File[] filesToRemove = new File(saveDir).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.startsWith(".");
                }
            });

            for (File remove : filesToRemove) {
                remove.delete();
            }
        } catch (Throwable t) {
            LOGGER.warn("Failed to cleanup", t);
        }

    }

    private void addFilesAlreadyDownloaded(String saveDir) {
        HashGenerator hashGenerator = new HashGenerator();
        File[] files = new File(saveDir).listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return !file.isDirectory();
            }
        });
        for (File file : files) {
            try {
                String hash = hashGenerator.createHash(file.getName(), file);
                downloaded.add(hash);
            } catch (Throwable t) {
                LOGGER.warn("Failed to generate hashfor:" + file.getAbsolutePath(), t);
            }
        }
    }

    private void downloadFileIfNeedTo(final DownloadListener downloadListener) {
        HashGenerator hashGenerator = new HashGenerator();
        List<Upload> deployed = replicationService.getDeployedFiles();
        if (deployed == null) return;
        for (final Upload upload : deployed) {
            try {
                File file = new File(upload.path, upload.fileName);
                LOGGER.info("Checking Download:" + file.getName());
                if (!file.exists() || !hashGenerator.createHash(file.getName(), file).equals(upload.hash)) {
                    // dont use the same executor the drainers use because we can hit a deadlock with all the tasks blocking to wait for these metas to be put on the queue
                    proxyFactory.getScheduler().execute(new Runnable() {
                        public void run() {
                            try {
                                download(upload.fileName, upload.path, upload.hash, upload.pieces, downloadListener, 1);
                            } catch (Throwable t) {
                                LOGGER.error(t.getMessage(), t);
                            }
                        }
                    });
                }
            } catch (Throwable t) {
                LOGGER.warn(t);
            }
        }
    }

    public void remove(String hash, String fileName, String path, DownloadListener downloadListener, boolean forceRemove) {

        LOGGER.info("Download removing:" + path + "/" + fileName + " hash:" + hash);
        HashGenerator hashGenerator = new HashGenerator();
        File file = new File(path, fileName);

        try {
            cleanupOldDownloaders(hash, fileName);

            String existingHash = hashGenerator.createHash(file.getName(), file);

            if (file.exists() && existingHash.equals(hash)) {

                // only delete if another file hasnt popped up
                Thread.sleep(1000);
                if (!hashGenerator.createHash(file.getName(), file).equals(hash))
                    return;

                LOGGER.info("Download removed:" + fileName + " hash:" + hash  + " Exists:" + file.exists());
                String hashKey = getHashKey(path, fileName, hash);
                String remove = leaseKeys.remove(hashKey);
                if (remove != null) {
                    leaseRenewalService.cancelLeaseRenewal(remove);
                } else {
                    LOGGER.warn("Failed to find lease for:" + path + fileName + hash + " keySet:" + leaseKeys.keySet());
                }
                downloaded.remove(hashKey);
                downloaders.remove(hashKey);
                downloadListener.downloadRemoved(file);
                downloadedFiles.remove(file.getPath());

                // dont delete the existing file.... leave it in tack and allow
                // the actual Download task to rename it when the download is
                // complete..
                if (forceRemove) file.delete();
                LOGGER.info(String.format("%s - %s has not been deleted [forceRemove:" + forceRemove + "] Exists:" + file.exists(), ReplicationServiceImpl.TAG, fileName));

            } else {
                //
                LOGGER.info(String.format("%s - %s was NOT deleted because of hashMismatch OR Exists[%s]", ReplicationServiceImpl.TAG, fileName, file.exists()));
            }
        } catch (Exception e) {
            LOGGER.warn("Remove problem with:" + fileName + " ex:" + e.getMessage(), e);
        }
    }

    /**
     * downloaded in the last 30 seconds
     *
     * @param file
     * @return
     */
    public boolean isJustDownloaded(File file) {
        Long when = downloadedFiles.get(file.getPath());
        if (when == null)
            return false;
        // was downloaded in the last 30 seconds
        return when > DateTimeUtils.currentTimeMillis() - (30 * 1000);
    }

    public void addToDownloaded(String hash, String path, String filename) {
        downloaded.add(getHashKey(path, filename, hash));
        downloadedFiles.put(path, DateTimeUtils.currentTimeMillis());
    }

    public void shutdown() {
        executor.shutdown();
        listenerThread.shutdown();
    }

}
