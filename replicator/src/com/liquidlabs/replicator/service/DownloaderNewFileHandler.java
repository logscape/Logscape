package com.liquidlabs.replicator.service;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.replicator.ReplicatorProperties;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.download.DownloadListener;
import com.liquidlabs.replicator.download.DownloadManager;

public class DownloaderNewFileHandler implements NewFileListener {
	private static final Logger LOGGER = Logger
			.getLogger(DownloaderNewFileHandler.class);

	private String id = "NewFileHandler";

	transient private final DownloadManager downloadManager;
	transient private final String endPoint;
	transient private final DownloadListener downloadListener;
	transient private final ReplicationService replicationService;

	private ExecutorService pool;

	public DownloaderNewFileHandler(DownloadManager downloadManager,
			String endPoint, DownloadListener downloadListener,
			ReplicationService replicationService, String resourceId) {
		this.downloadManager = downloadManager;
		this.endPoint = endPoint;
		this.downloadListener = downloadListener;
		this.replicationService = replicationService;
		this.id = DownloaderNewFileHandler.class.getSimpleName() + resourceId;
		pool = com.liquidlabs.common.concurrent.ExecutorService.newDynamicThreadPool("manager","download-manager");
	}

	public void newFileUploaded(final String filename, final String path,
			final String hash, final int pieces) {
		pool.execute(new Runnable() {

			public void run() {
				int seeders = waitForSeeder(hash, path, filename);

				try {
					downloadManager.download(filename, path, hash, pieces, downloadListener, seeders);
				} catch (Throwable t) {
					LOGGER.warn(String.format(
							"Failed to download[%s], going to retry ex:%s",
							filename, t.getMessage()));
					downloadManager.download(filename, path, hash, pieces, downloadListener, seeders);
				}

			}
		});

	}

	private int waitForSeeder(String hash, String path, String filename) {
		List<Meta> retrieveCurrentForHash = replicationService.retrieveCurrentForHash(hash, path, filename);
		if (retrieveCurrentForHash == null) {
			LOGGER.error("Wait For Seeder Failed:" + hash + " path:" + path + " file:" + filename);
			return 0;
		}
		int count = retrieveCurrentForHash.size();
		long timeout = System.currentTimeMillis() + 10000;
		while (count < 2 && System.currentTimeMillis() < timeout) {
			try {
				Thread.sleep((long) (1500 + (Math.random() * 2000)));
				retrieveCurrentForHash = replicationService.retrieveCurrentForHash(hash, path, filename);
				if (retrieveCurrentForHash == null) {
					LOGGER.error("Wait For Seeder Failed:" + hash + " path:" + path + " file:" + filename);
					return 0;
				}
				count = retrieveCurrentForHash.size();
			} catch (InterruptedException e) {
			}
		}
		return count;
	}

	public String getId() {
		return id;
	}

	public void removeFile(final String hash, final String fileName, final String path, final boolean forceRemove) {
		pool.execute(new Runnable(){
			@Override
			public void run() {
				LOGGER.info(String.format("RemoveFile[%s/%s] Exists:" + new File(path, fileName).exists(), path, fileName));
				downloadManager.remove(hash, fileName, path, downloadListener, forceRemove);
				
			}});
		
	}

}
