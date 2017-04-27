package com.liquidlabs.replicator.service;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;

import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.replicator.ReplicatorProperties;
import com.liquidlabs.replicator.UploadManager;
import com.liquidlabs.replicator.data.MetaInfo;
import com.liquidlabs.replicator.download.DownloadListener;
import com.liquidlabs.replicator.download.DownloadManager;


public class UploaderNewFileHandler implements NewFileListener {
	private String id = "UploadNewFileHandler";
	private final DownloadManager downloadManager;
	private final String endPoint;
	private final UploadManager uploadManager;
	private static final Logger LOGGER = Logger.getLogger(UploaderNewFileHandler.class);
	private final DownloadListener downloadListener;

	public UploaderNewFileHandler(UploadManager uploadManager, DownloadManager downloadManager, String endPoint, String saveDir, DownloadListener downloadListener, String resourceId) {
		this.uploadManager = uploadManager;
		this.downloadManager = downloadManager;
		this.endPoint = endPoint;
		this.downloadListener = downloadListener;
		this.id = getClass().getSimpleName() + PIDGetter.getPID();
		if (!new File(saveDir).exists()) new File(saveDir).mkdirs();
	}


	public void newFileUploaded(String fileName, String path, String hash, int pieces) {
		
		int attemptLimit = 3;
		int attempt = 0;
		while (attempt++ < attemptLimit) {
			try {
				LOGGER.info(String.format("%s - UploadManager downloading %s/%s on %s", ReplicationServiceImpl.TAG, path, fileName, endPoint));
				File download = downloadManager.download(fileName, path, hash, pieces, new DownloadListener() {
					public void downloadComplete(File tempFile, String hash) {
					}
		
					public void downloadRemoved(File name) {
					}}, 1);
				if (download != null) {
					LOGGER.info(String.format("%s - UploadManager completed download of [%s/%s] size[%d]. Publishing Availability", ReplicationServiceImpl.TAG, path, fileName, download.length()));
					try {
						uploadManager.publish(new MetaInfo(download, ReplicatorProperties.getChunkSizeKB()));
						Thread.sleep(5000);
						downloadListener.downloadComplete(download, hash);
						return;
					} catch (NoSuchAlgorithmException e) {
						LOGGER.error(e);
					} catch (IOException e) {
						LOGGER.error(e);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
				
			} catch (Throwable t) {
				LOGGER.warn(String.format("Download [%s] failed, attempt:%d", fileName, attempt));
			}
		}
	}
	

	public String getId() {
		return id;
	}

	
	public void removeFile(String hash, String fileName, String path, boolean forceRemove) {
		LOGGER.info("removeFile event:" + fileName + " hash:" + hash);
		downloadManager.remove(hash, fileName, path, downloadListener, forceRemove);
		uploadManager.retract(hash);
	}


}
