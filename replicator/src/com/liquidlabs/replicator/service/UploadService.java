package com.liquidlabs.replicator.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.replicator.ReplicatorProperties;
import com.liquidlabs.replicator.SysBouncer;
import com.liquidlabs.replicator.UploadManager;
import com.liquidlabs.replicator.data.FileUploader;
import com.liquidlabs.replicator.data.MetaInfo;
import com.liquidlabs.replicator.data.Piece;
import com.liquidlabs.replicator.data.Upload;
import com.liquidlabs.replicator.download.DownloadListener;
import com.liquidlabs.replicator.download.DownloadManager;
import com.liquidlabs.space.lease.LeaseRenewalService;
import com.liquidlabs.space.lease.Registrator;
import com.liquidlabs.space.lease.Renewer;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.lookup.LookupSpace;

public class UploadService implements LifeCycle, Uploader {
	
	public static Logger LOGGER = Logger.getLogger(UploadService.class);
	
	private final UploadManager uploadManager;
	private final ProxyFactory proxyFactory;
	private final ReplicationService replicationService;
	private final LeaseRenewalService leaseRenewalService;
	private final DownloadManager downloadManager;
	private final String saveDir;
	private NewFileListener newFileHandler;
	private String downloadLease;
	private String uploaderLease;
	private DirWatcher dirWatcher;

	private DownloadListener downloadListener;

	private final String resourceId;

	public UploadService(UploadManager uploadManager, ReplicationService replicationService, ProxyFactory proxyFactory, LeaseRenewalService leaseRenewalService, DownloadManager downloadManager, String saveDir, DirWatcher dirWatcher, String resourceId) {
		this.uploadManager = uploadManager;
		this.replicationService = replicationService;
		this.proxyFactory = proxyFactory;
		this.leaseRenewalService = leaseRenewalService;
		this.downloadManager = downloadManager;
		this.saveDir = saveDir;
		this.dirWatcher = dirWatcher;
		this.resourceId = resourceId;
	}

	private void start(DownloadListener downloadListener) {
		LOGGER.info("UPLOADSERVICE/REPLICATOR - Starting:" + proxyFactory.getAddress());
		this.downloadListener = downloadListener;
		newFileHandler = new UploaderNewFileHandler(uploadManager, downloadManager, proxyFactory.getEndPoint().toString(), saveDir, downloadListener, resourceId);
		downloadManager.initialize(saveDir, downloadListener);
		uploaderLease = registerUploader();
		downloadLease = registerDownloader();
		publishAvailability();
		
		startDirWatcher();
	}

	public void publishAvailability() {
		uploadManager.publishAvailability(new File(saveDir));
	}
	
	public void stop() {
		LOGGER.info("REPLICATOR - Stopping");
		leaseRenewalService.cancelLeaseRenewal(uploaderLease);
		leaseRenewalService.cancelLeaseRenewal(downloadLease);
	}


	private MetaInfo publishIt(File file, boolean forceUpdate) throws NoSuchAlgorithmException, IOException {
		LOGGER.info("Publishing:" + FileUtil.getPath(file));
		final MetaInfo metaInfo = new MetaInfo(file, ReplicatorProperties.getChunkSizeKB());
		downloadManager.addToDownloaded(metaInfo.hash(),file.getPath(), file.getName());
		uploadManager.newFile(metaInfo, forceUpdate);
		return metaInfo;
	}
	
	@Deprecated
	public byte[] download(String hash, int pieceNumber) {
		return download(hash, pieceNumber, "old-rep-lib");
	}
	public byte[] download(String hash, int pieceNumber, String downloaderHost) {
		LOGGER.info(String.format("%s - %s is downloading piece:%d hash %s", ReplicationServiceImpl.TAG, downloaderHost, pieceNumber, hash));
		Piece piece = uploadManager.findPiece(hash, pieceNumber);
		if (piece == null) {
			String msg = String.format("%s - failed to find piece %d for hash %s", ReplicationServiceImpl.TAG, pieceNumber, hash);
			LOGGER.warn(msg);
			return new byte[] { '\0','\0' };
		} else {
			try {
				return piece.readPiece();
			} catch (FileNotFoundException t) {
				LOGGER.warn("This Uploader doesnt have:" + piece);
				return new byte[] { '\0','\0' };
			} catch (Throwable t) {
				String msg = String.format("%s - %s Exception -failed to find piece %d for hash %s", ReplicationServiceImpl.TAG, downloaderHost, pieceNumber, hash);
				LOGGER.error(msg, t);
				throw new RuntimeException("Failed to handle piece:" + msg, t);
			}
		}
	}
	
	public void deployBundle(String bundle) throws Exception {
		File file = new File(bundle);
		LOGGER.info("REPLICATOR - deploying bundle " + FileUtil.getPath(file));
		final MetaInfo metaInfo = publishIt(file, false);
		this.downloadListener.downloadComplete(file, metaInfo.hash());
		LOGGER.info("REPLICATOR - done deploying bundle " + FileUtil.getPath(file));
	}

	
	public void deployFile(String filename, boolean forceUpdate) throws Exception {
		publishIt(new File(filename), forceUpdate);
	}
	
	private void startDirWatcher() {
		proxyFactory.getScheduler().scheduleWithFixedDelay(dirWatcher, 1, ReplicatorProperties.getDirPollInterval(), TimeUnit.SECONDS);
		dirWatcher.registerEventHandler(new DirEventHandler() {

			public void created(File file) {
				try {
					if (downloadManager.isJustDownloaded(file)) return;
					LOGGER.info("going to replicate:" + file.getPath());
					uploadManager.newFile(file);
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}

			public void deleted(File toRemove) {
				try {
					LOGGER.info("deleting:" + toRemove.getPath());
					replicationService.remove("UploadService, deleted:" + toRemove.getName(), new Upload("", toRemove.getName(), toRemove.getParent(), 1));
					toRemove.delete();
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}

			}

			public void modified(File file) {
				try {
					if (downloadManager.isJustDownloaded(file)) return;
					LOGGER.info("file updated, replicating:" + file.getPath());
					uploadManager.newFile(file);
				} catch (Exception e) {
					LOGGER.error(e.getMessage(), e);
				}
			}

			public void stop() {
			}
			
		});
	}
	
	
	private String registerDownloader() {
		String lease = replicationService.registerNewFileListener(newFileHandler, newFileHandler.getId(), ReplicatorProperties.getLeasePeriod());
		leaseRenewalService.add(new Renewer(replicationService, new Registrator() {
			public String register() {
				return replicationService.registerNewFileListener(newFileHandler, newFileHandler.getId(), ReplicatorProperties.getLeasePeriod());
			}

			public String info() {
				return replicationService.toString();
			}

			public void registrationFailed(int failedCount) {
			}}, lease, ReplicatorProperties.getLeasePeriod(), "UploadService-Downloader", LOGGER), ReplicatorProperties.getLeaseRenewPeriod(), lease);
		return lease;
	}

	private String registerUploader() {
		String uploaderLease = replicationService.registerUploader(new FileUploader(proxyFactory.getEndPoint()), ReplicatorProperties.getLeasePeriod());
		leaseRenewalService.add(new Renewer(replicationService, new Registrator() {
			public String register() {
				return replicationService.registerUploader(new FileUploader(proxyFactory.getEndPoint()), ReplicatorProperties.getLeasePeriod());
			}

			public String info() {
				return replicationService.toString();
			}

			public void registrationFailed(int failedCount) {
			}}, uploaderLease, ReplicatorProperties.getLeasePeriod(), "UploadService-Uploader", LOGGER), ReplicatorProperties.getLeaseRenewPeriod(), uploaderLease);
		return uploaderLease;
	}
	
	
	public static UploadService run(LookupSpace lookup, ProxyFactory proxyFactory, String saveDir, DownloadListener downloadListener, int port, String resourceId) throws URISyntaxException, IOException {
		try {
			port = proxyFactory.getAddress().getPort();
			
			ReplicationService replicationService = SpaceServiceImpl.getRemoteService("UploadRun", ReplicationService.class, lookup, proxyFactory, ReplicationService.NAME, true, false);
			LeaseRenewalService leaseRenewalService = new LeaseRenewalService(replicationService, proxyFactory.getScheduler());
			
			
			UploadManager uploadManager = new UploadManager(replicationService, leaseRenewalService, port);
			
			uploadManager.setPort(port);
			
			final String root = ReplicatorProperties.getRFSRoot();
			
//			// delayed write so it gets replicated
//			proxyFactory.getScheduler().schedule(new Runnable() {
//				public void run() {
//					writeReadTxtFile(root, "readme.txt", readme);
//				}
//			}, 10, TimeUnit.SECONDS);
			
			DirWatcher dirWatcher = new DirWatcher(null, new File(root));


			DownloadManager downloadManager = new DownloadManager(replicationService, leaseRenewalService, proxyFactory, new SysBouncer() {
				public void bounce(ResourceAgent agent) {
					try {
						agent.bounce(true);
					} catch (Exception e) {
					}
				}}, resourceId);
			UploadService uploadService = new UploadService(uploadManager, replicationService, proxyFactory, leaseRenewalService, downloadManager, saveDir, dirWatcher, resourceId);
			uploadService.start(downloadListener);
			LOGGER.info("Register " + Uploader.NAME + " @" + proxyFactory.getAddress());
			proxyFactory.registerMethodReceiver(Uploader.NAME, uploadService);
			return uploadService;
		} catch(Throwable t){
			LOGGER.error(t.getMessage(), t);
			throw new RuntimeException(t.getMessage(), t);
		}
	}


	private static void writeReadTxtFile(String root, String filename, String contents){
		try {
			File rootFile = new File(root);
			rootFile.mkdir();
			FileOutputStream fos = new FileOutputStream(new File(rootFile, filename));
			fos.write(contents.getBytes());
			fos.close();
		} catch (Exception e) {
			LOGGER.warn("Failed to write readme.txt:" + e.getMessage());
		}
	}
	static String readme = 
		"RFS README\n" +
		"==========\n" +
		"Seeding.\nThe RFS will seed from only those nodes with Uploaders running - this includes all ManagementHosts and those\n" +
		"found on the Replicator.services SLA. By default it will run 3 non-management uploaders, everything will be a Downloader.\n" +
		"Uploaders support Create/Modify/Delete propogation to all peers (also supporting nested directories).\n" +
		"Downloaders, on the otherhand are ReadOnly; any modifications to the RFS will not be replicated.\n\n" +
		"Files  with a '.' first char or a '.tmp' extension are ignored.\n\n" +
		"The RFS is periodically scanned for updates (creation, deletion, modified) - and those changes replicated.\n" +
		"When operation directly on the RFS it is safer to use either '.' first name char or '.tmp' extension.\n" + 
		"Then once complete rename the file, in doing operation making the operation atomic.\n";
	;


	public void start() {
	}
}
