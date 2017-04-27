package com.liquidlabs.replicator.service;

import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.log4j.Logger;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.replicator.ReplicatorProperties;
import com.liquidlabs.replicator.SysBouncer;
import com.liquidlabs.replicator.download.DownloadListener;
import com.liquidlabs.replicator.download.DownloadManager;
import com.liquidlabs.space.lease.LeaseRenewalService;
import com.liquidlabs.space.lease.Registrator;
import com.liquidlabs.space.lease.Renewer;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.SpaceServiceImpl;
import com.liquidlabs.vso.agent.ResourceAgent;
import com.liquidlabs.vso.lookup.LookupSpace;

public class DownloadService implements LifeCycle, Registrator{
	
	private static final Logger LOGGER = Logger.getLogger(DownloadService.class);
	
	private final DownloadManager downloadManager;
	private final ReplicationService replicationService;
	private final ProxyFactory proxyFactory;
	private final LeaseRenewalService leaseRenewalService;
	private final String saveDir;
	private NewFileListener newFileHandler;

	private String lease;

	private final String resourceId;
	
	public DownloadService(DownloadManager downloadManager, ReplicationService replicationService, ProxyFactory proxyFactory, LeaseRenewalService leaseRenewalService, String saveDir, String resourceId) {
		this.downloadManager = downloadManager;
		this.replicationService = replicationService;
		this.proxyFactory = proxyFactory;
		this.leaseRenewalService = leaseRenewalService;
		this.saveDir = saveDir;
		this.resourceId = resourceId;
		if (!new File(saveDir).exists()) new File(saveDir).mkdirs();
	}
	
	public void start(DownloadListener downloadListener) {
		try {
			newFileHandler = new DownloaderNewFileHandler(downloadManager, proxyFactory.getEndPoint().toString(), downloadListener, replicationService, resourceId);
			downloadManager.initialize(saveDir, downloadListener);
			lease = replicationService.registerNewFileListener(newFileHandler, newFileHandler.getId(), ReplicatorProperties.getLeasePeriod());

            if (lease == null){
                String msg = "Got NULL LEASE From : ReplicationService.registerNewFileListener";
                LOGGER.error(msg);
                throw new RuntimeException(msg);
            }
			
			Renewer leaseThing = new Renewer(replicationService, this, lease, ReplicatorProperties.getLeasePeriod(), "DownloadService", LOGGER);
			leaseRenewalService.add(leaseThing, ReplicatorProperties.getLeaseRenewPeriod(), lease);
			
			LOGGER.info("DOWNLOAD SERVICE STARTED");
		} catch (Throwable t) {
			LOGGER.error("Failed to start download service lease:" + lease, t);
			throw new RuntimeException("Failed to start DownloadService", t);
		}
	}
	
	public void stop() {
		LOGGER.info("REPLICATOR - Stopping");
		leaseRenewalService.cancelLeaseRenewal(lease);
		replicationService.unregisterNewFileListener(newFileHandler.getId());
	}
	
	public static DownloadService run(LookupSpace lookup, ProxyFactory proxyFactory, String saveDir, DownloadListener downloadListener, String resourceId) {
		try {
			ReplicationService replicationService = SpaceServiceImpl.getRemoteService("DownloadRun", ReplicationService.class, lookup, proxyFactory, ReplicationService.NAME, true, false);
			ScheduledExecutorService downloadScheduler = ExecutorService.newScheduledThreadPool("services");
			LeaseRenewalService leaseRenewalService = new LeaseRenewalService(replicationService, downloadScheduler);
			
			final String root = ReplicatorProperties.getRFSRoot();
			writeReadTxtFile(root, "downloader", "");
			
			DownloadService downloadService = new DownloadService(new DownloadManager(replicationService, leaseRenewalService, proxyFactory, new SysBouncer() {
				public void bounce(ResourceAgent agent) {
					try {
						agent.bounce(true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}}, resourceId), replicationService, proxyFactory, leaseRenewalService, saveDir, resourceId);
			downloadService.start(downloadListener);
			return downloadService;
		} catch (Throwable t){
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


	public void start() {
		LOGGER.info("REPLICATOR - Starting");
	}

	

	public String register() {
		return replicationService.registerNewFileListener(newFileHandler, newFileHandler.getId(), ReplicatorProperties.getLeasePeriod());
	}
	public String info() {
		return replicationService.toString();
	}

	@Override
	public void registrationFailed(int failedCount) {
		// TODO Auto-generated method stub
		
	}

}
