package com.liquidlabs.replicator.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.liquidlabs.replicator.ReplicatorProperties;
import com.liquidlabs.replicator.data.FileUploader;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.Upload;
import com.liquidlabs.transport.proxy.events.Event;
import com.liquidlabs.transport.proxy.events.Event.Type;
import com.liquidlabs.vso.Notifier;
import com.liquidlabs.vso.SpaceService;

public class ReplicationServiceImpl implements ReplicationService {
	
	private final SpaceService spaceService;
	private static final Logger LOGGER = Logger.getLogger(ReplicationServiceImpl.class);
	public final static String TAG = "REPLICATOR";
	
	public ReplicationServiceImpl(SpaceService spaceService) {
		this.spaceService = spaceService;
		LOGGER.info(String.format("%s - Constructed ReplicationService", TAG));
	}

	public String publishAvailability(Meta metaInfo, int expireIn) {
		if(metaInfo.isManager()){
			LOGGER.info(String.format("%s - %s Meta Available on %s:%d hash[%s]", TAG, metaInfo.getFileName(), metaInfo.getHostname(), metaInfo.getPort(), metaInfo.getHash()));
			return spaceService.store(metaInfo, expireIn);
		} else {
			List<Meta> found = spaceService.findObjects(Meta.class, String.format("fileName equals '%s' AND hash equals '%s' AND manager equals true", metaInfo.getFileName(), metaInfo.getHash()),false,-1);
			if (!found.isEmpty()){
				return spaceService.store(metaInfo, expireIn);
			} else {
				LOGGER.warn(String.format("%s - %s Meta hash from host %s doesn't match that of the manager", TAG, metaInfo.getFileName(), metaInfo.getHostname()));
			}
		}
		return null;
	}

	public void fileUploaded(final Upload upload, String hostname, boolean forceUpdate) {
		try {
			List<Upload> uploaded = spaceService.findObjects(Upload.class, String.format("fileName equals '%s' and path equals '%s'", upload.fileName, upload.path), false, 1);
			boolean exists = false;
			for (Upload upload2 : uploaded) {
				if (!upload2.equals(upload)) {
					spaceService.remove(Upload.class, upload2.id);
				} else {
					exists = true;
				}
			}
			LOGGER.info(String.format("%s - %s %s has been uploaded existing[%b] forced[%b] hash[%s]", TAG,hostname, upload.fileName, exists, forceUpdate, upload.hash));
			
			if (forceUpdate || !exists) {
				spaceService.getScheduler().schedule(new Runnable() {
					public void run() {
						// allow for the deletes to happen before the download of the replacement
						spaceService.store(upload, -1);
					}
				}, ReplicatorProperties.getPauseSecsBetweenDeleteAndDownload(), TimeUnit.SECONDS);
			}
		} catch (Throwable t) {
			LOGGER.warn(t.getMessage(), t);
		}
	}
	
	public String registerUploader(FileUploader uploader, int leaseExpiry) {
		LOGGER.info(String.format("%s - Registered Uploader %s", TAG, uploader.getAgentAddress()));
		return spaceService.store(uploader, leaseExpiry);
	}
	
	public int uploaderCount() {
		return spaceService.findObjects(FileUploader.class, null, false, Integer.MAX_VALUE).size();
	}
	
	Map<String, String> forceRemove = new ConcurrentHashMap<String, String>();
	public void remove(String client, Upload upload) {
		try {
			if (client.contains("forceRemove=true")) forceRemove.put(upload.path + upload.fileName, "");
			
			LOGGER.info(String.format("%s - clientContext[%s] %s/%s is being removed", TAG, client, upload.path, upload.fileName));
			List<Meta> metaist = spaceService.findObjects(Meta.class, String.format("fileName equals '%s' AND path equals '%s'", upload.fileName, upload.path), false, -1);
			if (metaist.size() > 0) {
				List<Upload> removed = spaceService.remove(Upload.class, String.format("fileName equals '%s' AND path equals '%s'", upload.fileName, upload.path), 100);
				LOGGER.info("Upload Removed:" + removed);
				if (removed == null && removed.size() == 0) LOGGER.warn("Failed to find Upload:" + upload.fileName + " remove events will fail");
				int purge = spaceService.purge(Meta.class, String.format("fileName equals '%s' AND path equals '%s'", upload.fileName, upload.path));
				LOGGER.info(String.format("%s - Purged %d Meta details for file %s with hash %s", TAG, purge, upload.fileName, upload.hash));
			} else {
				LOGGER.warn("%s Remove failed to find upload/meta for:" + upload.fileName);
				List<Meta> ms = spaceService.findObjects(Meta.class, "", false, -1);
				for (Meta meta : ms) {
					LOGGER.info(meta.toString());
				}
			}
			Thread.sleep(5000);
		} catch (Throwable t) {
			LOGGER.warn("%s Failed to remove:" + upload.fileName + " ex:" + t.getMessage(), t);
		} finally {
			if ((upload != null && upload.path != null)) forceRemove.remove(upload.path + upload.fileName);
			LOGGER.info("done");
		}
	}
	
	public List<Meta> retrieveCurrentForHash(String hash, String path, String filename) {
		return spaceService.findObjects(Meta.class, String.format("hash equals %s AND fileName equals '%s' and path equals '%s'", hash.trim(), filename, path), false, Integer.MAX_VALUE);
	}
	
	public String registerNewFileListener(final NewFileListener eventListener, String listenerId, int leaseExpiry) {
		String leaseKey = spaceService.registerListener(Upload.class, null, new Notifier<Upload>() {
			public void notify(Type event, Upload result) {
				if (event == Type.WRITE) {
					try {
						LOGGER.info("NewFileNotify:" + eventListener + " file:" + result.fileName);
						eventListener.newFileUploaded(result.fileName, result.path, result.hash, result.pieces);
					} catch (Throwable t) {
						LOGGER.warn("Notify failed:" + eventListener, t);
					}
				} else if (event == Type.TAKE) {
					if (result.when < new DateTime().minusSeconds(ReplicatorProperties.getNewFileSafeSeconds()).getMillis()) {
						LOGGER.info(String.format("Notifying Remove File:%s [%s/%s]", eventListener, result.fileName, result.path));
						eventListener.removeFile(result.hash, result.fileName, result.path, forceRemove.containsKey(result.path + result.fileName));
					} else {
						// crappy hack for problem where an uploaded file is immediately deleted
						LOGGER.info(String.format("Cannot remove file, 'too new' file[%s] lastMod[%s]", result.fileName, new DateTime(result.when)));
					}
				} else {
					try {
						LOGGER.info("NewFile UPDATED:" + eventListener + " file:" + result.fileName);		
						eventListener.newFileUploaded(result.fileName, result.path, result.hash, result.pieces);
					} catch (Throwable t) {
						LOGGER.warn("Notify failed:" + eventListener, t);
					}					
				}
			}}, listenerId, leaseExpiry, new Event.Type[] { Type.WRITE, Type.TAKE, Type.UPDATE });
		LOGGER.info(String.format("%s - fileListener registered id:%s address:%s lease:%s", TAG, listenerId, eventListener.toString(), leaseKey));
		return leaseKey;
		
	}
	
	public String registerMetaUpdateListener(final MetaUpdateListener metaUpdateHandler, String hash, int leaseExpiry, String listenerId) {
		String leaseKey = spaceService.registerListener(Meta.class, "hash equals " + hash, new Notifier<Meta>() {
			public void notify(Type event, Meta result) {
				metaUpdateHandler.newHost(result);
			}}, listenerId, leaseExpiry, new Event.Type[] {Type.WRITE});
		LOGGER.info(String.format("%s - metaUpdateListener registered id:%s address:%s lease:%s", TAG, listenerId, metaUpdateHandler.toString(), leaseKey));
		return leaseKey;
	}

	public void cancelLease(String leaseKey) {
		LOGGER.info("Cancelling Lease:" + leaseKey);
		spaceService.cancelLease(leaseKey);
	}

	public void renewLease(String leaseKey, int expires) {
		try {
			spaceService.renewLease(leaseKey, expires);
		} catch (Throwable t) {
			LOGGER.warn("RenewLease problem:" + " lease:" + leaseKey + " msg:" + t.getMessage());
			//throw new RuntimeException("Failed to find lease:" + leaseKey);
		}
	}


	public void start() {
		spaceService.start(this, "replicator-1.0");
		LOGGER.info(String.format("%s - ReplicationService Started", TAG));
	}


	public void stop() {
		spaceService.stop();		
	}
	
	public List<Upload> getDeployedFiles() {
		List<Upload> deployed = spaceService.findObjects(Upload.class, null, false, Integer.MAX_VALUE);
		return deployed;
	}

	public void unregisterNewFileListener(String listenerId) {
		spaceService.unregisterListener(listenerId);
	}

}
