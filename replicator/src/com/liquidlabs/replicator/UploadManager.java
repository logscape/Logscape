package com.liquidlabs.replicator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.Unpacker;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.MetaInfo;
import com.liquidlabs.replicator.data.Piece;
import com.liquidlabs.replicator.data.Upload;
import com.liquidlabs.replicator.service.ReplicationService;
import com.liquidlabs.replicator.service.ReplicationServiceImpl;
import com.liquidlabs.space.lease.LeaseRenewer;
import com.liquidlabs.space.lease.Registrator;
import com.liquidlabs.space.lease.Renewer;

public class UploadManager {

	private static final int META_RENEW_REQUENCY = ReplicatorProperties.getLeaseRenewPeriod();
	private static final int META_EXPIRES_IN = ReplicatorProperties.getLeasePeriod();
	private static final Logger LOGGER = Logger.getLogger(UploadManager.class);
	private final ReplicationService replicator;
	private final LeaseRenewer leaseRenewer;
	private final String hostname;
	private final Map<String, MetaInfo> managed = new ConcurrentHashMap<String, MetaInfo>();
	private int receiverPort;
	private final Map<String, String> leases = new ConcurrentHashMap<String, String>();

	public UploadManager(ReplicationService replicator, LeaseRenewer leaseRenewer, int receiverPort) {
		this.replicator = replicator;
		this.leaseRenewer = leaseRenewer;
		this.receiverPort = receiverPort;
		try {
			hostname = NetworkUtils.getIPAddress();
		} catch (Exception e) {
			LOGGER.error("Unable to determine hostname", e);
			throw new RuntimeException(e);
		}
	}

	public void publish(final MetaInfo metaInfo) {
		try {
			// empty files will have -1 hash - so ignore them
			if (metaInfo.hash().equals("-1"))
				return;
			if (!Unpacker.isValidZip(metaInfo.file())) {
				LOGGER.error("Meta, invalid ZIP found, not publishing it!:" + metaInfo);
				return;
			}
			managed.put(metaInfo.hash(), metaInfo);
			final Meta meta = metaInfo.generateInfo(hostname, receiverPort);
			LOGGER.info(String.format("Publishing Availablity of %s on %s:%s", metaInfo.toString(), meta.getHostname(), meta.getPort()));
			
			String leaseKey = replicator.publishAvailability(meta, META_EXPIRES_IN);
			if (leaseKey != null) {
				Registrator registrator = new Registrator() {
					public String info() {
						return "UploadManagerMetaRegistrator";
					}

					public String register() throws Exception {
						String leaseKey = replicator.publishAvailability(meta, META_EXPIRES_IN);
						LOGGER.info(String.format("%s Re-PublishingAvailability of:%s", ReplicationServiceImpl.TAG, leaseKey));
						leases.put(metaInfo.hash(), leaseKey);
						return leaseKey;
					}

					public void registrationFailed(int failedCount) {
					}
				};
				LOGGER.info(String.format("%s PublishingAvailability of:%s", ReplicationServiceImpl.TAG, leaseKey));
				Renewer renewer = new Renewer(replicator, registrator, leaseKey, META_EXPIRES_IN, "UploadManagerMetaRegistrator", LOGGER);

				leaseRenewer.add(renewer, META_RENEW_REQUENCY, leaseKey);
				leases.put(metaInfo.hash(), leaseKey);
				replicator.fileUploaded(new Upload(metaInfo.hash(), metaInfo.name(), metaInfo.path(), metaInfo.pieceCount()), hostname, false);
			}

		} catch (FileNotFoundException e) {
			String format = String.format("%s - Failed to publish metaInfo for %s", ReplicationServiceImpl.TAG, metaInfo.name());
			System.err.println(format);
			LOGGER.warn(format, e);
		}
	}

	public void waitForUploaders(String hash, long startSeed, String path, String filename) {
		try {
			long timeout = startSeed + 60000;
			int count = replicator.retrieveCurrentForHash(hash, path, filename).size();
			int uploaderCount = replicator.uploaderCount();
			while (count < uploaderCount && System.currentTimeMillis() < timeout) {
				Thread.sleep(1000);
				count = replicator.retrieveCurrentForHash(hash, path, filename).size();
			}
		} catch (InterruptedException e) {
		}
	}

	public MetaInfo newFile(File theFile) throws NoSuchAlgorithmException, IOException {
		MetaInfo metaInfo = new MetaInfo(theFile, ReplicatorProperties.getChunkSizeKB());
		newFile(metaInfo, false);
		return metaInfo;
	}

	public void newFile(final MetaInfo metaInfo, boolean forceUpdate) {
		try {
			managed.put(metaInfo.hash(), metaInfo);
			LOGGER.info(String.format("%s - Publishing %s/%s pieceCount:%d on %s:%d", ReplicationServiceImpl.TAG, metaInfo.path(), metaInfo.name(), metaInfo.pieceCount(), hostname, receiverPort));

			String leaseKey = replicator.publishAvailability(metaInfo.generateInfo(hostname, receiverPort), META_EXPIRES_IN);
			if (leaseKey != null) {
				Registrator registrator = new Registrator() {
					public String info() {
						return "MetaPublishAvailability";
					}

					public String register() throws Exception {
						String leaseKey = replicator.publishAvailability(metaInfo.generateInfo(hostname, receiverPort), META_EXPIRES_IN);
						leases.put(metaInfo.hash(), leaseKey);
						return leaseKey;
					}

					public void registrationFailed(int failedCount) {
					}
				};
				Renewer renewer = new Renewer(replicator, registrator, leaseKey, META_EXPIRES_IN, "MetaPublishAvailability", LOGGER);
				leaseRenewer.add(renewer, META_RENEW_REQUENCY, leaseKey);

				leases.put(metaInfo.hash(), leaseKey);

				replicator.fileUploaded(new Upload(metaInfo.hash(), metaInfo.name(), metaInfo.path(), metaInfo.pieceCount()), hostname, forceUpdate);
			}
		} catch (FileNotFoundException e) {
			String format = String.format("%s - Failed to publish metaInfo for %s", ReplicationServiceImpl.TAG, metaInfo.name());
			LOGGER.warn(format, e);
			throw new RuntimeException(format);
		}
	}

	public Piece findPiece(String hash, Integer pieceNumber) {
		MetaInfo metaInfo = managed.get(hash);
		if (metaInfo == null) {
			LOGGER.warn(String.format("%s - Failed to find a replicated file with hash = %s", ReplicationServiceImpl.TAG, hash));
			Collection<MetaInfo> values = managed.values();
			for (MetaInfo m : values) {
				LOGGER.info("File:" + m.name() + " h:" + m.hash());
			}
			return null;
		}

		Piece[] pieces = metaInfo.pieces();
		if (pieces.length <= pieceNumber) {
			LOGGER.warn(String.format("%s - Failed to find piece number %d for replicated file with hash = %s", ReplicationServiceImpl.TAG, pieceNumber, hash));
			return null;
		}

		return pieces[pieceNumber];
	}

	

	public void publishAvailability(File deployDir) {
		LOGGER.info("Publish availability:" + deployDir.getAbsolutePath());
		File[] files = deployDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return isValid(name);
			}
		});
		for (File file : files) {
			try {
				publish(new MetaInfo(file, ReplicatorProperties.getChunkSizeKB()));
			} catch (Exception e) {
				LOGGER.warn(String.format("%s - Failed to publish metaInfo for %s", ReplicationServiceImpl.TAG, file.getName()));
			}
		}

	}

    boolean isValid(String name) {
        return !name.startsWith(".") && !name.endsWith(".bak");
    }


    public void retract(String hash) {
		String leaseKey = leases.remove(hash);
		if (leaseKey != null) {
			LOGGER.info(String.format("%s Cancelling Lease:%s", ReplicationServiceImpl.TAG, leaseKey));
			leaseRenewer.cancelLeaseRenewal(leaseKey);
		} else {
			LOGGER.warn("REPLICATOR Failed to find leaseKey for hash:" + hash);
		}
	}

	public void stop() {
		for (String lease : leases.keySet()) {
			leaseRenewer.cancelLeaseRenewal(lease);
		}
		leases.clear();
	}

	public void setPort(int myPort) {
		this.receiverPort = myPort;
	}

}
