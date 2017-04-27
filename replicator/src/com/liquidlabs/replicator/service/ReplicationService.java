package com.liquidlabs.replicator.service;

import java.util.List;

import com.liquidlabs.common.LifeCycle;
import com.liquidlabs.replicator.data.FileUploader;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.Upload;
import com.liquidlabs.space.lease.Leasor;
import com.liquidlabs.transport.proxy.ReplayOnAddressChange;

public interface ReplicationService extends Leasor, LifeCycle {

	String NAME = ReplicationService.class.getSimpleName();
	String publishAvailability(Meta metaInfo, int expireIn);
	List<Meta> retrieveCurrentForHash(String hash, String path, String filename);
	
	@ReplayOnAddressChange
	String registerNewFileListener(NewFileListener newFileListener, String listenerId, int leaseExpiry);
	
	@ReplayOnAddressChange
	String registerMetaUpdateListener(MetaUpdateListener metaUpdateHandler,
			String hash, int leaseExpiry, String listenerId);
	void fileUploaded(Upload upload, String hostname, boolean forceUpdate);
	List<Upload> getDeployedFiles();
	void remove(String client, Upload upload);
	
	@ReplayOnAddressChange
	String registerUploader(FileUploader uploader, int leaseExpiry);
	int uploaderCount();
	void unregisterNewFileListener(String listenerId);

}
