/**
 * 
 */
package com.liquidlabs.replicator;

import java.util.ArrayList;
import java.util.List;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.replicator.data.FileUploader;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.Upload;
import com.liquidlabs.replicator.service.MetaUpdateListener;
import com.liquidlabs.replicator.service.NewFileListener;
import com.liquidlabs.replicator.service.ReplicationService;

public class FakeReplicationService implements ReplicationService {
	private List<Meta> metas = new ArrayList<Meta>(); 
	public String publishAvailability(Meta metaInfo, int expireIn) {
		metas.add(metaInfo);
		System.out.println("ReplicationService:" + metaInfo + " pieces:" + metaInfo.getPieces().length);
		return "foo";
	}

	public void cancelLease(String leaseKey) {
	}

	public void renewLease(String leaseKey, long expires) {
	}

	public List<Meta> retrieveCurrentForHash(String hash, String path, String filename) {
		return metas;
	}

	public void start() {
	}

	public void stop() {
		// TODO Auto-generated method stub
		
	}

	public String registerMetaUpdateListener(
			MetaUpdateListener metaUpdateHandler, String hash,
			int leaseExpiry, String listenerId) {
		return "false-lease";
	}

	public String registerNewFileListener(NewFileListener newFileListener,
			String listenerId, int leaseExpiry) {
		return "fake-lease";
	}

	public void fileUploaded(Upload upload, String hostname, boolean forceUpdate) {
	}

	public List<Upload> getDeployedFiles() {
		return null;
	}

	public void remove(String client, Upload upload) {
	}

	public String registerUploader(FileUploader uploader, int leaseExpiry) {
		return null;
	}

	public int uploaderCount() {
		return 0;
	}

	public void renewLease(String leaseKey, int expires) {
	}

	public void unregisterNewFileListener(String listenerId) {
	}
	
}