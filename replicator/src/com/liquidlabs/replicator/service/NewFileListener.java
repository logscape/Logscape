package com.liquidlabs.replicator.service;

import com.liquidlabs.transport.proxy.clientHandlers.FailFastAndDisable;
import com.liquidlabs.transport.proxy.Remotable;

public interface NewFileListener extends Remotable {
	
	@FailFastAndDisable
	void newFileUploaded(String fileName, String path, String hash, int pieces);

	@FailFastAndDisable
	void removeFile(String hash, String fileName, String path, boolean forceRemove);
	
	String getId();

}
