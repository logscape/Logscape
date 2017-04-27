package com.liquidlabs.replicator.service;

import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.download.Downloader;

public class MetaUpdateHandler implements MetaUpdateListener {

	transient private final Downloader downloader;
	private String id = "MetaUpdateHandler";

	public MetaUpdateHandler(Downloader downloader, String resourceId) {
		this.downloader = downloader;
		this.id = getClass().getSimpleName() + PIDGetter.getPID();
	}

	public String getId() {
		return id;
	}

	
	public void newHost(Meta metaInfo) {
		downloader.addMeta(metaInfo);
	}

}
