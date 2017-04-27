package com.liquidlabs.replicator.download;

import java.io.File;


public interface DownloadListener {
	public void downloadComplete(File tempFile, String hash);
	public void downloadRemoved(File name);
}
