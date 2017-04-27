package com.liquidlabs.replicator.service;



public interface Uploader {
	public static final String NAME = Uploader.class.getSimpleName();
	void deployBundle(String file) throws Exception;
	void deployFile(String theFile, boolean forceUpload) throws Exception;

	/**
	 * User the version that passes in the downloaderHost instead
	 * @param hash
	 * @param pieceNumber
	 * @return
	 */
	@Deprecated
	byte[] download(String hash, int pieceNumber);
	byte[] download(String hash, int pieceNumber, String downloaderHost);
	void publishAvailability();
}
