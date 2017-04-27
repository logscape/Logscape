/**
 * 
 */
package com.liquidlabs.replicator.download;

import com.liquidlabs.replicator.data.PieceInfo;

class DownloadResult {
	private final PieceInfo pieceInfo;
	final boolean success;
	final boolean retryRequired;

	public DownloadResult(PieceInfo pieceInfo, boolean success, boolean retryRequired) {
		this.pieceInfo = pieceInfo;
		this.success = success;
		this.retryRequired = retryRequired;
	}
	public PieceInfo getPieceInfo() {
		return pieceInfo;
	}
	
}