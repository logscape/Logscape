package com.liquidlabs.replicator.download;

import java.util.List;

import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.PieceInfo;

public interface DownloadTaskListener {

	void taskComplete(PieceInfo pieceInfo, Meta meta);

	void hashFailed(PieceInfo pieceInfo, Meta meta, List<String> uris);

	void downloadFailed(PieceInfo pieceInfo, Meta meta, Throwable e, List<String> uris);

}
