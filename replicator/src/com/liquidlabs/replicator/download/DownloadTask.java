package com.liquidlabs.replicator.download;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.PieceInfo;
import com.liquidlabs.replicator.service.ReplicationServiceImpl;
import com.liquidlabs.replicator.service.Uploader;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl.ADDRESSING;

public class DownloadTask implements Callable<DownloadResult>{
	private static final Logger LOGGER = Logger.getLogger(DownloadTask.class);
	private final PieceInfo pieceInfo;
	private final FileChannel out;
	private Meta meta;
	private final HashGenerator hasher;
	private List<DownloadTaskListener> listeners = new ArrayList<DownloadTaskListener>();
	private final ProxyFactory proxyFactory;
	private final List<String> uris;

	public DownloadTask(ProxyFactory proxyFactory, PieceInfo pieceInfo, FileChannel out, HashGenerator hasher, List<String> uris) {
		this.proxyFactory = proxyFactory;
		this.pieceInfo = pieceInfo;
		this.out = out;
		this.hasher = hasher;
		this.uris = new ArrayList<String>(uris);
	}
	
	public void addDownloadTaskListenener(DownloadTaskListener listener) {
		listeners.add(listener);
	}
	
	public void use(Meta meta) {
		this.meta = meta;
	}
	
	private void fireDownloadTaskComplete() {
		for (DownloadTaskListener listener : listeners) {
			listener.taskComplete(pieceInfo, meta);
		}
	}
	
	private void fireDownloadTaskHashFailed(PieceInfo pieceInfo2, Meta meta2) {
		for (DownloadTaskListener listener : listeners) {
			listener.hashFailed(pieceInfo, meta, uris);
		}
	}
	
	private void fireDownloadTaskFailed(Throwable e) {
		for (DownloadTaskListener listener : listeners) {
			listener.downloadFailed(pieceInfo, meta, e, uris);
		}
	}
	
	public DownloadResult call() throws Exception {
		
		Uploader uploadService = null;
		try {
			LOGGER.info(String.format("Attempting to download piece %d of file %s/%s from %s", pieceInfo.getPieceNumber(), meta.getPath(), meta.getFileName(), Arrays.toString(uris)));
			if (meta == null) {
				throw new RuntimeException("No host set for this download task");
			}
			
			long start = DateTimeUtils.currentTimeMillis();
			
			Collections.shuffle(uris);
			for (String uri : uris) {
				LOGGER.info(String.format("Downloading Piece:%d of file %s/%s from:%s", pieceInfo.getPieceNumber(), meta.getPath(), meta.getFileName(), uri));
				uploadService = proxyFactory.getRemoteService(Uploader.NAME, Uploader.class, ADDRESSING.KEEP_FIRST_RANDOM, uri);
				
				byte[] read = uploadService.download(meta.getHash(), pieceInfo.getPieceNumber(), NetworkUtils.getHostname());
				if (read == null) throw new RuntimeException("Client:" + NetworkUtils.getHostname() + " Failed to retrieve Hash For :" + meta );
				if (read.length == 0 || read.length == 2) {
					LOGGER.warn(pieceInfo + " doesnt exist on the server, retrying:" + uploadService);
					continue;
				}
				String createHash = hasher.createHash(read);
				if (createHash.equals(pieceInfo.getHash())) {
					ByteBuffer allocate = ByteBuffer.wrap(read);
					out.write(allocate, pieceInfo.getStart());
					long elapsed = DateTimeUtils.currentTimeMillis() - start;
					LOGGER.info(String.format("%s - %s SUCCESS downloaded Piece %d of file %s/%s from %s bytes[%d] elapsed[%d]ms", ReplicationServiceImpl.TAG, uri, pieceInfo.getPieceNumber(), meta.getPath(), meta.getFileName(), uploadService, read.length, elapsed));
					
					fireDownloadTaskComplete();
					return new DownloadResult(pieceInfo, true, false);
				}
				LOGGER.warn(String.format("%s - %s Hash Mismatch - FAILED to download Piece %d of file %s/%s from %s", ReplicationServiceImpl.TAG, uri, pieceInfo.getPieceNumber(), meta.getPath(), meta.getFileName(), uploadService));
			}
			LOGGER.error(String.format("%s - %s Hash Mismatch - FAILED to download Piece %d of file %s/%s from %s", ReplicationServiceImpl.TAG, Arrays.toString(uris), pieceInfo.getPieceNumber(), meta.getPath(), meta.getFileName(), uploadService));
			return new DownloadResult(pieceInfo, false, false);
			
			

		} catch (Throwable e) {
			// this gets really noisy!
			LOGGER.info(e.toString());
			fireDownloadTaskFailed(e);
			return new DownloadResult(pieceInfo, false, true);
		} finally {
		}
	}

	

	

}
