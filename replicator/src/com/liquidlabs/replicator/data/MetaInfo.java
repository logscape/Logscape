package com.liquidlabs.replicator.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.vso.VSOProperties;

public class MetaInfo {

	private final File file;
	private final int chunkSizeBytes;
	private Piece[] pieces;
	private String hash;
	private HashGenerator hasher = new HashGenerator();

	public MetaInfo(File file, int chunkSizeKb) throws NoSuchAlgorithmException, IOException {
		this.file = file;
		this.chunkSizeBytes = 1024 * chunkSizeKb;
		pieces = createPieces();
		this.hash = hasher.createHash(file.getName(), file);
	}

	public Piece[] pieces() {
		return pieces;
	}

	private Piece[] createPieces() throws NoSuchAlgorithmException, IOException {
		int nPieces = (int) (file.length() / chunkSizeBytes);
		int remainder = (int) (file.length() % chunkSizeBytes);
		nPieces += remainder == 0 ? 0 : 1;
		Piece[] pieces = new Piece[nPieces];
		for (int i = 0; i < pieces.length; i++) {
			int length = chunkSizeBytes;
			if (remainder > 0 && i == pieces.length - 1) {
				length = remainder;
			}
			pieces[i] = new Piece(i * chunkSizeBytes, length, file);

		}
		return pieces;
	}

	public Meta generateInfo(String hostname, int port) throws FileNotFoundException {
		Meta meta = new Meta(file.getName(), file.getParent(), hash, hostname, port, VSOProperties.isManager());
		for (int i = 0; i < pieces.length; i++) {
			meta.addPieceInfo(i, pieces[i].length(), pieces[i].hash(), pieces[i].start());
		}
		return meta;
	}

	public int pieceCount() {
		return pieces.length;
	}


	public String name() {
		return file.getName();
	}
	public String path() {
		return file.getParent();
	}

	public String hash() {
		return hash;
	}
	public long fileLength() {
		return file.length();
	}
	public String toString() {
		return getClass().getSimpleName() + "name:" + file.getAbsolutePath() + " hash:" + hash + " fileSize:" + file.length() + " lastmod:" + file.lastModified();
	}

	public File file() {
		return file;
	}

}
