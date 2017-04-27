package com.liquidlabs.replicator.data;

import com.liquidlabs.common.HashGenerator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.NoSuchAlgorithmException;

public class Piece {

	private final int start;
	private final int length;
	private final File file;
	private String hash;
	private byte[] responseStart;


	public Piece(int start, int length, File file) throws IOException, NoSuchAlgorithmException {
		this.start = start;
		this.length = length;
		this.file = file;
		this.hash = new HashGenerator().createHash(readPiece());
		String msg = "0~" + length + "~";
		responseStart = msg.getBytes();
	}

	public int start() {
		return start;
	}

	public int length() {
		return length;
	}

	public String hash() {
		return hash;
	}

	public synchronized byte [] readPiece() throws IOException {
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		try {
			raf.seek(start);
			byte [] buf = new byte[length];
			raf.read(buf);
			return buf;
		} finally {
			raf.close();
		}
	}
	public String toString() {
		return super.toString() + " file:" + this.file.getName() + " s:" + this.start;
	}
}
