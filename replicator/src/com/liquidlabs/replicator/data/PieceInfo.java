package com.liquidlabs.replicator.data;

public class PieceInfo {

	private final int pieceNumber;
	private final String hash;
	private final int start;
	private final int length;

	public PieceInfo(int pieceNumber, String hash, int start, int length) {
		this.pieceNumber = pieceNumber;
		this.hash = hash;
		this.start = start;
		this.length = length;
	}

	public int getPieceNumber() {
		return pieceNumber;
	}

	public String getHash() {
		return hash;
	}

	public int getStart() {
		return start;
	}

	public int getLength() {
		return length;
	}
	
	

}
