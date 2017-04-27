package com.liquidlabs.common.concurrent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Compress {
	private final String header;
	private byte[] headerBytes;

	public Compress(String header){
		this.header = header;
		this.headerBytes = this.header.getBytes();
	}

	public byte[] getGZipBytesWithHeader(byte[] sendPacket) throws IOException {
		byte[] gzip = gzip(sendPacket);
		
		byte[] result = new byte[headerBytes.length + gzip.length];
		System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
		System.arraycopy(gzip, 0, result, headerBytes.length, gzip.length);
		return result;
	}

	public boolean isCompressed(byte[] packet) {
		for (int i = 0; i < headerBytes.length; i++){
			if (headerBytes[i] != packet[i]) return false;
		}
		return true;
	}

	public byte[] getUnGzipBytesWithHeader(byte[] recPacket) throws IOException {
		byte[] result = new byte[recPacket.length - headerBytes.length];
		System.arraycopy(recPacket, headerBytes.length, result, 0, result.length);
		return this.ungzip(result);
	}
	
	public byte[] ungzip(final byte[] gzipped) throws IOException {
		final GZIPInputStream inputStream = new GZIPInputStream(new ByteArrayInputStream(gzipped));
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(gzipped.length);
		final byte[] buffer = new byte[4096];
		int bytesRead = 0;
		while (bytesRead != -1) {
			bytesRead = inputStream.read(buffer, 0, 4096);
			if (bytesRead != -1) {
				byteArrayOutputStream.write(buffer, 0, bytesRead);
			}
		}
		byte[] ungzipped = byteArrayOutputStream.toByteArray();
		inputStream.close();
		byteArrayOutputStream.close();
		return ungzipped;
	}

	public byte[] gzip(byte[] ungzipped) throws IOException {
		final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bytes);
		gzipOutputStream.write(ungzipped);
		gzipOutputStream.close();
		return bytes.toByteArray();
	}
}
