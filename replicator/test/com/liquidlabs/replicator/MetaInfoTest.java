package com.liquidlabs.replicator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import com.liquidlabs.replicator.data.MetaInfo;
import com.liquidlabs.replicator.data.Piece;

import junit.framework.TestCase;

public class MetaInfoTest extends TestCase {
	
	private File file;

	@Override
	protected void setUp() throws Exception {
		file = File.createTempFile("abc", "123");
		file.deleteOnExit();
	}
	
	public void testShouldBreakUpIntoMultiplePiecesBasedOnChunkSize() throws Exception {
		writeBytes(1024);
		MetaInfo metaInfo = new MetaInfo(file, 512);
		Piece[] pieces = metaInfo.pieces();
		assertEquals(2, pieces.length);
	}
	
	public void testShouldGenerateOnePieceWhenChunkSizeEqualFileSize() throws Exception {
		writeBytes(512);
		MetaInfo metaInfo = new MetaInfo(file, 512);
		Piece[] pieces = metaInfo.pieces();
		assertEquals(1, pieces.length);
	}
	
	public void testShouldGenerateExtraPieceWhenRemainderLessThanChunkSize() throws Exception {
		writeBytes(1086);
		MetaInfo metaInfo = new MetaInfo(file, 512);
		Piece[] pieces = metaInfo.pieces();
		assertEquals(3, pieces.length);
	}
	
	
	public void testShouldGenerateOnePieceWhenFileSizeLessThanChunkSize() throws Exception {
		writeBytes(100);
		MetaInfo metaInfo = new MetaInfo(file, 512);
		Piece[] pieces = metaInfo.pieces();
		assertEquals(1, pieces.length);
	}
	
	public void testPieceShouldContainStartPositionAndLength() throws Exception {
		writeBytes(512);
		MetaInfo metaInfo = new MetaInfo(file, 512);
		Piece[] pieces = metaInfo.pieces();
		assertEquals(0, pieces[0].start());
		assertEquals(512 * 1024, pieces[0].length());
	}
	
	public void testPiecesShouldContainCorrectStartPositionAndLength() throws Exception {
		writeBytes(1086);
		MetaInfo metaInfo = new MetaInfo(file, 512);
		Piece[] pieces = metaInfo.pieces();
		assertEquals(0, pieces[0].start());
		assertEquals(512 * 1024, pieces[0].length());

		assertEquals(512 * 1024, pieces[1].start());
		assertEquals(512 * 1024, pieces[1].length());
		
		assertEquals(1024 * 1024, pieces[2].start());
		assertEquals((1086 - 1024) * 1024, pieces[2].length());
	}
	
	

	private void writeBytes(int kbytes) throws FileNotFoundException,
			IOException {
		FileOutputStream stream = new FileOutputStream(file);
		byte [] buf = new byte[kbytes * 1024];
		Arrays.fill(buf, (byte)1);
		stream.write(buf);
		stream.close();
	}

}
