package com.liquidlabs.common.concurrent;

import com.liquidlabs.common.compression.CompressorConfig;
import junit.framework.TestCase;
import static org.junit.Assert.*;

import org.junit.Test;

public class CompressTest  {

	Compress compress = new Compress("_GZIPPED_");


    @Test
    public void testShouldCompressLZ4Stuff() throws Exception {

        byte[] compressedWithSrclength = CompressorConfig.compress("yay1".getBytes());
        String decompress = new String(CompressorConfig.decompress(compressedWithSrclength));
        assertEquals("yay1",decompress);
    }


        @Test
	public void testShouldCompressStuff() throws Exception {

		String stringToCheck = "Stufffffffffffffffffffffffff";
		byte[] gzip = compress.gzip(stringToCheck.getBytes());

		byte[] ungzip1 = compress.ungzip(gzip);
		assertEquals(stringToCheck, new String(ungzip1));
	}

    @Test
	public void testShouldMakeBigPayload() throws Exception {
		String bigBytes = "SomeBigBytes";
		byte[] result = compress.getGZipBytesWithHeader(bigBytes.getBytes());
		assertTrue(result.length > 5);
	}

    @Test
	public void testShouldRecognizeCompressedData() throws Exception {
		boolean isCompressed = compress.isCompressed("_GZIPPED_".getBytes());
		assertTrue(isCompressed);
	}

    @Test
	public void testShouldDecompressStuff() throws Exception {

		String bigBytes = "SomeBigBytes";
		byte[] sresult = compress.getGZipBytesWithHeader(bigBytes.getBytes());
		byte[] result = compress.getUnGzipBytesWithHeader(sresult);
		assertEquals("SomeBigBytes", new String(result));
	}
}
