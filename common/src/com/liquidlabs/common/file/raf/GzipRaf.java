package com.liquidlabs.common.file.raf;

import com.liquidlabs.common.compression.GzipCodec;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class GzipRaf extends RafBase implements RAF {

    InputStream is = null;

	public GzipRaf(String filename) throws IOException {
        super(filename, false);
        is = getInputStream();
        seek(0);
    }

	public void close() throws IOException {
		if (is != null) {
			is.close();
			is = null;
            super.close();
		}
	}

    int streamPos = 0;
    protected void skip(long pos) throws IOException {
        //is = getInputStream();
        long delta = pos - lastFilePos;
//        System.err.println("Delta:" + delta + " SEEK:" + pos + " CPOS:" + getFilePointer() + " lastFile:" + lastFilePos);

        long skip = is.skip(delta-1);
        streamPos += skip;
//        System.out.println("SKIP:" + pos + " LastFilePos:" + lastFilePos + " Delta:" + delta + " Skipped:" + skip + " SPOS:" + streamPos);
    }
    protected int readChunk() throws IOException {
        int read = is.read(bb.array());
        //System.out.println("Read:" + read);
        streamPos += read;
        return read;
	}
    public void reset() throws IOException {
//        System.out.println("RESETTING");
        if (this.is != null) this.is.close();
        this.is = getInputStream();
        super.reset();
        this.is.read();
    }

    private InputStream getInputStream() throws IOException {
        streamPos = 0;
//        return is = new GZIPInputStream(new BufferedInputStream(new FileInputStream(super.filename), 1024 * 1024), 64 * 1024);
//		return	is = new MultiMemberGZIPInputStream(new BufferedInputStream(new FileInputStream(super.filename), 64 * 1024), 64 * 1024);
        return new GzipCodec().createInputStream(new BufferedInputStream(new FileInputStream(super.filename), 128 * 1024));
    }

}
