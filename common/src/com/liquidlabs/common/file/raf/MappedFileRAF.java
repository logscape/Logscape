package com.liquidlabs.common.file.raf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * Map the file into the hosts system memory (windows will keep a file lock)
 * Benchmarking shows that performance drops off after a while (on OSX)
 *
 */

public class MappedFileRAF extends RafBase implements RAF {

	private FileChannel ch;

	private long lastSeek;
    private final MappedByteBuffer mb;

    public MappedFileRAF(String file) throws IOException {
        super(file);
        FileInputStream f = new FileInputStream( file );
        ch = f.getChannel( );
        mb = ch.map( FileChannel.MapMode.READ_ONLY, 0L, ch.size() );
		seek(0);
	}

	public void close() throws IOException {
		ch.close();
        super.close();
	}

	long lastRequest = -1;
	long lastLength = -1;
	/**
	 * Cache the file.length() for the last seconds worth - file.length is a resource hog
	 */
	public long length() throws IOException {
		long now = System.currentTimeMillis();
		if (now - lastRequest < 500) {
			return lastLength;
		} else {
			lastRequest = now;
			lastLength = ch.size();
			return lastLength;
		}
	}

    public void reset() throws IOException {
        mb.position(0);
        super.reset();
    }
    protected void skip(long pos) throws IOException {
        mb.position((int)pos);
    }

    protected int readChunk() throws IOException {
        byte[] array = bb.array();
        int nGet = Math.min( mb.remaining( ), array.length );
        mb.get( array, 0, nGet );
        return nGet;
    }

}
