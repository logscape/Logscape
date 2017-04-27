package com.liquidlabs.common.file.raf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.liquidlabs.common.FastByteArrayOutputStream;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafBase;

/**
 * 
 * These links are very interesting - note mapped-files on windows are stuffed - the map holds a file lock open until system.gc is called
 * 
 * http://nadeausoftware.com/articles/2008/02/java_tip_how_read_files_quickly#Benchmarks
 * http://www.kdgregory.com/index.php?page=java.byteBuffer
 *
 * NOTE: We are not using Direct memory allocation - this means we can look ahead at the buffer and use the memory allocated - only once....
 * doing so allows for bulk copy and we can hit the controller speed limits
 *
 */

public class ByteBufferRAF extends RafBase implements RAF {


	protected final File file;
	private FileChannel ch;
	private FileInputStream fis;

	public ByteBufferRAF(String file) throws IOException {
        super(file);
		this.file = new File(file);
		fis = new FileInputStream( this.file );
		ch = fis.getChannel( );
		seek(0);
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
			lastLength = file.length();
			return lastLength;
		}
	}

    public void close() throws IOException {
        ch.close();
        fis.close();
        super.close();
    }

    public void reset() throws IOException {
        ch.position(0);
        super.reset();
    }
    protected void skip(long pos) throws IOException {
        ch.position(pos);
    }

    protected int readChunk() throws IOException {
        return ch.read( bb );
    }
    @Override
    protected int readMore() throws IOException {
        byte[] array = new  byte[bb.remaining()];
        bb.get(array);
        bb.position(0);
        bb.put(array);
        int read = readChunk();
        bb.position(0);
        return read;
    }

}
