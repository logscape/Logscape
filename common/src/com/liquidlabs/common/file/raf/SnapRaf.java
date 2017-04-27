package com.liquidlabs.common.file.raf;

import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.*;

public class SnapRaf extends RafBase implements RAF {

    InputStream is = null;

	public SnapRaf(String filename) throws IOException {
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

    protected void skip(long pos) throws IOException {
        is.skip(pos - lastFilePos);
    }
    protected int readChunk() throws IOException {
		return  is.read(bb.array());
	}
    public void reset() throws IOException {
        if (this.is != null) this.is.close();
        this.is = getInputStream();
        super.reset();
    }

    private InputStream getInputStream() throws IOException {
        return new SnappyFramedInputStream(new BufferedInputStream(new FileInputStream(super.filename), 32 * 1024));
    }
    public static void compress(String infile) throws Exception {
        OutputStream fos = new SnappyFramedOutputStream(new FileOutputStream(infile + ".snap"));
        ByteBufferRAF raf = new ByteBufferRAF(infile);
        String line = "";
        while ((line = raf.readLine()) != null) {
            fos.write((line + "\n").getBytes());
        }
        fos.close();

    }

}
