package com.liquidlabs.common.file.raf;

import org.apache.tools.bzip2.CBZip2InputStream;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class BzipRaf extends RafBase implements RAF {

    private InputStream is;

    public BzipRaf(String filename) throws IOException {
        super(filename, false);
        this.is = getStream();
        seek(0);
	}

    private InputStream getStream() throws IOException {
        InputStream fis = new BufferedInputStream(new FileInputStream(this.filename));

        // read the 2 file header bytes
        fis.read();
        fis.read();
        return new CBZip2InputStream(fis);
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
        if (is != null) is.close();;
        this.is = getStream();
        this.is.read();
        super.reset();
    }


}
