package com.liquidlabs.common.file.raf;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.apache.tools.bzip2.CBZip2InputStream;

import java.io.*;

public class Lz4Raf extends RafBase implements RAF {

    private InputStream is;

    public Lz4Raf(String filename) throws IOException {
        super(filename, false);
        this.is = getStream();
        seek(0);
	}

    private InputStream getStream() throws IOException {
        InputStream fis = new BufferedInputStream(new FileInputStream(this.filename));
        return new LZ4BlockInputStream(fis,LZ4Factory.fastestInstance().fastDecompressor());
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
