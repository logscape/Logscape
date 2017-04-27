package com.liquidlabs.common.file.raf;

import com.liquidlabs.common.Pool;
import com.liquidlabs.common.StringUtil;
import org.apache.log4j.Logger;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 14/02/2013
 * Time: 10:50
 * To change this template use File | Settings | File Templates.
 */
public abstract class RafBase {
    //TODO sort out what out EOLine character sequence is....
    private final static Logger LOGGER = Logger.getLogger(RafBase.class);


    protected final String filename;

    protected long lastSeek = -1000;
    protected ByteBuffer bb;
    protected StringBuilder sb = new StringBuilder("0");
    static boolean poolEnabled = Integer.getInteger("raf.bb.pool.max",1000) != -1;
    static Pool pool = new Pool(50, Integer.getInteger("raf.bb.pool.max",1000), new Pool.Factory() {
        public Object newInstance() {
            if (RAF.isDirect) {
                return ByteBuffer.allocateDirect(RAF.BUFFER_LENGTH);
            } else {
                return ByteBuffer.wrap(new byte[RAF.BUFFER_LENGTH]);
            }
        }

    });

    int linesRead;
    private String breakRule;
    private boolean usingPool = false;

    public RafBase(String filename) {
        this.filename = filename;
        configureByteBuffer(true);

    }
    public RafBase(String filename, boolean isDirect) {
        this.filename = filename;
        configureByteBuffer(isDirect);
    }

    private void configureByteBuffer(boolean isDirect) {
        if (!isDirect) {
            usingPool = false;
            bb = ByteBuffer.wrap(new byte[RAF.BUFFER_LENGTH]);
            return;
        }
        if (poolEnabled) {
            usingPool = true;
            bb = (ByteBuffer) pool.fetchFromPool();
        } else {
            usingPool = false;
            if (isDirect) {
                bb = ByteBuffer.allocateDirect(RAF.BUFFER_LENGTH);
            } else {
                bb = ByteBuffer.wrap(new byte[RAF.BUFFER_LENGTH]);
            }
        }
    }


    public String getFilename() {
        return this.filename;
    }

    protected RafSweeper sweeper = new RafSweeper();

    public String readLineSingle(int minLineLength) throws IOException {
        int sweepable = sweeper.isSweepable(bb, (short) minLineLength);

//        if (sweepable == -1) {
//            long lastSeekMaybe = lastSeek + bb.position();
//            int bytes = readMore();
//            if (bytes > 1) {
//                sweepable = sweeper.isSweepable(bb,(short) 0);
//            } else if (bytes == -1) {
//                lastSeek = lastSeekMaybe;
//                return null;
//            }
//        }

        if (sweepable != -1) {
            return readInBulk(sweepable);
        }
        else {
            return readByChar();
        }
    }

    public String readLine() throws IOException {

        int sweepable = sweeper.isSweepable(bb, (short) 0);
        if (sweepable != -1) {
            return readInBulk(sweepable);
        }
        else {
            return readByChar();
        }
    }

    public int getNewLineLength() {
        return sweeper.getSkipAmount();
    }
    private String readInBulk(int sweepable) {

        linesRead = 1;
        int pos = bb.position();
        int skip = sweeper.getSkipAmount();
        int limit = bb.limit();
        int length = sweepable - pos;
        if (length < 0) {
            return null;
        }

        try {
            String result = bb.isDirect() ? readISO(bb, pos, length) : readISO(bb.array(), pos, length) ;
            bb.position(skip + pos + length);
            return result;

        } catch (Exception e) {
            e.printStackTrace();;
            throw new RuntimeException("ReadInBulkFailure: limit:" + limit + " pos:" + pos + " len:" + length + " aa:" + bb.remaining() + " skip:" + skip + " E:" + e.toString(),e);
        }
    }

    boolean isMLang = System.getProperty("mlang", "true").equals("true");
    public String readISO(ByteBuffer bytes, int from, int length) {
        if (isMLang) {
            return readBytes(bytes, from, length);
        }


        char[] charBuffer = new char[length];
        int bpos = 0;
        for(int i = from; i < from + length; i++) {
            charBuffer[bpos++] = (char) bytes.get();
        }
        return StringUtil.wrapCharArray(charBuffer);

    }

    private String readBytes(ByteBuffer bytes, int from, int length) {
        byte[] bb = new byte[length];
        int bpos = 0;
        for(int i = from; i < from + length; i++) {
            bb[bpos++] = bytes.get();
        }
        return new String(bb);
    }

    public String readISO(byte[] bytes, int from, int length) {
        if (isMLang) {
            byte[] bb = new byte[length];
            int bpos = 0;
            for(int i = from; i < from + length; i++) {
                bb[bpos++] = (bytes[i]);
            }
            return new String(bb);


        }
        char[] charBuffer = new char[length];
        int bpos = 0;
        for(int i = from; i < from + length; i++) {
            charBuffer[bpos++] = (char)(bytes[i]);
        }
        return StringUtil.wrapCharArray(charBuffer);
    }

    private String readByChar() throws IOException {
        sb.delete(0, sb.length());
        sb.ensureCapacity(128);
        int c = RAF.EOF;
        this.linesRead = 0;
        boolean eol = false;
        while (!eol) {
            c = read();
            if (c == RAF.EOL_N || c == RAF.EOF || sb.length() > RAF.maxLineLength) {
                eol = true;
                linesRead++;
            } else if (c == RAF.EOL_R) {
                eol = true;
                linesRead++;
                // move to the next cursor position only if it is a EOLN
                byte[] peek = peek(0, 1);
                if (peek.length > 0 && peek[0] == RAF.EOL_N) read();

            } else {
                sb.append((char) c);
            }
        }
        if ((c == RAF.EOF) && (sb.length() == 0)) {
            return null;
        }
        return sb.toString();
    }
    public String readLine(int length) throws IOException {
        return readLine();
    }

    long lastFilePos = -1;
    public void seek(long pos) throws IOException {
        long from = getBufferStartPos();
        long to = getBufferMaxPos();
        long delta = pos - from;
        // we are within this buffers window...then move the buffer position
        if (lastSeek > -1 && delta > 0 && pos >= from && pos <= to) {
            bb.position((int) (bb.position()+ delta));
            return;
        }

        // noop - cursor is at this position
        if (pos == from && to > from) {
            return;
        }
        // if we are sliding back but within the same buffer - then repsotion the pointer
        if (delta < 0 && bb.position() + delta > 0) {
            // reposition the buffer
            bb.position((int) (bb.position() + delta));
            //System.err.println("Setting POS:" + pos + " Delta:" + delta);
            lastFilePos = pos;
            return;
        }
        if (pos < getFilePointerRAF()) {
            //if (LOGGER.isDebugEnabled()) LOGGER.debug(this.filename + " DD Seek:" + pos + " lastPos:" + lastFilePos + " FilePtr:" + getFilePointer());
            if (LOGGER.isDebugEnabled()) LOGGER.debug(this.filename + " >>>>>>>> Possible:OutOfSequence Data - ReverseSeek" + pos + " lastPos:" + lastFilePos + " File:" + getFilePointerRAF());
            reset();
        }

        if (lastFilePos != pos && lastFilePos+1 != pos && pos != 0){// && lastFilePos+1 == pos) {
            //System.err.println("RafBase SKIP:" + pos + " lastPOs:" + lastFilePos + " Delta:" + (pos - lastFilePos) + " File:" + this.filename);
            skip(pos);
        }
        lastSeek = pos;
        readNext(pos);
    }

    private void readNext(long pos) throws IOException {
        bb.clear();
        int limit = readChunk();
        lastFilePos = pos + limit;
        bb.position(0);
        if (limit >= 0) bb.limit(limit);
        else bb.limit(0);
    }

    private long getBufferStartPos() {
        return lastSeek + bb.position();
    }

    private long getBufferMaxPos() {
        return lastSeek + bb.limit();
    }

    abstract void skip(long pos) throws IOException;


    abstract int readChunk() throws IOException;
    protected int readMore() throws IOException {
        return -1;
    }


    public void setBreakRule(String breakRule) {
        this.breakRule = breakRule;
    }
    public int linesRead() {
        return linesRead;
    }
    public long length() throws IOException {
        // this aint gonna work - it needs to decompressed file length!
        return new File(this.filename).length();
    }
    public byte read() throws IOException {
        if (bb.remaining() == 0) {
            // seek to the end of the last position
            seek(getFilePointerRAF());
            if (bb.remaining() == 0) {
                return RAF.EOF;
            }
        }
        return bb.get();
    }
    public long getFilePointer() throws IOException {
        return lastSeek + bb.position();
    }

    final public long getFilePointerRAF() throws IOException {
        return lastSeek + bb.position();
    }
    public boolean wasEOLFound() {
        return true;
    }
    final public byte[] peek(int offset, int length) {
        /**
         * note - this is not implemented properly when we hit buffer end - its going to say - break!
         */
        if (bb.remaining() - offset < length) {
            return new byte[0];
        }
        try {
            int position = bb.position() + offset;
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = bb.get(position + i);
            }
            return bytes;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public void close() throws IOException {
        if (!usingPool) {
            if (bb.isDirect()) {
                Cleaner cleaner = ((DirectBuffer) bb).cleaner();
                if (cleaner != null) cleaner.clean();
            }
        } else {
            bb.clear();
            pool.putInPool(bb);
        }
        bb = null;
    }

    public void reset() throws IOException {
        this.lastFilePos = -1000;
        this.lastFilePos = 0;
    }
    public int getMinLineLength() {
        return 0;
    }

    @Override
    protected void finalize() throws Throwable {
        close();
    }
}
