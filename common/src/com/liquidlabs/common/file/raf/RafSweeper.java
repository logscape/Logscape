package com.liquidlabs.common.file.raf;

import java.nio.ByteBuffer;

/**
 *
 * Sweepable means that the array can be scanned for bulk copy
 * User: neil
 * Date: 13/02/2013
 * Time: 16:36
 * To change this template use File | Settings | File Templates.
 */
public class RafSweeper {

    static char[] winSplitZero = new char[] { '\r'};
    static char[] winSplit = new char[] { '\r','\n' };
    static char[] nixSplit = new char[] { '\n' };

    char[] splitter = null;// nixSplit;
    int calls;

    public RafSweeper() {
    }
    public RafSweeper(char[] splitter) {
        this.splitter = splitter;
    }


    public int isSweepable(ByteBuffer bb, short minLineLength) {

        if (splitter == null) {
            getSplitter(bb);
        }

        if (bb.remaining() == 0) return -1;

        return sweepBuffer(bb, splitter, minLineLength);

    }

    private void getSplitter(ByteBuffer bb) { splitter = getSplitterFromHeap(bb);  }

    private char[] getSplitterFromHeap(ByteBuffer bb) {
        boolean finished = false;
        int pos = bb.position();
        try {
            while (!finished && bb.hasRemaining()) {
                char aChar = (char) bb.get();
                if (aChar == winSplit[0]) {
                    // peek at the next one
                    char next = (char) bb.get();
                    if (next == winSplit[1]) return winSplit;
                    else return winSplitZero;
                } else if (aChar == nixSplit[0]) return nixSplit;
            }
            if (!bb.hasRemaining()) return nixSplit;
        } finally {
            bb.position(pos);
        }
        throw new RuntimeException("Failed to discover EOL char");
    }

    private int getEndLimit(ByteBuffer bb) {
        int length = bb.limit() - bb.position();
        if (length > RAF.maxLineLength) return RAF.maxLineLength-1;
        return bb.limit();
    }

    final private int sweepBuffer(ByteBuffer bb, char split[], short minLineLength) {
        int bbStart = bb.position();
        final int bbResetTo = bbStart;
        try {
            if (calls > 10) bbStart += minLineLength;
            int bbEnd = getEndLimit(bb);

            // need to check for windows EOL on the first 5 calls
            for (int i = bbStart; i < bbEnd-1; i++) {
                byte byteValue = bb.get();
                if (byteValue == split[0]) {
                    return i;
                }
            }
            return -1;
        } finally {
            bb.position(bbResetTo);
        }
    }

    final public int getSkipAmount() {
        return splitter.length;
    }
}
