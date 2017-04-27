/**
 *
 */
package com.liquidlabs.common.file.raf;

import java.io.IOException;

public class LineReader {
    /**
     *
     */
    private RAF raf;

    /**
     * @param
     */
    LineReader(RAF raf) {
        this.raf = raf;
    }

    int length = 0;
    boolean wasNewLineRead = false;
    private BreakRule keepReadingRule = new DefaultKeepReadingRule();
    private boolean alwaysReadFullEvents;
    StringBuilder sb = new StringBuilder("0");
    int linesRead;

    public int linesRead() {
        return linesRead;
    }
    public String readLine(int minLength) throws IOException {
        if (keepReadingRule.isLineBreakable()) {
            return readLineUsingBreaker((short) minLength);
        } else if (keepReadingRule.isExplicitRule()) {
            return doExplicitNew();
        } else {
            return readLineOld();
        }
    }
    public String readLine() throws IOException {

        if (keepReadingRule.isLineBreakable()) {
            return readLineUsingBreaker((short) 0);
        } else if (keepReadingRule.isExplicitRule()) {
            return doExplicitNew();
        } else {
            return readLineOld();
        }
    }
    protected String readLineOld() throws IOException {

        sb.delete(0, sb.length());
        sb.ensureCapacity(128);
        byte c = MLineByteBufferRAF.EOF;
        byte prevC = MLineByteBufferRAF.EOF;
        boolean eol = false;
        int linesRead = 0;
        length = 0;
        final long startPos = raf.getFilePointerRAF();

        while (!eol) {
            c = raf.read();
            if (c == MLineByteBufferRAF.EOF) {
                // reposition to where we started
                eol = true;

                // explicit was specified so we need to read until we hit the next explicit tag (i.e. not EOLN- but **Alert or something)
                if (alwaysReadFullEvents) {
                    // REWIND
                    raf.seek(startPos);
                    this.linesRead = 0;
                    lastRafPos = startPos;
                    return null;
                }
            } else {
                if (c == MLineByteBufferRAF.EOL_N) {
                    String peek = peekAhead(0);
                    linesRead++;
                    if (!keepReadingRule.isKeepReading(peek) || linesRead > RAF.MAX_LINES_PERLINE || sb.length() > RAF.maxTotalLength){
                        eol = true;
                    } else {
                        sb.append((char) c);
                    }
                } else if (c == MLineByteBufferRAF.EOL_R) {
                    linesRead++;
                    String peek = peekAhead(1);
                    if (!keepReadingRule.isKeepReading(peek) || linesRead > RAF.MAX_LINES_PERLINE || sb.length() > RAF.maxTotalLength){
                        eol = true;
                        long cur = raf.getFilePointerRAF();
                        if ((raf.read()) != MLineByteBufferRAF.EOL_N) {
                            raf.seek(cur);
                        }
                    } else {
                        long cur = raf.getFilePointerRAF();
                        if ((raf.read()) != MLineByteBufferRAF.EOL_N) {
                            raf.seek(cur);
                        }
                        sb.append((char) c);
                    }

                } else {
                    // inline instead of super.append(c) for performance reasons
                    sb.append((char) c);
                    // protect against massive log lines that can cause OOM
                    // Explicit was specified so we need to peek at everything
                    if (alwaysReadFullEvents) {
                        String peek = peekAhead(0);
                        if (!keepReadingRule.isKeepReading(peek)) {
                            // move the file pointer on the length of the splitter
                            for (int m = 0; m < keepReadingRule.getCharsLength(); m++) {
                                c = raf.read();
                            }
                            eol = true;
                            linesRead++;
                        }
                    }
                    if (length++ > MLineByteBufferRAF.maxTotalLength){
                        eol = true;
                    }
                }
            }
            prevC = c;
        }
        lastRafPos = raf.getFilePointerRAF();
        wasNewLineRead = wasNewLineRead || prevC == MLineByteBufferRAF.EOL_R || prevC == MLineByteBufferRAF.EOL_N;
        this.linesRead  = linesRead;
        if (c == MLineByteBufferRAF.EOF && sb.length() == 0) {
            return null;
        }
        return sb.toString();
    }
    public int getMinLength() {
        return this.minLengthFound;
    }

    private String readLineUsingBreaker(short minLength) throws IOException {
        return readLineUsingBreaker2(minLength);
    }
    private String readLineUsingBreaker2(short minLength) throws IOException {
        this.linesRead = 0;
        try {

            // if there was a seek then ditch the previously cached line
            if (readAheadLine != null && raf.getFilePointerRAF() - (readAheadLine.length() + newLineLength) != lastRafPos) {
                readAheadLine = null;
                hitEOF = false;
            }

            /**
             * Handle EOF From previous read
             */
            if (hitEOF) {
                return null;
            }

            /**
             * Pick up previous readLine or read a new One
             */
            String currentLine = readAheadLine;
            if (currentLine != null) {
                readAheadLine = null;
                newLineLength = raf.getNewLineLength();
            } else {
                currentLine = raf.readLineSingle(minLength);
            }

            String nextLine = raf.readLineSingle(minLength);

            if (nextLine == null) {
                eofRafPos = raf.getFilePointerRAF();
                eofPos = eofRafPos;
                hitEOF = true;
                this.linesRead = 1;
                return currentLine;
            }

            if (!keepReadingRule.isKeepReading(nextLine)) {
                readAheadLine = nextLine;
                this.linesRead = 1;
                return currentLine;
            }

            /**
             * More than 1 line to collect so accumulate it to the SB
             */
            sb.delete(0, sb.length());
            this.linesRead = 1;
            sb.append(currentLine);

            // READ-Remainder until we hit the end OR run to the line size limit
            while (nextLine != null && keepReadingRule.isKeepReading(nextLine) && sb.length() < RAF.maxTotalLength) {
                sb.append("\n");
                sb.append(nextLine);
                if (nextLine.length() < minLengthFound) minLengthFound = nextLine.length();
                this.linesRead++;
                nextLine = raf.readLineSingle(minLength);

            }
            // hit EOF
            if (nextLine == null) {
                //eofPos = new java.io.File(raf.getFilename()).length();
                eofRafPos = raf.getFilePointerRAF();
                eofPos = eofRafPos;
                hitEOF = true;
                return sb.toString();
            } else {
                readAheadLine = nextLine;
            }
            return sb.toString();
        } finally {
            lastRafPos = raf.getFilePointerRAF();
            if (readAheadLine != null) {
                lastRafPos -= (readAheadLine.length() + newLineLength);
            }
        }
    }

    /**
     * State stores for the call below
     */
    boolean hitEOF = false;
    long eofPos = 0;
    long eofRafPos = 0;
    long lastRafPos = 0;
    String readAheadLine = null;
    int minLengthFound = Integer.MAX_VALUE;
    int newLineLength;

    private String readLineUsingBreaker1(short minLength) throws IOException {
        this.linesRead = 0;
        try {

            // if there was a seek then ditch the previously cached line
            if (readAheadLine != null && raf.getFilePointerRAF() - (readAheadLine.length() + newLineLength) != lastRafPos) {
                readAheadLine = null;
                hitEOF = false;
            }

            /**
             * Handle EOF From previous read
             */
            if (hitEOF) {
                if (readAheadLine != null) {
                    String sss = readAheadLine;
                    readAheadLine = null;
                    return sss;
                }
                if (eofRafPos != raf.getFilePointerRAF()) {
                    eofPos = 0;
                    hitEOF = false;
                } else {
                    // do this to make sure we read the last file line
                    // see if the file changed
                    long nowPos = new java.io.File(raf.getFilename()).length();
                    if (nowPos == eofPos) return null;
                    else hitEOF = false;
                }
            }

            /**
             * Pick up previous readLine or read a new One
             */

            String currentLine = readAheadLine;
            if (currentLine != null) {
                newLineLength = raf.getNewLineLength();
                readAheadLine = null;
            } else {
                currentLine = raf.readLineSingle(minLength);
                newLineLength = raf.getNewLineLength();
            }
            // hit EOF
            if (currentLine == null) {
                eofRafPos = raf.getFilePointerRAF();
                eofPos = eofRafPos;
                hitEOF = true;
                this.linesRead = 0;
                return null;
            }

            if (currentLine.length() < minLengthFound) minLengthFound = currentLine.length();

            /**
             * Read the next Line
             */

            String nextLine = raf.readLineSingle(minLength);

            if (nextLine == null) {
                eofRafPos = raf.getFilePointerRAF();
                eofPos = eofRafPos;
                hitEOF = true;
                this.linesRead = 1;
                return currentLine;
            }
            if (nextLine.length() < minLengthFound) minLengthFound = nextLine.length();

            if (!keepReadingRule.isKeepReading(nextLine)) {
                readAheadLine = nextLine;
                this.linesRead = 1;
                return currentLine;
            }

            /**
             * More than 1 line to collect so accumulate it to the SB
             */
            sb.delete(0, sb.length());
            this.linesRead = 1;
            sb.append(currentLine);

            // READ-Remainder until we hit the end OR run to the line size limit
            while (nextLine != null && keepReadingRule.isKeepReading(nextLine) && sb.length() < RAF.maxTotalLength) {
                sb.append("\n");
                sb.append(nextLine);
                if (nextLine.length() < minLengthFound) minLengthFound = nextLine.length();
                this.linesRead++;
                nextLine = raf.readLineSingle(minLength);

            }
            // hit EOF
            if (nextLine == null) {
                //eofPos = new java.io.File(raf.getFilename()).length();
                eofRafPos = raf.getFilePointerRAF();
                eofPos = eofRafPos;
                hitEOF = true;
                return sb.toString();
            } else {
                readAheadLine = nextLine;
            }
            return sb.toString();
        } finally {
            lastRafPos = raf.getFilePointerRAF();
            if (readAheadLine != null) {
                lastRafPos -= (readAheadLine.length() + newLineLength);
//                System.err.println("Rewind:" + readAheadLine.length());
            }
        }
    }

    String explicitReadAhead = null;
    private String doExplicitNew() throws IOException {
        int linesRead = 0;
        sb.delete(0, sb.length());
        if (explicitReadAhead != null) {
            sb.append(explicitReadAhead);
            linesRead++;
        }
        explicitReadAhead = null;
        String nextLine = "";
        boolean finished = false;
        String breakChars = keepReadingRule.explicitBreak();
        while (!finished && nextLine != null ) {
            nextLine = raf.readLineSingle(0);
         //   System.out.println("GOT:" + nextLine);
            if (nextLine != null) {
                if (sb.length() == 0) {
                    sb.append(nextLine);
                    linesRead++;
                    nextLine = "";
                } else if (nextLine.endsWith(breakChars)) {
                    sb.append("\n").append(nextLine);
                    finished = true;
                    linesRead++;
                } else if (!nextLine.startsWith(breakChars)) {
                    sb.append("\n").append(nextLine);
                    linesRead++;

                } else {
                    explicitReadAhead = nextLine;
                    finished = true;
                }
                // dont blow up on massive data
                if (sb.length() > RAF.maxTotalLength) finished = true;
            }
        }
        this.linesRead  = linesRead;
        if (linesRead == 0) return null;
        return sb.toString();
    }

    // SLOW - but need to peek every char
    private String doExplicitRead() throws IOException {
        byte c = MLineByteBufferRAF.EOF;
        byte prevC = MLineByteBufferRAF.EOF;
        boolean eol = false;
        int linesRead = 0;
        length = 0;
        final long startPos = raf.getFilePointerRAF();

        while (!eol) {
            c = raf.read();
            if (c == MLineByteBufferRAF.EOF) {
                // reposition to where we started
                // if (linesRead == 0) linesRead++;
                eol = true;
            } else {

                String peek = peekAhead(0);

                if (!keepReadingRule.isKeepReading(peek) || linesRead > RAF.MAX_LINES_PERLINE || sb.length() > RAF.maxTotalLength){
                    sb.append((char) c);
                    // throw away the SPLIT chars
                    for (int i = 0; i < peek.length(); i++) {
                        byte cc = raf.read();
                        if (cc == RAF.EOL_N || c ==RAF.EOL_R) {
                            linesRead++;
                        }
                    }

                    eol = true;
                } else {
                    sb.append((char) c);
                }
                if (length++ > MLineByteBufferRAF.maxTotalLength){
                    eol = true;
                }

                prevC = c;
            }
        }

        // explicit was specified so we need to read until we hit the next explicit tag (i.e. not EOLN- but **Alert or something)
//		if (alwaysReadFullEvents) {
//				// REWIND
//				raf.seek(startPos);
//				this.linesRead = 0;
//				return null;
//		}
        lastRafPos = raf.getFilePointerRAF();
        wasNewLineRead = wasNewLineRead || prevC == RAF.EOL_R || prevC == RAF.EOL_N;
        this.linesRead  = linesRead;
        if (c == MLineByteBufferRAF.EOF && sb.length() == 0) {
            return null;
        }
        return sb.toString();
    }
    private String peekAhead(int offset) throws IOException {
        int breakLength = keepReadingRule.breakLength();
        return new String(raf.peek(offset, breakLength));
    }

    public boolean wasEOLFound() {
        return wasNewLineRead;
    }
    public void setKeepReadingRule(BreakRule rule) {
        this.keepReadingRule = rule;

    }
    public void setBreakRule(String breakRule) {
        setKeepReadingRule(DefaultKeepReadingRule.getRule(breakRule));
        if (breakRule != null && breakRule.contains("Explicit")) {
            alwaysReadFullEvents = true;
        }

    }
}