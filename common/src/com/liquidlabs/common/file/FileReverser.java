package com.liquidlabs.common.file;

import java.io.*;

public class FileReverser {
    private byte[] bytebuffer;
    private int maxRead = 32 * 1024;
    private java.io.RandomAccessFile raf;

    public FileReverser(File file) throws FileNotFoundException {
        raf = new java.io.RandomAccessFile(file, "r");
    }

    public void reverseTo(File outFile) throws IOException {
        PrintWriter printWriter = new PrintWriter(outFile);

        long readEnd = raf.length();

        byte[] remainder = new byte[0];
        do {
            int readSize = readEnd < maxRead ? (int) readEnd : maxRead;
            bytebuffer = new byte[readSize + remainder.length];
            raf.seek(readEnd - readSize);
            long pos = raf.getFilePointer();
            read(remainder, readSize);
            remainder = reverse(printWriter);
            readEnd = pos;
        } while (readEnd > 0);
        if (remainder.length > 0) {
            printWriter.print(new String(remainder));
        }
        raf.close();
        printWriter.close();
    }

    private void read(byte[] remainder, int readSize) throws IOException {
        raf.read(bytebuffer);
        if (remainder.length > 0) {
            System.arraycopy(remainder, 0, bytebuffer, readSize, remainder.length);
        }
    }

    private byte[] reverse(PrintWriter printWriter) {
        String [] lines = new String(bytebuffer).split("\n");
        for (int i = lines.length - 1; i > 0; i--) {
            printWriter.println(lines[i]);
        }
        return leftOverBytes(lines[0]);
    }

    private byte[] leftOverBytes(String line) {
        byte[] lineBytes = line.getBytes();
        byte[] leftOver = new byte[lineBytes.length + 1];
        System.arraycopy(lineBytes, 0, leftOver, 0, lineBytes.length);
        leftOver[leftOver.length - 1] = '\n';
        return leftOver;
    }
}
