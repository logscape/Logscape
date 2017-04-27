package com.liquidlabs.log.server;

import com.liquidlabs.common.Pair;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

public class ChunkOfFileHandler {

    private Writer outputStream;
    private final LogLineOutput writer;
    private final File file;
    private final int pageSize;

    public ChunkOfFileHandler(Writer outputStream, LogLineOutput lineWriter, File file, int pageSize) {
        this.outputStream = outputStream;
        this.writer = lineWriter;
        this.file = file;
        this.pageSize = pageSize;
    }

    public Pair<Long,Long> nextChunk(String theTemplate, long nextLineNo, long seekPos, long from) throws IOException {
        final RAF raf = RafFactory.getRafSingleLine(file.getAbsolutePath());
        try {
            if(raf.length() < seekPos && !FileUtil.isCompressedFile(file.getName())) {
                return new Pair<Long, Long>(-1L, -1L);
            }
            raf.seek(seekPos);
            long read = 0;
            long written = 0;
            String line;
            boolean templateWritten = false;

//            if (from > nextLineNo) {
//                from = nextLineNo;
//            }
            boolean writtenLine = false;


            while(written < pageSize && ((line = raf.readLine()) != null)) {
                long currentLine = nextLineNo + read;
                if (currentLine >= Math.max(from,1)) {
                    if (!writtenLine) {
                        writer.writeHtmlLine(nextLineNo, "line:" + from, 0);
                        writtenLine = true;
                    }
                    if (!templateWritten && theTemplate.length() > 0) {
                        templateWritten = true;
                        writeTemplate(writer, theTemplate, line, currentLine);
                    }
                    writer.writeHtmlLine(from, line, currentLine);
                    written++;
                }
                read++;
            }
            // passed the end of file
            if (!writtenLine) {
                writer.writeHtmlLine(nextLineNo, "line:" + nextLineNo, 0);
            }
                return new Pair<Long, Long>(nextLineNo + read, raf.getFilePointer());
        } finally {
            if (raf != null) raf.close();
        }



    }

    private void writeTemplate(LogLineOutput writer, String theTemplate, String line, long lineNumber) {
        String mode = "less";
        if (line.startsWith("{") || line.startsWith("[")) {
            mode = "json";
        }
        try {
            theTemplate = theTemplate.replace("MODE", mode);
            theTemplate = theTemplate.replace("FROM", Long.valueOf(lineNumber).toString());
            outputStream.write(theTemplate);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
