package com.liquidlabs.log.server;

import org.apache.commons.lang3.StringEscapeUtils;

import java.io.IOException;
import java.io.Writer;

public class LogLineOutput {
    private Writer writer;

    public LogLineOutput(Writer writer) {
        this.writer = writer;
    }

    public void writeHtmlLine(long fromLine, String line, long lineCount) throws IOException {
        if (lineCount == fromLine) {
       //     writer.write(LogHttpServer.spanMark);
        } else {
        //    writer.write(LogHttpServer.spanPre);
        }
       // writer.write("<b class='line_num'>" + lineCount +": </b>");
        writer.write(StringEscapeUtils.escapeHtml4(line));
      //  writer.write(LogHttpServer.spanPost);
        writer.write('\n');
    }

}
