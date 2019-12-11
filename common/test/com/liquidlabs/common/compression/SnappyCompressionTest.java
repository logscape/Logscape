package com.liquidlabs.common.compression;

import com.google.common.io.LineReader;
import com.liquidlabs.common.DateUtil;
import org.joda.time.DateTime;
import org.junit.Test;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 22/10/2013
 * Time: 16:22
 * To change this template use File | Settings | File Templates.
 */
public class SnappyCompressionTest {

    @Test
    public void shouldAppendCompressedData() throws Exception {
        String testFile = "build/test.snap";
        new File(testFile).delete();
        new File("build").mkdir();

        OutputStream sos = new SnappyFramedOutputStream(new FileOutputStream(testFile, true));
        for (int i = 0; i < 10; i++) {
            sos.write(("line:" + i + "\n").getBytes());
        }
        sos.flush();
        System.out.println("len:"+ new File(testFile).length());
        SnappyFramedOutputStream sos2 = new SnappyFramedOutputStream(new FileOutputStream(testFile, true));
        for (int i = 10; i < 20; i++) {
            sos2.write(("line:" + i + "\n").getBytes());
        }
        sos2.close();
        System.out.println("len2:"+ new File(testFile).length());

        InputStream sis = new SnappyFramedInputStream(new FileInputStream(testFile));

        byte[] array = new byte[1024];
        int read = sis.read(array);
        System.out.println("GOT:\n" + new String(array));

        byte[] array2 = new byte[1024];
        int read2 = sis.read(array2);
        System.out.println("GOT:\n" + new String(array2));

    }


}
