package com.liquidlabs.common.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;

import com.liquidlabs.common.file.FileReverser;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: damian
 * Date: Oct 11, 2009
 * Time: 1:40:43 PM
 * To change this template use File | Settings | File Templates.
 */

public class FileReverserTest {

    @Test
    public void shouldWorkWhenOnlyOneLine() throws IOException {
        File file = File.createTempFile("foo", "moo");
        FileOutputStream out = new FileOutputStream(file);
        out.write("1 - This is line number 1 of 1".getBytes());
        out.close();
        File outFile = File.createTempFile("out", "poo");
        new FileReverser(file).reverseTo(outFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(outFile)));
        String s = reader.readLine();
        assertEquals("1 - This is line number 1 of 1", s);
        assertNull(reader.readLine());


    }

    @Test
    public void shouldReverseLinesInAFile() throws Exception {
        File file = File.createTempFile("foo", "moo");
        FileOutputStream out = new FileOutputStream(file);
        for (int i = 1000; i > 0; i--) {
            out.write(String.format("%d - This is line number %d\n", i, i).getBytes());
        }
        out.flush();
        out.close();
        File outFile = File.createTempFile("out", "poo");
        new FileReverser(file).reverseTo(outFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(outFile)));
        for (int i = 1; i <= 1000; i++) {
            String line = reader.readLine();
            assertEquals(String.format("%d - This is line number %d", i, i), line);
        }
    }


    @Test
    public void shouldReverseLinesInABiggerFile() throws Exception {
        File file = File.createTempFile("bigger", "moo");
        FileOutputStream out = new FileOutputStream(file);
        int size = 10 * 1024;
        for (int i = size; i > 0; i--) {
            out.write(String.format("%d - This is line number %d of the even bigger file than before. God knows how big it is but it must be massive\n", i, i).getBytes());
        }
        out.flush();
        out.close();
        File outFile = File.createTempFile("out", "big");
        long start = System.currentTimeMillis();
        new FileReverser(file).reverseTo(outFile);
        long stop = System.currentTimeMillis();
        System.out.printf("Outfile size = %d megabytes took %d seconds to reverse\n", file.length()/1024/1024, (stop - start)/1000);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(outFile)));
        for (int i = 1; i <= size; i++) {
            String line = reader.readLine();
            assertEquals(String.format("%d - This is line number %d of the even bigger file than before. God knows how big it is but it must be massive", i, i), line);
        }
    }


}
