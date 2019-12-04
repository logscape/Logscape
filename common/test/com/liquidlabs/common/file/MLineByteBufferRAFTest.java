package com.liquidlabs.common.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.MLineByteBufferRAF;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MLineByteBufferRAFTest {

	RAF raf;

    private PrintWriter printWriter;
    private File file;
    String extra = "\n";

    @Before
    public void setUp() throws Exception {

        file = new File("build","MLineBBRafTest.txt");
        if (file.exists()) file.delete();


        printWriter = new PrintWriter(file);
        printWriter.print("line 0.0a" + extra);
        printWriter.print("line 1.1a" + extra);
        printWriter.print("line 2.0a" + extra);
        printWriter.print("\tline 2.1" + extra);
        printWriter.print("line 3.0a" + extra);
        printWriter.print(" line 3.1" + extra);
        printWriter.close();

        raf = new MLineByteBufferRAF(file.getAbsolutePath());
    }

    @After
    public void tearDown() throws Exception {
        file.delete();
    }


    @Test
    public void shouldReadLines() throws Exception {
        String l = "";
        int i = 0;
        while ((l = raf.readLine()) != null) {
            i++;
            System.out.println("\n-- i:" + i + "line:" + l);
        }
        assertEquals(4, i);

    }


    @Test
    public void shouldCountLines() throws Exception {
        String l = "";
        int i = 0;
        while ((l = raf.readLine()) != null) {
            i++;
            System.out.println("\n-- i:" + i + "line:" + l);
        }
        assertEquals(4, i);

    }


    static class Pair {
        private final long pos;
        String line;

        public Pair(long filePointer, String line) {
            this.pos = filePointer;
            this.line = line;
        }

        @Override
        public String toString() {
            return Long.toString(this.pos);
        }
    }
    @Test
    public void shouldCollectLineOffsetsCorrectly() throws Exception {
        ArrayList<Pair> linesRead = new ArrayList<Pair>();

        StringBuilder collected = new StringBuilder();
        collectLines(linesRead, collected);
        System.out.println("Pointers:" + linesRead + "\n\n\n");

        // now reverse read back each line
        Collections.reverse(linesRead);
        for (Pair pair : linesRead) {
            raf = new MLineByteBufferRAF(file.getAbsolutePath());
            long long1 = pair.pos;
            raf.seek(long1);
            String actual = raf.readLine();
            if (actual != null) {
                System.out.println("Reading:" + long1 + " :" + actual);
                if (!pair.line.equals(actual)) {
                    System.out.println("ERROR Seek:" + long1);
                }

                assertEquals("EXPECTED FAIL FOR Line1.1. (bug!)Got The wrong line: " + pair.line + " POS:" + pair.pos + " - " + actual, pair.line, actual);
            }

            raf.close();

        }
    }

    private void collectLines(ArrayList<Pair> linesRead, StringBuilder collected) throws IOException {
        String line = "";
        raf.seek(0);

        while ( line != null) {
            Pair pair = new Pair(raf.getFilePointer(), raf.readLine());
            if (pair.line != null) {
                linesRead.add(pair);

                collected.append(pair.line).append(extra);
                System.out.println("Pos:[" + pair.toString() + " thisLine:"+ pair.line.length() + " Collected:" + collected.length() + " - " + pair.line + "]");
            }
            line = pair.line;
        }
    }

    @Test
    public void shouldReadCorrectLines() throws Exception {
        String line0 = raf.readLine();
        String line1 = raf.readLine();
        assertEquals(1, raf.linesRead());
        String line2 = raf.readLine();
        assertEquals(2, raf.linesRead());
        String line3 = raf.readLine();
        assertEquals(2, raf.linesRead());

        assertTrue(line2.startsWith("line 2"));
        assertTrue("didnt end with line 2.1:" + line2, line2.contains("\tline 2.1"));

        assertTrue(line3.startsWith("line 3"));
        assertTrue(line3.contains(" line 3.1"));
    }
	
//	int readCount;
//	@Test
//	public void shouldDetectWasEol() throws Exception {
//		new File(TEST_FILE).createNewFile();
//		MLineByteBufferRAF raf = new MLineByteBufferRAF(TEST_FILE) {
//			@Override
//			public byte read() throws IOException {
//				if(readCount++ < 10) {
//					return RAF.EOL_N;
//				}
//				return RAF.EOF;
//			}
//		};
//
//		raf.readLine();
//		assertTrue("should have found newline", raf.wasEOLFound());
//	}
//
//	@Test
//	public void shouldDetectWasNotEol() throws Exception {
//		new File(TEST_FILE).createNewFile();
//		MLineByteBufferRAF raf = new MLineByteBufferRAF(TEST_FILE) {
//			@Override
//			public byte read() throws IOException {
//				if(readCount++ == 0) {
//					return 'a';
//				}
//				return RAF.EOF;
//			}
//		};
//
//		raf.readLine();
//		assertFalse("should not have found newline", raf.wasEOLFound());
//	}


}
