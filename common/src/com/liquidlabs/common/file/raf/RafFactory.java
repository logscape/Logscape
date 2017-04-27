package com.liquidlabs.common.file.raf;

import com.liquidlabs.common.file.raf.GzipRaf;
import com.liquidlabs.common.file.raf.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;

public class RafFactory {

	private static String rafMethod = System.getProperty("raf.type", ByteBufferRAF.class.getSimpleName());

	public static RAF getRaf(String filename, String breakRule) throws IOException {
		if (filename == null) throw new RuntimeException("Filename is NULL");
		if (breakRule.equals(BreakRule.Rule.SingleLine.name())) return getRafSingleLine(filename);
		RAF raf = null;

        try {
            if (filename.endsWith(".gz")) raf = new MLineGzip(filename);
		    if (filename.endsWith(".gzip"))  raf = new MLineGzip(filename);
        } catch (IOException ioe) {
            return new MLineByteBufferRAF(filename);

        }
        try {
		    if (filename.endsWith(".bz2")) raf = new MLineBzip(filename);
        } catch (IOException ioe) {
            if (ioe.getMessage().contains("formatted: expected")) {
                return new MLineByteBufferRAF(filename);
            }
        }
        if (filename.endsWith(".snap")) return new MLineSnapRAF(filename);
		if (filename.endsWith(".lz4")) return new MLineLz4RAF(filename);
//		if (raf == null && rafMethod.equals("Raf")) raf = new MLineRaf(new File(filename));
//		if (raf == null && rafMethod.equals(BufferedVRaf.class.getSimpleName())) raf = new MLineVRaf(new File(filename));
//		if (raf == null && rafMethod.equals(ByteBufferRAF.class.getSimpleName())) raf = new MLineByteBufferRAF(filename);
		if (raf == null) raf = new MLineByteBufferRAF(filename);
		raf.setBreakRule(breakRule);
		return raf;
	}
	public static RAF getRafSingleLine(String filename) throws IOException {
		return getRafSingleLine(filename, rafMethod);
	}
	public static RAF getRafSingleLine(String filename, String rafMethod) throws IOException {
		if (filename == null) throw new RuntimeException("Filename is NULL");
		if (filename.endsWith(".gz")) return new GzipRaf(filename);
		if (filename.endsWith(".gzip")) return new GzipRaf(filename);
		if (filename.endsWith(".bz2")) return new BzipRaf(filename);
        if (filename.endsWith(".snap")) return new SnapRaf(filename);
		if (filename.endsWith(".lz4")) return new Lz4Raf(filename);
        return new ByteBufferRAF(filename);
	}

}
