package com.liquidlabs.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.apache.log4j.Logger;

import com.liquidlabs.common.file.FileUtil;

public class Unpacker {
	
	public static boolean isValidZip(File file) {
		if (file.getName().endsWith(".zip")) {
			File tempDir = null;
			try {
				String tempDirName = System.getProperty("java.io.tmpdir");
				tempDir = new File(tempDirName + "/"  + System.currentTimeMillis());
				boolean success = tempDir.mkdirs();
				new Unpacker().unpack(tempDir , file);
				// all good - cleanup
				return true;
			} catch (Throwable t) {
				// boom - failed
			} finally {
				FileUtil.deleteDir(tempDir);
				
			}
			return false;
		} else {
			return true;
		}
	}
	private final static Logger LOGGER = Logger.getLogger(Unpacker.class);
	
	public void unpack(File parent, File bundle) {
		ZipInputStream zipInputStream = null;
		File unzipDir = parent;
		try {
			int i = 0;
			zipInputStream = new ZipInputStream(new FileInputStream(bundle));
			
			ZipEntry nextEntry = null;
			while ((nextEntry = zipInputStream.getNextEntry()) != null) {
				i++;
				if (nextEntry.isDirectory()) {
					new File(unzipDir, nextEntry.getName()).mkdirs();
				} else {
					 writeToFile(unzipDir, zipInputStream, nextEntry);
				}
			}
			if (!System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
				new ProcessBuilder("chmod", "-R", "a+x", unzipDir.getAbsolutePath()).start();
			}
			if (i == 0) throw new RuntimeException("Didnt find any zip items:" + bundle.getPath());
		} catch (ZipException e) {
			throw new RuntimeException("ZipException, Failed to unpack file:" + bundle.getName() + " ex:" + e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.error(e);
			throw new RuntimeException("IOZipException, Failed to unpack file:" + bundle.getName() + " ex:" + e.getMessage(), e);
		} finally {
			try {
				if (zipInputStream != null) zipInputStream.close();
			} catch (IOException e) {
				LOGGER.error(e);
			}
		}
	}
	private void writeToFile(File unzipDir, ZipInputStream zipInputStream, ZipEntry nextEntry) throws IOException {
		File file = new File(unzipDir, nextEntry.getName());
		file.getParentFile().mkdir();
		FileOutputStream outputStream = new FileOutputStream(file);
		writeToStream(zipInputStream, outputStream);
	}
	private void writeToStream(ZipInputStream zipInputStream,
			FileOutputStream outputStream) throws IOException {
		byte [] buf = new byte[4098];
		int read;
		try {
			while ((read = zipInputStream.read(buf, 0, 4098)) != -1) {
				outputStream.write(buf, 0, read);
			}
		}finally {
			outputStream.close();
		}
	}

}
