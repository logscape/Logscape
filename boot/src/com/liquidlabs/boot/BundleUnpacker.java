package com.liquidlabs.boot;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class BundleUnpacker {

	private HashGenerator hasher = new HashGenerator();
	
	
	public boolean isSystemBundle(File toCheck) {
		ZipInputStream zipInputStream = null;
		try {
			zipInputStream = new ZipInputStream(new FileInputStream(toCheck));
			ZipEntry nextEntry = null;
			while((nextEntry = zipInputStream.getNextEntry())!=null) {
				if(nextEntry.getName().endsWith(".bundle")) {
					ByteArrayOutputStream stream = new ByteArrayOutputStream();
					writeToStream(zipInputStream, stream);
					String [] lines = new String(stream.toByteArray()).split("\n");
					for (String line : lines) {
						if (line.trim().toLowerCase().startsWith("<bundle name")) {
							return line.contains("system=\"true\"") || line.contains("system='true'");
						}
					}
				}
			}
		}catch (Throwable t) {
		} finally {
			if (zipInputStream != null) {
				try {
					zipInputStream.close();
				} catch (IOException e) {
				}
			}
		}
		
		return false;
	}
	
	private void writeToStream(ZipInputStream zipInputStream,
			OutputStream outputStream) throws IOException {
		byte [] buf = new byte[10 * 1024];
		int read;
		try {
			while ((read = zipInputStream.read(buf, 0, 10 * 1024)) != -1) {
				outputStream.write(buf, 0, read);
			}
		}finally {
			outputStream.close();
		}
	}
	
	public String unpack(File bundleDir, File bundle) {
		String bundleName = getBundleName(bundle);
		File unzipDir = new File(bundleDir, bundleName);
		if (unzipDir.exists()) {
			delete(unzipDir);
		}
		
		unzipDir.mkdir();
		
		ZipInputStream zipInputStream = null;
		try {
			zipInputStream = new ZipInputStream(new FileInputStream(bundle));
			
			ZipEntry nextEntry = null;
			while ((nextEntry = zipInputStream.getNextEntry()) != null) {
				try {
				if (nextEntry.isDirectory()) {
					new File(unzipDir, nextEntry.getName()).mkdirs();
				} else {
					 writeToFile(unzipDir, zipInputStream, nextEntry);
				}
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
			if (!System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
				new ProcessBuilder("chmod", "-R", "a+x", unzipDir.getAbsolutePath()).start();
			}
			writeHash(unzipDir, hasher.createHash(bundle.getName(), bundle));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (zipInputStream != null) zipInputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return bundleName;
	}

	private void writeToFile(File unzipDir, ZipInputStream zipInputStream, ZipEntry nextEntry) throws IOException {
		FileOutputStream outputStream = new FileOutputStream(new File(unzipDir, nextEntry.getName()));
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

	private void writeHash(File unzipDir, String hash) throws IOException {
		FileOutputStream outputStream = new FileOutputStream(new File(unzipDir, "vs.hash"));
		try {
			outputStream.write(hash.getBytes());
		} finally {
			outputStream.close();
		}
		
	}

	public String getBundleName(File bundle) {
		String bundleName = bundle.getName();
		bundleName = bundleName.substring(0, bundleName.lastIndexOf("."));
		return bundleName;
	}
	
	private void delete(File dir) {
        File[] files = dir.listFiles();
        for (File file : files) {
                if (file.isDirectory()) delete(file);
                else file.delete();
        }
        dir.delete();
	}

}
