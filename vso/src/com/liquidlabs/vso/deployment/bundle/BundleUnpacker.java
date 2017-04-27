package com.liquidlabs.vso.deployment.bundle;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;

import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.common.file.FileUtil;

public class BundleUnpacker {

	private static Logger LOGGER = Logger.getLogger(BundleUnpacker.class);
	private HashGenerator hasher = new HashGenerator();
	private final File systemBundles;
	private final File deployedBundles;
	
	public BundleUnpacker(File system_bundles, File deployed_bundles) {
		this.systemBundles = system_bundles;
		this.deployedBundles = deployed_bundles;
	}

	public boolean isBundle(File toCheck) {
        if (!toCheck.getName().endsWith(".zip")) return false;
		ZipInputStream zipInputStream = null;
		try {
			zipInputStream = new ZipInputStream(new FileInputStream(toCheck));
			ZipEntry nextEntry = null;
			while((nextEntry = zipInputStream.getNextEntry())!=null) {
				if(nextEntry.getName().endsWith(".bundle")) {
					return true;
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
	
	public Bundle unpack(File bundleZip, boolean allowSystemUnpack) {
		String bundleName = getBundleName(bundleZip.getName());
		Bundle theBundle = getBundle(bundleZip);
		if (theBundle == null) throw new RuntimeException("No bundle exists for:" + bundleName);
		
		theBundle.id = bundleName;
		
		// cannot autoDeploy System bundles as the processes are already running
		// the system needs to be bounced so the restarting Agent unzips and it is deployed then
		if (!allowSystemUnpack && theBundle.isSystem()) return theBundle;
		
		File bundleDir = theBundle.isSystem() ? systemBundles : deployedBundles;
		LOGGER.info("Unzipping bundle: " + bundleName + " to " + bundleDir.getAbsolutePath());
		File unzipDir = new File(bundleDir, bundleName);
		if (unzipDir.exists()) {
			FileUtil.deleteDir(unzipDir);
		}
		
		unzipDir.mkdir();
		
		ZipInputStream zipInputStream = null;
		try {
			zipInputStream = new ZipInputStream(new BufferedInputStream(new FileInputStream(bundleZip)));
			
			ZipEntry nextEntry = null;
			while ((nextEntry = zipInputStream.getNextEntry()) != null) {
				try {
					if (nextEntry.isDirectory()) {
						new File(unzipDir, nextEntry.getName()).mkdirs();
					} else {
						 writeToFile(unzipDir, zipInputStream, nextEntry);
					}
				} catch (Throwable t) {
					LOGGER.error("Failed to expand:" + nextEntry, t);
				}
			}
			if (!System.getProperty("os.name").toUpperCase().contains("WINDOWS")) {
				new ProcessBuilder("chmod", "-R", "a+x", unzipDir.getAbsolutePath()).start();
			}
			writeHash(unzipDir, hasher.createHash(bundleZip.getName(), bundleZip));
		} catch (ZipException e) {
			throw new RuntimeException("ZipException, Failed to unpack file:" + bundleZip.getName() + " ex:" + e.toString(), e);
		} catch (IOException e) {
			LOGGER.warn("UnpackFailed:", e);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Failed to unzip file:" + bundleZip.getName() + " ex:" + e.toString(), e);
		} finally {
			try {
				if (zipInputStream != null) zipInputStream.close();
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
		return theBundle;
	}

	public Bundle getBundle(File bundleZipFile) {
		ZipInputStream zipInputStream = null;
		ArrayList<String> filenames = new ArrayList<String>();
		try {
			zipInputStream = new ZipInputStream(new BufferedInputStream(new FileInputStream(bundleZipFile)));
			ZipEntry nextEntry = null;
			while ((nextEntry = zipInputStream.getNextEntry()) != null) {
				filenames.add(nextEntry.getName());
				if (nextEntry.getName().endsWith(".bundle")) {
					File bundleXml = File.createTempFile("the", ".bundle");
					OutputStream bundleStream = new BufferedOutputStream(new FileOutputStream(bundleXml));
					writeToStream(zipInputStream, bundleStream);
					BundleSerializer serializer = new BundleSerializer(new File(VSOProperties.getDownloadsDir()));
					Bundle loadBundle = serializer.loadBundle(bundleXml.getAbsolutePath());
					loadBundle.setBundleIdFromZip(bundleZipFile.getName());
					serializer.postProcessServices(loadBundle);
					bundleXml.delete();
					return loadBundle;
				}
			}
		} catch (Throwable t) {
			LOGGER.info("ZIP Contents:" + filenames);
			LOGGER.error(String.format("Failed to locate any bundles in zip:%s path:%s", bundleZipFile.getName(), bundleZipFile.getAbsolutePath()), t);
			throw new RuntimeException(t.getMessage(), t);
		} finally {
			if (zipInputStream != null) {
				try {
					zipInputStream.close();
				} catch (IOException e) {
				}
			}
		}
		return null;
	}

	private void writeToFile(File unzipDir, ZipInputStream zipInputStream, ZipEntry nextEntry) throws IOException {
		File file = new File(unzipDir, nextEntry.getName());
		file.getParentFile().mkdirs();
		FileOutputStream outputStream = new FileOutputStream(file);
		writeToStream(zipInputStream, new BufferedOutputStream(outputStream));
	}

	private void writeToStream(ZipInputStream zipInputStream,
			OutputStream outputStream) throws IOException {
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

	public String getBundleName(String bundleName) {
		if (bundleName.indexOf(".") == -1) return null;
		bundleName = bundleName.substring(0, bundleName.lastIndexOf("."));
		return bundleName;
	}

	public Bundle getBundle(String bundleId) {
		if (bundleId.endsWith(".zip")) {
			bundleId = getBundleName(bundleId);
		}
		if (bundleId.indexOf("-") == -1) return null;
		
		String bundleName = bundleId.substring(0, bundleId.lastIndexOf("-")) + ".bundle";
		if (new File(systemBundles, bundleId).exists())   return new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle(new File(systemBundles + "/" + bundleId, bundleName).getAbsolutePath());
		if (new File(deployedBundles, bundleId).exists()) return new BundleSerializer(new File(VSOProperties.getDownloadsDir())).loadBundle(new File(deployedBundles + "/" + bundleId, bundleName).getAbsolutePath());
		return null;
	}

	public boolean deleteExpandedBundleDir(String bundleName) {
		Bundle bundle = getBundle(bundleName);
		if (bundle != null) {
			if (bundle.isSystem()) {
				FileUtil.deleteDir(new File(systemBundles, bundleName));
				return true;
			}
			if (!bundle.isSystem()) {
				FileUtil.deleteDir(new File(deployedBundles, bundleName));
				return true;
			}
		}
		return false;
		
	}
}
