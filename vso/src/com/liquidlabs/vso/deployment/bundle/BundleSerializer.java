package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.common.Logging;
import com.liquidlabs.common.file.FileUtil;
import com.thoughtworks.xstream.XStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class BundleSerializer {
	Logger logger = Logger.getLogger(BundleSerializer.class);
    static Logging auditLogger = Logging.getAuditLogger("VAuditLogger", "BundleSerializer");
    private final File downloads;

    public BundleSerializer(File downloads) {
        this.downloads = downloads;
    }

	public Bundle loadBundle(String xmlOrFilename){
		try {
            xmlOrFilename = makePathNative(xmlOrFilename);
            auditLogger.emit("LoadingBundle", xmlOrFilename);

			XStream stream = configureXStream();
			Bundle bundle = null;
			InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(xmlOrFilename);

			if (!xmlOrFilename.endsWith(".bundle")) {
				bundle = (Bundle) stream.fromXML(xmlOrFilename);
				if (new File(xmlOrFilename).getParentFile() != null) {
					bundle.id = bundle.getBundleIdForFilename(new File(xmlOrFilename).getParentFile().getName());
				}
			} else if (resourceAsStream != null) {
				bundle = (Bundle) stream.fromXML(resourceAsStream);
				resourceAsStream.close();
			} else {
				logger.info("Loading Bundle:" + xmlOrFilename);
				if (!new File(xmlOrFilename).exists()) {
					xmlOrFilename = findAnyBundle(xmlOrFilename);
					if (xmlOrFilename == null) throw new RuntimeException("No bundle found for:" + xmlOrFilename);
					else logger.info("Revert Load to file:" + xmlOrFilename);
				}
				FileReader fileReader = new FileReader(xmlOrFilename);
				bundle = (Bundle) stream.fromXML(fileReader);
				fileReader.close();
				if (bundle != null && new File(xmlOrFilename).getParentFile() != null) {
					bundle.setId(bundle.getBundleIdForFilename(new File(xmlOrFilename).getParentFile().getName()));
				}
			}
			if (bundle == null) throw new RuntimeException("Failed to locate bundle:" + xmlOrFilename);
			postProcessServices(bundle);
			return bundle;
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}
    public static String makePathNative(String path) {
        // nix
        if (File.separator.equals("/")) {
            if (path.contains("\\")) path = path.replaceAll("\\\\", File.separator);
        } else {
            if (path.contains("/")) {
                path = path.replaceAll("\\/", "\\\\");
                path = path.replaceAll("\\\\\\\\", "\\\\");
                if (path.startsWith("\\") && path.contains(":")) path = path.substring(1);
            }
            if (path.contains("\\\\")) path = StringUtils.replace(path, "\\\\", File.separator);
        }
        return path;
    }


    private String findAnyBundle(String xmlOrFilename) {
		File parentFile = new File(xmlOrFilename).getParentFile();
		if (parentFile != null && parentFile.exists() && parentFile.isDirectory()) {
			String[] list = parentFile.list(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".bundle");
				}
			});
			if (list != null && list.length > 0) return new File(parentFile,list[0]).getAbsolutePath();

		}
		return null;
	}

	public void postProcessServices(Bundle bundle) {
        Properties bundleOverrides = loadOverrides(bundle.getBundleName());
//        logger.info("Override:" + bundle.getName() + " props:" + bundleOverrides);
		for (Service service : bundle.getServices()) {
			service.setBundleId(bundle.id);
			service.setSystem(bundle.isSystem());
            service.overrideWith(bundleOverrides);
		}
	}



    private Properties loadOverrides(String bundleName) {
        File overrides = new File(downloads, String.format("%s-override.properties", bundleName));
        auditLogger.emit("LoadingOverride", overrides.getName());
        Properties properties = new Properties();
        if(overrides.exists() && overrides.isFile()) {
            FileInputStream fileInputStream = null;
            try{
                logger.info("Loading overrides for bundle: " + bundleName);
                fileInputStream = new FileInputStream(overrides);
                properties.load(fileInputStream);
            } catch(IOException io) {

            } finally {
                if (fileInputStream!=null) {
                    try {
                        fileInputStream.close();
                    } catch (IOException e) {

                    }
                }
            }
        } else {
            auditLogger.emit("LoadingOverride-NotFound", overrides.getName());

        }
        return properties;
    }

    /* (non-Javadoc)
      * @see com.liquidlabs.vso.deployment.bundle.BundleHandler#getXML(com.liquidlabs.vso.deployment.bundle.Bundle)
      */
	public String getXML(Bundle bundle) {
		XStream stream = configureXStream();
		return stream.toXML(bundle);
	}
	private XStream configureXStream() {
		XStream stream = new XStream();
		stream.alias(Bundle.class.getSimpleName(), Bundle.class);
		stream.useAttributeFor(Bundle.class, "name");
		stream.useAttributeFor(Bundle.class, "version");
		stream.useAttributeFor(Bundle.class, "system");
		stream.useAttributeFor(Bundle.class, "autoStart");
		stream.alias(Service.class.getSimpleName(), Service.class);
		return stream;
	}


}
