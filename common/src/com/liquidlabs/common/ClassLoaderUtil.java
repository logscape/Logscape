package com.liquidlabs.common;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;

public class ClassLoaderUtil {

    //To check the .class file, try the following.
    public static void dumpJavaClassVersion(String path) throws Exception  {
        File file = new File(path);
        InputStream in = new FileInputStream(file);
            byte[] header = new byte[8];
            int nread = in.read(header);
            if (nread != header.length) {
                System.err.printf("Only %d bytes read%n", nread);
                return;
            }
            if (header[0] != (byte)0xCA || header[1] != (byte)0xFE
                    || header[2] != (byte)0xBA || header[3] != (byte)0xBE) {
                System.err.printf("Not a .class file (CAFE BABE): %02X%02X %02X%02X",
                        header[0], header[1], header[2], header[3]);
                return;
            }
            int minorVs = ((header[4] & 0xFF) << 8) | (header[5] & 0xFF);
            int majorVs = ((header[4] & 0xFF) << 8) | (header[5] & 0xFF);
            final String[] versionsFrom45 = {"1.1", "1.2", "1.3", "1.4", "5", "6", "7", "8", "9", "10"};
            int majorIx = majorVs - 45;
            String majorRepr = majorIx < 0 || majorIx >= versionsFrom45.length ? "?" : versionsFrom45[majorIx];
            System.out.printf("Version %s, internal: minor %d, major %d%n",
                    majorRepr, minorVs, majorVs);

    }
	public static void addFiles(URLClassLoader systemClassLoader, File[] libraries) throws MalformedURLException {
		if (libraries == null || libraries.length == 0) return;
        if (libraries != null) {
            for (File library : libraries) {
                try {
					com.liquidlabs.common.ClassLoaderUtil.addFileToPath(systemClassLoader, library);
				} catch (Exception e) {
				}
            }
        }
	}
	
	/**
	 * Add Directory or JAR to the given URL ClassLoader
	 * @param loader
	 * @param file
	 * @throws MalformedURLException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public static void addFileToPath(URLClassLoader loader, File file) throws MalformedURLException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		URL url = file.getName().endsWith(".jar") ? toJarURL(file) : file.toURI().toURL();
		addURL(loader, url);
	}
	private static URL toJarURL(File library) throws MalformedURLException {
		return new URL(String.format("jar:%s!/", library.toURI().toURL()));
	}
	
	public static void addURL(URLClassLoader loader, URL url)
			throws NoSuchMethodException, IllegalAccessException,
			InvocationTargetException {
		HashSet<URL> urls = new HashSet<URL>(Arrays.asList(loader.getURLs()));
		if (urls.contains(url)) return;
		
		Class urlClass = URLClassLoader.class;
		Method method = urlClass.getDeclaredMethod("addURL", new Class[] { URL.class });
		method.setAccessible(true);
		method.invoke(loader, new Object[] { url });
	}

}
