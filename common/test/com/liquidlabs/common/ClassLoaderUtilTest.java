package com.liquidlabs.common;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

public class ClassLoaderUtilTest {
	
	
	@Test
	public void shouldAddOnlyOnce() throws Exception {
		URLClassLoader systemClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		File file = new File("test-data/classLoaderTest");
		assertFalse(getURLs(systemClassLoader).toString().contains(file.getName()));
		ClassLoaderUtil.addFiles(systemClassLoader, new File[] { file } );
		assertEquals(2, getURLs(systemClassLoader).toString().split(file.getName()).length);
		
		ClassLoaderUtil.addFiles(systemClassLoader, new File[] { file } );
		assertEquals(2, getURLs(systemClassLoader).toString().split(file.getName()).length);
		
	}
	
	@Test
	public void addDirectoryToSystemCL() throws Exception {
		URLClassLoader systemClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        ClassLoaderUtil.addFiles(systemClassLoader, new File[] {new File("imaginaryPath")} );
		assertThat(getURLs(systemClassLoader).toString(), containsString("imaginaryPath"));
	}
	
	@Test
	public void shouldAddJarPathToSystemCL() throws Exception {
		URLClassLoader systemClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		File file = new File("test-data/test.jar");
		assertTrue(file.exists());
		assertFalse(getURLs(systemClassLoader).toString().contains(file.getName()));
		
		ClassLoaderUtil.addFiles(systemClassLoader, new File[] { file } );
		assertTrue(getURLs(systemClassLoader).toString().contains(file.getName()));
	}

	private HashSet<URL> getURLs(URLClassLoader systemClassLoader) {
		HashSet<URL> before = new HashSet<URL>(Arrays.asList(systemClassLoader.getURLs()));
		return before;
	}
}
