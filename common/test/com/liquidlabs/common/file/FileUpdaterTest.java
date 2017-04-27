package com.liquidlabs.common.file;

import org.junit.Test;

import com.thoughtworks.xstream.XStream;

public class FileUpdaterTest {

	@Test
	public void testShouldAddFileEntry() {
		
		FileConf fileConf = new FileConf();
		fileConf.addFileEntry("myTest.props", "-Xmx64M", "-Xmx512M");
		XStream stream = new XStream();
		System.out.println(stream.toXML(fileConf));
		
	}
	

}
