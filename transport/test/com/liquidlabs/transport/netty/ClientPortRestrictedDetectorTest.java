package com.liquidlabs.transport.netty;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;

public class ClientPortRestrictedDetectorTest {
	
	
	private ClientPortRestrictedDetector detector;

	@Before
	public void before() {
		detector = new ClientPortRestrictedDetector();

	}

	@Test
	public void shouldReturnListOfExistingFiles() throws Exception {
		File file1 = new File("build", "file.props");
		FileUtil.mkdir("build/path1");
		File file2 = new File("build/path1", "file.props");
		File file3 = new File("build/path2", "notMefile3.props");
		
		file1.createNewFile();
		file2.createNewFile();
		
		List<File> results = detector.findListOfExistingFiles(".;build;build/path1;build/path2", "file.props");
		assertTrue(results.size() == 2);
		
		
	}
	
	@Test
	public void shouldFindTrueInLinesList() throws Exception {
		assertTrue(detector.isValueFound("someProp=true", Arrays.asList("line1","sysprops=stuff and things -DsomeProp=true")));
		assertFalse(detector.isValueFound("someXXProp=true", Arrays.asList("line1","sysprops=stuff and things -DsomeProp=true")));
	}

}
