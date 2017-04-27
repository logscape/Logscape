package com.liquidlabs.log;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.monitor.LoggingEventMonitor;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.index.FileItem;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.indexer.krati.KratiIndexer;
import com.liquidlabs.transport.proxy.ProxyFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class AgentLogServiceAdminTest {
	
	private ProxyFactory proxyFactory;
	private Indexer indexer;

	@Before
	public void before() {
		proxyFactory = mock(ProxyFactory.class);
		indexer = new KratiIndexer("build/agentLSAdmin");
		
		addFieldSets();
        when(proxyFactory.getEndPoint()).thenReturn("endPoint");
	}

	private void addFieldSets() {
		indexer.addFieldSet(FieldSets.getCiscoASALog());
		indexer.addFieldSet(FieldSets.getCiscoPIXLog());
		indexer.addFieldSet(FieldSets.getNetScreenMsg());
		indexer.addFieldSet(FieldSets.getNetScreenTraffic());
		indexer.addFieldSet(FieldSets.getBasicFieldSet());
		indexer.addFieldSet(FieldSets.get2008EVTFieldSet());
		indexer.addFieldSet(FieldSets.getLog4JFieldSet());
		indexer.addFieldSet(FieldSets.getNetScreenMsg());
		indexer.addFieldSet(FieldSets.getNetScreenTraffic());
		indexer.addFieldSet(FieldSets.getAccessCombined());
		indexer.addFieldSet(FieldSets.getSysLog());
        indexer.addFieldSet(FieldSets.getNTEventLog());
        indexer.addFieldSet(FieldSets.getLog4JFieldSet());
        indexer.addFieldSet(FieldSets.getAgentStatsFieldSet());
        indexer.addFieldSet(FieldSets.getBasicFieldSet());

	}
	

	@Test
	public void shouldListDirsWhenFilesExceeded() throws Exception {
		
		File testDir = new File("build/" + getClass().getSimpleName());
		boolean isDeleteFile = true;// false;
		int dirCount = 10;// * 1000;
		int fileCount = 50;// * 100 * 1000;
		if (isDeleteFile) {
			FileUtil.deleteDir(testDir);
			testDir.mkdir();
			for (int i = 0; i < fileCount; i++) {
				File file = new File(testDir, "afile" +i);
				file.createNewFile();
			}
			for (int i = 0; i < dirCount; i++) {
				new File(testDir, "dir" +i).mkdir();
			}
		}

		int fileLimit = 5;
		int dirLimit = 5;
		
		AgentLogServiceAdminImpl service = new AgentLogServiceAdminImpl(proxyFactory, indexer, new LoggingEventMonitor());
		service.setFileListLimit(fileLimit);
		service.setDirListLimit(dirLimit);
		List<FileItem> subDir = service.listDirContents(testDir.getAbsolutePath(), "", "*", false);
		
		int foundDirCount = 0;
		int foundFileCount = 0;
		for (FileItem fileItem : subDir) {
			if (fileItem.type.equals("dir")) foundDirCount++;
			else foundFileCount++;
		}
		FileUtil.deleteDir(testDir);
		assertTrue("Got Wrong File count:" + foundFileCount, foundFileCount <= fileLimit * 2);
		assertTrue("Got Wrong Dir count:" + foundDirCount, foundDirCount >= dirLimit && foundDirCount < dirLimit  + 10);
        verify(proxyFactory).registerMethodReceiver(anyString(), anyString());
		
	}
	
	@Test
	public void shouldListRootDir() throws Exception {
		AgentLogServiceAdminImpl service = new AgentLogServiceAdminImpl(proxyFactory, indexer, new LoggingEventMonitor());
		List<FileItem> listDirContents = service.listDirContents("", "", "*", false);
		assertTrue("Didnt get Root Dir listing", listDirContents.size() > 0);
	}
	@Test
	public void shouldListSubDir() throws Exception {
		AgentLogServiceAdminImpl service = new AgentLogServiceAdminImpl(proxyFactory, indexer, new LoggingEventMonitor());
		List<FileItem> listDirContents = service.listDirContents("", "", "*", false);
		assertTrue("Didnt get content listing", listDirContents.size() > 0);
		
		List<FileItem> subDir = service.listDirContents("/", "", "*", false);//listDirContents.get(0).label);
		assertTrue(subDir.size() > 0);
	}

}

