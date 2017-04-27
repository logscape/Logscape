package com.liquidlabs.log.fields;

import com.liquidlabs.common.file.FileUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;


public class FieldSetAssemberTest {
	
	private boolean sortFieldSets = true;
    private String tags;

    @Test
	public void shouldNotAllowStupidFieldSet() throws Exception {
		FieldSet basicFieldSet = FieldSets.getBasicFieldSet();
		ArrayList<FieldSet> sets = new ArrayList<FieldSet>();
		sets.add(basicFieldSet);
		basicFieldSet.filePathMask = "logs";
		basicFieldSet.fileNameMask = "*";
		List<String> lines = FileUtil.readLines("test-data/agent.log",100);
		FieldSet fieldSetSelected = new FieldSetAssember().determineFieldSet("test-data/agent.log", sets, lines, sortFieldSets, tags);
		
		assertNull(fieldSetSelected);
		
	}
	@Test
	public void shouldNotBlowUpOnSingleFieldsetMisMatch() throws Exception {
		FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
		List<String> lines = FileUtil.readLines("test-data/logexample/syslog.log",100);
		FieldSet fieldSetSelected = new FieldSetAssember().determineFieldSet("test-data/syslog.log", Arrays.asList( log4JFieldSet), lines, sortFieldSets, tags);
		assertNull(fieldSetSelected);
	}
	
	
	@Test
	public void shouldFindSysLogFormat() throws Exception {
		FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
		FieldSet basicFieldSet = FieldSets.getBasicFieldSet();
		FieldSet sysLogFieldSet = FieldSets.getSysLog();
		sysLogFieldSet.fileNameMask = "*sys*";
        sysLogFieldSet.filePathMask = "**";
		List<String> lines = FileUtil.readLines("test-data/logexample/syslog.log",100);
		FieldSet fieldSetSelected = new FieldSetAssember().determineFieldSet("test-data/logexample/syslog.log", Arrays.asList( basicFieldSet, log4JFieldSet, sysLogFieldSet), lines, sortFieldSets, tags);
		assertNotNull(fieldSetSelected);

		String test = FieldSetUtil.test(sysLogFieldSet,sysLogFieldSet.example);
		System.out.println(test);
		
		assertEquals("syslog", fieldSetSelected.getId());
		
		
	}
	@Test
	public void shouldRevertOnPrecedence() throws Exception {
		FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
        log4JFieldSet.filePathMask += ",./test-data";
		FieldSet basicFieldSet = FieldSets.getBasicFieldSet();
		List<String> lines = FileUtil.readLines("test-data/engine.log",100);
		FieldSet fieldSetSelected = new FieldSetAssember().determineFieldSet("test-data/engine.log", Arrays.asList( basicFieldSet, log4JFieldSet), lines, sortFieldSets, tags);
		assertNotNull(fieldSetSelected);
		assertEquals("basic", fieldSetSelected.getId());
		
	}
	@Test
	public void shouldValidateLog4JFileCorrectly() throws Exception {
		ArrayList<FieldSet> sets = new ArrayList<FieldSet>();
		sets.add(FieldSets.get());
        FieldSet log4JFieldSet = FieldSets.getLog4JFieldSet();
        log4JFieldSet.filePathMask += ",./test-data";
        sets.add(log4JFieldSet);
		sets.add(FieldSets.getCiscoASALog());
		sets.add(FieldSets.getCiscoIDSIPDLog());
		sets.add(FieldSets.getNetScreenMsg());
		sets.add(FieldSets.getNetScreenTraffic());
		sets.add(FieldSets.getNTEventLog());
		sets.add(FieldSets.getBasicFieldSet());
		sets.add(FieldSets.getSysLog());
		List<String> lines = FileUtil.readLines("test-data/agent.log",100);
		FieldSet fieldSetSelected = new FieldSetAssember().determineFieldSet("test-data/agent.log", sets, lines, sortFieldSets, "logscape-logs");
		
		assertNotNull(fieldSetSelected);
		assertEquals("log4j",fieldSetSelected.id);
	}

}
