package com.liquidlabs.ffilter;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.StringContains.containsString;

public class FileFilterTest {
	
	private static final String TEST_FILE = "build/test-data/filefilter/conf/agent.conf";
	private static final String TEST_FILE2 = "build/test-data/filefilter/conf/lookup.conf";
	private static final String TEST_FILE3 = "build/test-data/filefilter/logscape.sh";
	private static final String TEST_FILE4 = "build/test-data/filefilter/logscape.bat";
	private static final String TEST_FILE5 = "build/test-data/filefilter/agent.bat";
	private static final String TEST_FILE6 = "build/test-data/filefilter/props/boot.properties";
	private FileFilter fileFilter;


	@Before
	public void setup() {
		fileFilter = new FileFilter(".","test-data/filefilter/setup.conf","test-data/filefilter/test-data");
		FileFilter.copyFile(new File("test-data/filefilter/conf/agent.conf"), new File(TEST_FILE));
		FileFilter.copyFile(new File("test-data/filefilter/conf/lookup.conf"), new File(TEST_FILE2));
		FileFilter.copyFile(new File("test-data/filefilter/logscape.sh"), new File(TEST_FILE3));
		FileFilter.copyFile(new File("test-data/filefilter/logscape.bat"), new File(TEST_FILE4));
		FileFilter.copyFile(new File("test-data/filefilter/agent.bat"), new File(TEST_FILE5));
		FileFilter.copyFile(new File("test-data/filefilter/props/boot.properties"), new File(TEST_FILE6));
        fileFilter.addKeyValue("LUHOST", "SPECIAL-HOST");
        fileFilter.addKeyValue("LUPORT", "11000");

	}
	
	@Test
	public void shouldReadFromWindowsRegistry() throws Exception {
		//String value = fileFilter.readRegistry("HKLM", "SOFTWARE\\JavaSoft\\Java Runtime Environment\\1.6", "JavaHome");
		String value = WinRegistry.readString(WinRegistry.HKEY_LOCAL_MACHINE, "SOFTWARE\\JavaSoft\\Java Runtime Environment\\1.6", "JavaHome");
		String value2 = WinRegistry.readString(WinRegistry.HKEY_LOCAL_MACHINE, "SOFTWARE\\JavaSoft\\Java Runtime Environment\\1.6", "RuntimeLib");
		System.out.println("GOt:" + value + " lib:" + value2);
		// coz it wont work on anything but windows. Move to another test and somehow make it so it only runs on windows.
        if (value2 != null) {
            System.out.println("RTLIB:" + new File(value2).exists());
        }
	}
	
	@Test
	public void shouldRenameBakFilesAndCompleteContents() throws Exception {
		fileFilter.extractVars();
		assertTrue(new File(TEST_FILE).exists());
		fileFilter.rewriteFile(new File(TEST_FILE));
		assertTrue(new File(TEST_FILE).exists());
		assertTrue(new File(TEST_FILE+".bak").exists());
	}
	@Test
	public void testShouldHandleAGENT_BATFile() throws Exception {
		fileFilter.extractVars();
		List<String>  lines = fileFilter.processFile(new File(TEST_FILE5));
		assertTrue(lines.size() == 1);
		System.out.println("Lines:>>>" + lines.get(0));
		assertTrue("Got Wrong value:" + lines.get(0), lines.get(0).contains("stcp://HOST:11000"));
	}
	@Test
	public void testShouldHandleVSCAPE_BATFile() throws Exception {
		fileFilter.extractVars();
		List<String>  lines = fileFilter.processFile(new File(TEST_FILE4));
		assertTrue(lines.size() == 1);
		System.out.println("Lines:>>>" + lines.get(0));
	}
	@Test
	public void testShouldHandleScriptFile() throws Exception {
		fileFilter.extractVars();
		List<String>  lines = fileFilter.processFile(new File(TEST_FILE3));
		assertTrue(lines.size() > 1);
		// check the sysprops line was fixed properly....
//		sysprops=-DVSCAPE -Dvso.boot.lookup.replication.port=15000 -Dvso.lookup.peers=stcp://localhost:15000,stcp://localhost:25000 -Dvso.resource.type=Management -Dlog4j.configuration=./agent-log4j.properties -Dweb.app.port=8080 -Dfile.encoding=ISO-8859-1 -Dvso.group=LDN 
		for (String string : lines) {
			if (string.startsWith("sysprops")) {
				assertTrue("Got String:" + string, string.contains(" -Dvso.resource.type=Manager -D"));
			}
		}

	}

	
	@Test
	public void testShouldNOTProcessAFileWithMisMatchingName() throws Exception {
		fileFilter.extractVars();
		List<String>  lines = fileFilter.processFile(new File(TEST_FILE2));
		assertTrue(lines.size() > 1);
		String matchingLine = "";
		for (String string : lines) {
			if (string.contains("wrapper.app.parameter.2")) matchingLine = string;
		}
		System.out.println(matchingLine);
		assertFalse("Should resolve variable because xml specified matching name", matchingLine.contains("SPECIAL-HOST"));
	}
	@Test
	public void testShouldProcessAFileWithAMatchingName() throws Exception {
		fileFilter.extractVars();
		List<String>  lines = fileFilter.processFile(new File(TEST_FILE));
		assertTrue(lines.size() > 1);
		String matchingLine = "";
		for (String string : lines) {
			if (string.contains("wrapper.app.parameter.2")) matchingLine = string;
		}
		assertThat(matchingLine, containsString("SPECIAL-HOST"));
	}
	
	@Test
	public void testShouldExtactKVAndResolveVars() throws Exception {
		Map<String, String> vars = fileFilter.extractVars();
		assertEquals("stcp://SPECIAL-HOST:11000", vars.get("wrapper.app.parameter.2"));
	}
	
	@Test
	public void shouldNotReplace_SH_SCRIPT_VARIABLE() throws Exception {
		fileFilter.extractVars();
//		key="wrapper.java.command" value="C:\Program Files\Java\jre6"
		fileFilter.addKeyValue("MANAGEMENT_HOST", "someHost");
		String data1 = fileFilter.filterLine("stuff", "nohup ./boot.sh  stcp://$MANAGEMENT_HOST:$MANAGEMENT_PORT 6 > boot.log 2>&1 &");
		String data2 = fileFilter.filterLine("stuff", "MANAGEMENT_HOST=localhost");
		System.out.println("Got:" + data1);
		System.out.println("Got:" + data2);
		assertTrue(data1.contains("MANAGEMENT_HOST"));
		assertTrue(data2.contains("someHost"));
	}
	
	@Test
	public void shouldFilterLineWithVars() throws Exception {
		fileFilter.extractVars();
//		key="wrapper.java.command" value="C:\Program Files\Java\jre6"
		fileFilter.addKeyValue("wrapper.java.command", "C:\\Program Files\\Java\\jre6");
		String line = "wrapper.java.command=java";
		String data = fileFilter.filterLine("stuff", line);
		System.out.println("Got:" + data);
		assertEquals("wrapper.java.command=C:\\Program Files\\Java\\jre6", data);
		
	}
	
	@Test
	public void shouldExtractKVWithSpace() throws Exception {
		String[] keyValue = fileFilter.getKeyValue("    <add key=\"JavaHome\" value=\"c:\\program files\\jdk\"/>\r\n");
		assertEquals("JavaHome", keyValue[0]);
		assertEquals("c:\\program files\\jdk", keyValue[1]);
	}
	@Test
	public void shouldExtractKV() throws Exception {
		String[] keyValue = fileFilter.getKeyValue("    <add key=\"LUHOST\" value=\"localhost\"/>\r\n");
		assertEquals("LUHOST", keyValue[0]);
		assertEquals("localhost", keyValue[1]);
	}
	
	@Test
	public void shouldExtractVars() throws Exception {
		Map<String, String> vars = fileFilter.extractVars();
		assertTrue(vars.size() > 0);
	}
	
	@Test
	public void shouldFILTER_VAR() throws Exception {
		//<add key="MANAGEMENT_HOST" value="HOST"/>
		fileFilter.addKeyValue("MANAGEMENT_HOST","HOST");
		String line = fileFilter.filterVariable("MANAGEMENT_HOST", "somehost", "#### 2. set MANAGEMENT_HOST host");
		assertEquals("#### 2. set MANAGEMENT_HOST somehost", line);
	}
    @Test
    public void shouldFILTER_VAR_JS() throws Exception {
        //params.redir="$WEB_URL">
        String line = fileFilter.filterVariable("params.redir=", "NEWVALUE", "if (params.redir == null) params.redir=\"WEB_URL\"");
        assertEquals("if (params.redir == null) params.redir=\"NEWVALUE\"", line);
    }



//	@Test
//	public void shouldAccumulateVars() throws Exception {
//		fileFilter.addKeyValue("BOOT","localhost");
//		String line = fileFilter.filterVariable("boot", "value", "stcp://$BOOT:11000");
//		assertEquals("stcp://localhost:11000", line);		
//	}
	
	@Test
	public void shouldResolveVars() throws Exception {
		fileFilter.addKeyValue("BOOT","localhost");
		String line = fileFilter.resolveLine("stcp://$BOOT:11000");
		assertEquals("stcp://localhost:11000", line);		
	}
	
	@Test
	public void shouldFilterVarWithNewValueSpacedCorrectly() throws Exception {
		String result = fileFilter.filterVariable("some.token", "new Value","some.token=some old value");
		System.out.println("Got:" + result);
		assertTrue(result.contains("new Value"));
		assertFalse(result.contains("old"));
	}
	
	@Test
	public void shouldEnableCommentedLine() throws Exception {
		String result = fileFilter.filterVariable("some.token", "newValue","#-Dxmx256M -Dsome.token=stuff other stuff");
		System.out.println("Got:" + result);
		assertTrue(result.startsWith("-Dxmx"));
	}

	@Test
	public void shouldReplaceTokenOnLineInMiddle() throws Exception {
		String result = fileFilter.filterVariable("some.token", "newValue","-Dxmx256M -Dsome.token=stuff other things");
		System.out.println("Got:" + result);
		assertTrue(result.contains(" -Dsome.token=newValue other things"));
	}
	@Test
	public void shouldReplaceTokenOnLineOnEOL() throws Exception {
		String result = fileFilter.filterVariable("some.token", "newValue","-Dxmx256M -Dsome.token=stuff");
		System.out.println("Got:" + result);
		assertTrue(result.contains("newValue"));
	}
	
	@Test
	public void testShouldMatchLine() throws Exception {
		assertTrue(fileFilter.matches("some.token","-Dxmx256M -Dsome.token=stuff"));
		assertFalse(fileFilter.matches("some.token","-Dxmx256M -Dsome.X.token=stuff"));
	}

}
