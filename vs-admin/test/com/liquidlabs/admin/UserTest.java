package com.liquidlabs.admin;

import static org.junit.Assert.*;

import org.junit.Test;

public class UserTest {
	
	@Test
	public void shouldWorkWithFileIncludeRestrictions() throws Exception {
		//if (user.isFileAllowed(logFile.getFileName(), logFile.getTags()))
		String notThis = "/opt/logscape/SocketServers/172.16.100.21/172.16.100.21_33448/FWall/FWall-11Jul31.log";
		String yesThis1 = "/opt/logscape/SocketServers/38.123.140.133/38.123.140.133_33448/FWall/YES-11Jul31.log";
		String yesThis2 = "/opt/logscape/SocketServers/38.123.140.130/38.123.140.130_33448/FWall/YES-11Jul31.log";
		User user = new User();
		user.setUsername("guest");
		user.department = "RUII";
		user.fileIncludes = "*38.123.140.130*,*38.123.140.133*";
		assertFalse(user.isFileAllowed("", notThis, "forwardedLogs"));
		assertTrue(user.isFileAllowed("", yesThis1, "forwardedLogs"));
		assertTrue(user.isFileAllowed("", yesThis2, "forwardedLogs"));
	}
	
	@Test
	public void shouldMatchFilesWithIncludesFilter() throws Exception {
		String guest = "C:\\work\\workspace\\master\\build\\logscape\\work\\schedule\\11Sep13-schedule-guest2.log";
		String guest2 = "C:\\work\\workspace\\master\\build\\logscape\\work\\schedule\\11Sep13-schedule-guest2.log";
		
		User user = new User();
        user.setUsername("guest");
		user.department = "guest";
		user.fileIncludes = "tag:guest";
		user.fileExcludes = ".*pwd.*,.*password.*";
		
		assertTrue(user.isFileAllowed("", guest, "logscape-logs"));
		// THIS WONT WORK BECAUSE IT USES *logscape*dept* - groups/depts cannot subgroup each other
		//assertFalse(user.isFileAllowed(guest2, "logscape-logs"));
	}
	
	@Test
	public void shouldAllowImplicitUserPath() throws Exception {
		String userPath = "C:\\work\\workspace\\logscape\\webapp\\reports\\11Sep09\\user\\Sample_Report09-Sep-11_10-12-52.pdf";
		String userPath2 = "C:\\work\\workspace\\logscape\\webapp\\reports\\11Sep09\\user2\\Sample_Report09-Sep-11_10-12-52.pdf";
		String guestPath = "C:\\work\\workspace\\logscape\\webapp\\reports\\11Sep09\\guest\\Sample_Report09-Sep-11_10-12-52.pdf";
		User user = new User();
        user.setUsername("harry");
		user.department = "guest";
		user.fileIncludes = "tag:harry";
		assertFalse(user.isFileAllowed("", userPath, ""));
//		assertTrue(user.isFileAllowed(guestPath, ""));
		
		assertFalse(user.isFileAllowed("", userPath2, ""));
	}
	
	@Test
	public void shouldWorkWthIncludeWildTags() throws Exception {
		User user = new User();
        user.setUsername("harry");
		user.fileIncludes = "tag:*dev*";
		assertTrue(user.isFileAllowed("", "a three file","dev-md"));
		assertFalse(user.isFileAllowed("", "a three file","de1v-md"));
	}

	
	@Test
	public void shouldWorkWthIncludeTags() throws Exception {
		User user = new User();
        user.setUsername("harry");
		user.fileIncludes = "tag:group";
		assertTrue(user.isFileAllowed("", "a three file","group"));
	}
	@Test
	public void shouldWorkWthTags() throws Exception {
		User user = new User();
        user.setUsername("harry");
		user.fileIncludes = "*one*,*two*,tag:aTag";
		assertTrue(user.isFileAllowed("", "a three file","aTag"));
	}

	@Test
	public void shouldExcludeFilesWhenSpecified() throws Exception {
		User user = new User();
        user.setUsername("harry");
		user.fileExcludes = "*one*,*two*";
		
		assertFalse(user.isFileAllowed("", "a one file",""));
		assertFalse(user.isFileAllowed("", "a two file",""));
		assertTrue(user.isFileAllowed("", "a three file",""));
	}
	
	
	@Test
	public void shouldIncludeFilesWhenFilterSpecd() throws Exception {
		User user = new User();
        user.setUsername("harry");
		user.fileIncludes = "*one*,*two*";
		assertTrue(user.isFileAllowed("", "a one file",""));
		assertTrue(user.isFileAllowed("", "a two file",""));
		assertFalse(user.isFileAllowed("", "a three file",""));
	}
	
	@Test
	public void shouldDoAllFilesWhenNoFilterSpecified() throws Exception {
		User user = new User();
        user.setUsername("harry");
		assertTrue(user.isFileAllowed("","some file",""));
	}

}
