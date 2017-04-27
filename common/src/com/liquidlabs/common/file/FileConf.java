package com.liquidlabs.common.file;

import java.util.HashMap;
import java.util.Map;

public class FileConf {
	Map<String, FileEntry> files = new HashMap<String, FileEntry>();


	public void addFileEntry(String filename, String key, String value) {
		if (!files.containsKey(filename)) files.put(filename, new FileEntry(filename));
		files.get(filename).put(key, value);
		
	}
	
	public static class FileEntry {
		public FileEntry(String file) {
			this.file = file;
		}
		String file;
		Map<String, String> updates = new HashMap<String, String>();
		
		public void put(String key, String value) {
			updates.put(key, value);
		}
	}
	public static class Update {
		String value;
	}

}
