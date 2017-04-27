package com.liquidlabs.common;

import java.io.File;

public class FileHelper {
	
	static public File[] mkAndList(String rootDir){
		if (!new File(rootDir).exists()) {
			String[] split = rootDir.split("/");
			StringBuilder dir = new StringBuilder();
			for (String cDir : split) {
				dir.append(cDir);
				new File(dir.toString()).mkdir();
				dir.append("/");
			}
			new File(rootDir).mkdir();
		}
		return new File(rootDir).listFiles();
	}
	
	static public void delete(File dir) {
		if (dir.exists()) {
			File[] files = dir.listFiles();
			for (File file : files) {
					if (file.isDirectory()) delete(file);
					else file.delete();
			}
			dir.delete();
		}
	}

}
