package com.liquidlabs.ffilter;

import java.io.*;
import java.util.*;

public class FileFilter {

	public static void main(String[] args) {
		String workingDir = args[0];
		System.out.println("Working Dir:" + workingDir);
        FileFilter fileFilter = new FileFilter(workingDir, "etc/setup.conf","boot.properties:etc/logscape-service-x86.ini:etc/logscape-service-x64.ini:conf/agent.conf:conf/lookup.conf:logscape.sh:logscape.bat");
		Map<String, String> extractVars = fileFilter.extractVars();
//		System.out.println("Values:" + extractVars);
		fileFilter.process();
		
	}
	public void process() {
		String[] path = this.filePath.split(":");
		String fullFilePath = "";
		for (String file : path) {
			try {
				File file2 = new File(rootDir, file);
				if (!file2.exists()) continue;
//				System.out.println("Processing:" + file2.getAbsolutePath());
				fullFilePath = file2.getAbsolutePath();
				rewriteFile(file2);
			} catch (Throwable t) {
				t.printStackTrace();
			}
			if (file.endsWith(".sh")) executeChmodExecute(fullFilePath);
		}
		
	}
	private void executeChmodExecute(String fullFilePath) {
		try {
			Process exec = Runtime.getRuntime().exec("chmod +x " + fullFilePath);
			exec.waitFor();
			exec.destroy();
		} catch (Exception e) {
		}
		
	}
	private final String configFile;
	private final String filePath;
	HashMap<String, String> vars = new HashMap<String, String>();
	HashMap<String, String> varForFilename = new HashMap<String, String>();
	private final String rootDir;

	public FileFilter(String root, String configFile, String filePath) {
		this.rootDir = root;
		System.out.println("Using\tConfig:" + configFile);
		System.out.println("     \tPath:" + filePath);
		this.configFile = configFile;
		this.filePath = filePath;
	}

	public boolean matches(String token, String line) {
		return line.contains(token);
	}

	String filterVariable(String key, String value, String line) {
		try {
			if (line.trim().length() == 0) return line;
			if (line.indexOf(key) == -1) return line;
			if (line.indexOf("$" + key) > 0) return line;
			
			String firstPart = line.substring(0, line.indexOf(key) + key.length());
            String token = " ";
			int nextToken = line.indexOf(token, firstPart.length());
            int nextQuote = line.indexOf("\"", firstPart.length());
            if (nextQuote != -1 && (nextToken == -1 || nextQuote < nextToken)) {
                nextToken = nextQuote;
                token = "\"";
            }

            String existingValue = "";

            if (nextToken > firstPart.length() +1 ) {
                nextToken = firstPart.length();
            }

			// filter to the next space, unless - the new value also contains a space, in which case do the EOL
			if (nextToken == -1 || nextToken > firstPart.length() +1 ) {

				existingValue = line.substring(firstPart.length()+1);
			} else {
                int endToken = line.indexOf(token, nextToken+1);
                if (endToken == -1) endToken = line.length();

                existingValue = line.substring(nextToken+1, endToken);
			}
			String result = existingValue.length() > 0 ? replaceLast(line, existingValue, value) : line;
			
			// only process lines with single # script comment
			if (result.startsWith("#") && !result.startsWith("##")) result = result.substring(1);
			return result;
		} catch (Throwable t) {
			System.err.println("Failed to process:" + key + " value:" + value + " line:" + line);
			t.printStackTrace();
			return line;
		}
	}

	private String replaceLast(String line, String existingValue, String value) {
		//line.replace(existingValue, value)
		if (!line.contains(existingValue)) return line;
		String var = line.substring(0, line.lastIndexOf(existingValue));
		String tailEnd = line.substring(var.length() + existingValue.length());
		
		return var + value + tailEnd;
	}
	public void addKeyValue(String key, String value) {
		this.vars.put(key, value);
	}

	public String resolveLine(String line) {
		for (String key : vars.keySet()) {
			if (line.contains("$" + key)) line = line.replace("$" + key,  vars.get(key));
		}
		return line;
	}

	public Map<String, String> extractVars() {
		File configFile = new File(this.rootDir, this.configFile);
		if (!configFile.exists()) {
			throw new RuntimeException("Failed to load configFile, root:" + this.rootDir + " cfg:" + this.configFile);
		}
		
//		System.out.println("Extracting variables from:" + configFile.getAbsolutePath());
		Map<String, String> results = vars;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(configFile));
			String line = "";
			while ((line = reader.readLine()) != null) {
				String[] keyValueFile = getKeyValue(line);
				if (keyValueFile.length == 0) continue;
				
				if (keyValueFile[1].trim().length() == 0) {
//					System.out.println("Ignoring substition:" + keyValueFile[0]);
					continue;
				}
				String value = resolveLine(keyValueFile[1]);
				results.put(keyValueFile[0], value);
				if (keyValueFile[2].length() > 0) this.varForFilename.put(keyValueFile[0], keyValueFile[2]);
			}
			reader.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return results;
	}

	public String[] getKeyValue(String line) {
		if (line.indexOf("key") == -1) return new String[0];
		String key = extractToken(line, "key=\"");
		String value = extractToken(line, "value=\"");
		String file = extractToken(line, "file=\"");
		return new String[] { key, value, file } ;
	}

	private String extractToken(String line, String token) {
		if (line.indexOf(token) == -1) return "";
		return line.substring(line.indexOf(token)+token.length(), line.indexOf("\"", line.indexOf(token) + token.length())).trim();
	}

	public List<String> processFile(File file) {
		List<String> contents = new ArrayList<String>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = "";
			while ((line = reader.readLine()) != null) {
				contents.add(filterLine(file.getName(), line) );
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Cannot process:" + file.getName()+ " msg:" + e.getMessage());
		}
		return contents;
	}

	String filterLine(String filename, String line) {
		Set<String> keys = this.vars.keySet();
		for (String key : keys) {
			if (this.varForFilename.containsKey(key)){
				if (this.varForFilename.get(key).equals(filename)) {
					line = filterVariable(key, vars.get(key), line);
				}
			} else {
				if (line.startsWith("sysprops")) {
					//System.out.println("Processing:" + key + " line:" + line);
					
				}
				line = filterVariable(key, vars.get(key), line);				
			}
		}
		return line;
	}

	public void rewriteFile(File file) {
		try {
			List<String> newContents = processFile(file);
			File dest = new File(file.getAbsolutePath()+".bak");
			if (dest.exists()) dest.delete();
			boolean renameTo = file.renameTo(dest);
			if (!renameTo) System.err.println("Rename FAILED to rename to .bak file");
			PrintWriter writer = new PrintWriter(new FileWriter(file));
			for (String line : newContents) {
				writer.print(line + "\n");
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public static boolean copyFile(File from, File to) {
		mkdir(to.getParent());
		FileOutputStream fos = null;
		FileInputStream fis = null;
		try {
			fos = new FileOutputStream(to);
			fis = new FileInputStream(from);
			byte[] buffer = new byte[256 * 1024];
			while (fis.available() != 0) {
				int amount = fis.read(buffer);
				if (amount > 0) fos.write(buffer, 0, amount);
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (fos != null)
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			if (fis != null)
				try {
					fis.close();
				} catch (IOException e) {
				}
		}
	}
	public static File mkdir(String path) {
		String seperator = File.separator;
		if (seperator.equals("\\")) seperator = "\\" + File.separator;
		String[] dir = makePathNative(path).split(seperator);
		if (dir == null) throw new RuntimeException("Failed to createPath:" + path);
		StringBuilder sb = new StringBuilder();
		for (String string : dir) {
			sb.append(string);
			File file = new File(sb.toString());
			if (!file.exists()) file.mkdir();
			
			sb.append(File.separator);
		}
		return new File(path);
	}
	public static String makePathNative(String path) {
		// nix
		if (File.separator.equals("/")) {
			path = path.replaceAll("\\\\", File.separator);
		} else {
			path = path.replaceAll("\\/", "\\\\");
			path = path.replaceAll("\\\\\\\\", "\\\\");
		}
		return path;
	}



}
