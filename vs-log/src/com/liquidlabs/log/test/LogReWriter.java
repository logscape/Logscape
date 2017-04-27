package com.liquidlabs.log.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.util.DateTimeExtractor;

/**
 * Rewrites a logfile X days of logs back from today
 *
 */
public class LogReWriter {

	DateTimeExtractor timeExtractor;
	DateTimeFormatter formatter;
	String format;
	

	public LogReWriter(String format) {
		timeExtractor = new DateTimeExtractor(format);
		formatter = DateTimeFormat.forPattern(format);
		this.format = format;
	}
	
	public int rewriteDir(String destName, String sourceName, int daysOffset) throws IOException {
		FileUtil.mkdir(destName);
		File source = new File(sourceName);
		if (!source.exists()) throw new RuntimeException("SourceDir does not exist:" + sourceName);
		File[] listFiles = source.listFiles();
		if (listFiles == null) throw new RuntimeException("No files found in directory:" + source.getAbsolutePath());
		
		int count = 0;
		for (File file : listFiles) {
			System.out.println("Rewritting:" + file.getAbsolutePath());
			int rewrite = rewrite(destName + File.separator + file.getName(), file.getAbsolutePath(), daysOffset);
			System.out.println("Rewritten:" + file.getName() + " lines:" + rewrite);
			count++;
		}
		
		
		return count;
	}	

	public int rewrite(String dest, String source, int daysOffset) throws IOException {
		
		System.out.println("  extracting data 1");
		int elapsedMins = getElapsedMins(source);
		if (elapsedMins == -1) return 0;
		DateTime newLogStartTime = new DateTime().minusMinutes(elapsedMins +1);
		newLogStartTime = newLogStartTime.minusDays(daysOffset);
		
		System.out.println("  extracting data 2");
		DateTime sourceStartTime = getLogStartTime(source);
		System.out.println("  extracting data 3");
		List<String> templateLines = getTemplateLines(source);
		FileOutputStream fos = getFos(true, dest);
		
		int lines = 0;
		
		System.out.println("  processing");
		for (String line : templateLines) {
			Date time = timeExtractor.getTime(line, System.currentTimeMillis());
			if (time == null) {
				fos.write(line.getBytes());
				fos.write("\n".getBytes());
			} else {
				// log line delta
				DateTime delta = new DateTime(time).minus(sourceStartTime.getMillis());
				DateTime newLineTime = newLogStartTime.plus(delta.getMillis());
				String newLine = String.format("%s %s\n", formatter.print(newLineTime), line.substring(format.length()));
				fos.write(newLine.getBytes());
			}
			lines++;
		}
		System.out.println("  done");
		fos.close();
		return lines;
	}
	public int getElapsedMins(String filename) throws IOException {
		
		DateTime endTime = getLogEndTime(filename);
		DateTime startTime = getLogStartTime(filename);
		if (endTime == null || startTime == null) return -1;
		DateTime minus = endTime.minus(startTime.getMillis());
		return (int) (minus.getMillis() / (1000 * 60));
	}

	public DateTime getLogEndTime(String filename) throws IOException {
		System.out.println("      et>");
		try {
			List<String> templateLines = getTemplateLines(filename);
			Collections.reverse(templateLines);
			int attempt = 0;
			for (String line : templateLines) {
				Date time = timeExtractor.getTime(line,  System.currentTimeMillis());
				if (time != null) return new DateTime(time);
				if (attempt++ > 20) return null;
			}
			return null;
		} finally {
			System.out.println("      et<");
		}
	}
	public DateTime getLogStartTime(String filename) throws IOException {
		try{ 
			System.out.println("     st>");
			List<String> templateLines = getTemplateLines(filename);
			int attempt = 0;
			for (String line : templateLines) {
				Date time = timeExtractor.getTime(line,  System.currentTimeMillis());
				if (time != null) return new DateTime(time);
				if (attempt++ > 20) return null;
			}
			return null;
		} finally {
			System.out.println("      st<");
		}
	}
	private List<String> getTemplateLines(String source) throws IOException {
		FileInputStream fis = new FileInputStream(source);
		Scanner scanner = new Scanner(fis);
		ArrayList<String> result = new ArrayList<String>();
		while (scanner.hasNext()) {
			String nextLine = scanner.nextLine();
			if (nextLine.endsWith(">")) nextLine += "\n";
			result.add(nextLine);
		}
		fis.close();
		return result;
	}

	private FileOutputStream getFos(boolean deleteIfExists, String dest) throws IOException, FileNotFoundException {
		File file = new File(dest);
		if (deleteIfExists) {
			file.delete();
		}
		file.createNewFile();
		
		FileOutputStream fos = new FileOutputStream(file, !deleteIfExists);
		return fos;
	}

}
