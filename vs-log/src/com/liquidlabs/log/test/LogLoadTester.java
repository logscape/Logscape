package com.liquidlabs.log.test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.joda.time.DateTime;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;

public class LogLoadTester {

	private final String source;
	private String dest;
	private final String format;
	private final boolean cutHead;
	private final DateTime endTime;

	public LogLoadTester(String source, String dest, String format, boolean cutHead) {
		this(source, dest, format, cutHead, new DateTime());
	}
	public LogLoadTester(String source, String dest, String format, boolean cutHead, DateTime endTime) {
		this.source = source;
		this.dest = dest;
		this.format = format;
		this.cutHead = cutHead;
		this.endTime = endTime;
	}
	public void setDest(String dest) {
		this.dest = dest;
	}

	public DateTime getStartTimeFor(int linesPerSecond, int initialLines) {
		int seconds = initialLines/linesPerSecond;
		return endTime.minusSeconds(seconds);
	}

	public int writeBurst(int ratePerSecond, int lines) throws IOException {
		OutputStream fos = new BufferedOutputStream(getFos(true));
		long  startTime = getStartTimeFor(ratePerSecond, lines).getMillis();
		System.err.println("StartTime:" + DateUtil.shortDateTimeFormat3.print(getStartTimeFor(ratePerSecond, lines)));
		List<String> readLines = getTemplateLines(source);
		
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		
		long delta = 1000/ratePerSecond;
		
		String year = Integer.toString(new DateTime().getYear());
		for (int i = 0; i < lines; i++) {
			int toRead = i % readLines.size();
			String line = readLines.get(toRead);
			boolean insertDate = line.startsWith(year);
			if (cutHead) {
				if (line.length() > format.length() && insertDate) {
					line = line.substring(format.length(), line.length());
				}
			}
			if (i % 1000 == 0) {
				fos.write(String.format("LINE:%d\n",i).getBytes());
			}
			line = formatLine(startTime, formatter, line, true);
			fos.write(line.getBytes());
			// burst the time so its is in groups of 'rate-per-second'
			if (i % ratePerSecond == 0) {
				startTime += (delta * ratePerSecond);
			}
		}
		return lines;
	}

	private String formatLine(long startTime, SimpleDateFormat formatter, String line, boolean insertDate) {
		if (!insertDate) return line + "\n";
		if (cutHead || line.startsWith("DEBUG") || line.startsWith("WARN") || line.startsWith("ERROR") || line.startsWith("INFO")) {
			return String.format("%s %s\n", formatter.format(new Date(startTime)), line);  
		}
		return line + "\n";
	}
	public int writeTail(int ratePerSecond, int timeInSeconds) throws IOException, InterruptedException {
		List<String> readLines = getTemplateLines(source);
		int sleepCount = 0;
		boolean working = true;
		int linesWritten = 0;
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		FileOutputStream fos = getFos(false);
		while (working) {
			for (int i = 0; i < ratePerSecond; i++) {
				String line = readLines.get(linesWritten++ % readLines.size());
				line = formatLine(new DateTime().getMillis(), formatter, line, false);
				fos.write(line.getBytes());
			}
			
			if (sleepCount++ > timeInSeconds) {
				working = false;
			}
			Thread.sleep(1000);
		}
		return linesWritten;
		
	}

	private FileOutputStream getFos(boolean deleteIfExists) throws IOException, FileNotFoundException {
		File file = new File(dest);
		if (deleteIfExists) {
			file.delete();
		}
		FileUtil.mkdir(file.getParent());
		file.createNewFile();
		
		FileOutputStream fos = new FileOutputStream(file, !deleteIfExists);
		return fos;
	}

	private List<String> getTemplateLines(String source) throws IOException {
		FileInputStream fis = new FileInputStream(source);
		Scanner scanner = new Scanner(fis);
		ArrayList<String> result = new ArrayList<String>();
		while (scanner.hasNext()) {
			String nextLine = scanner.nextLine();
			result.add(nextLine);
		}
		fis.close();
		return result;
	}


}
