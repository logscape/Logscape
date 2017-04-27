package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.rawlogserver.handler.ContentFilteringLoggingHandler.TypeMatcher;
import com.liquidlabs.rawlogserver.handler.fileQueue.DefaultFileQueuer;
import com.liquidlabs.rawlogserver.handler.fileQueue.FileQueuer;
import javolution.util.FastMap;
import org.joda.time.DateTimeUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SplunkPacketizer implements StreamHandler {
	protected static String eol = System.getProperty("line.separator");
	
	public Map<String, FileQueuer> fileQueue = new FastMap<String, FileQueuer>();
	List<TypeMatcher> matchers = new ArrayList<TypeMatcher>();

	private final ScheduledExecutorService scheduler;

	public SplunkPacketizer(ScheduledExecutorService scheduler) {
		this.scheduler = scheduler;
		// ensure a scheduled flush writes data off to disk
		scheduler.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				Collection<FileQueuer> queuers = fileQueue.values();
				for (FileQueuer queuer : queuers) {
					queuer.run();
				}
				
			}
		}, 2, Integer.getInteger("raw.server.fq.delay.secs", 2), TimeUnit.SECONDS);

	}
	public StreamHandler copy() {
		return new SplunkPacketizer(scheduler);
	}
	
	public void handled(byte[] payload, String address, String host, String rootDir) {
		SplunkPacket splunkPacket = new SplunkPacket(payload);
		if (splunkPacket.raw.length() <= 1) return;
		
		String nowDate = getNowDate();

		TypeMatcher typeMatcher = getTypeMatcher(splunkPacket.raw);
		if (typeMatcher == null) typeMatcher = new TypeMatcher();
		
		
		File file = setupFileOutput(rootDir,host, splunkPacket, typeMatcher.parts, nowDate + typeMatcher.type);
		String filePath = file.getAbsolutePath();
		if (!fileQueue.containsKey(filePath)) {
			File parentFile = file.getParentFile();
			if (!parentFile.exists()) parentFile.mkdirs();
			fileQueue.put(filePath, new DefaultFileQueuer(filePath, false));
		}
		// broken due to TZ differences
		String nowTimeStamp = DateUtil.shortDateTimeFormat3.print(splunkPacket.getTime());
		String writeData = timestampPayload(splunkPacket.raw, typeMatcher.isTimeStamp(), nowTimeStamp);
		fileQueue.get(filePath).append(writeData);
		
	}
	
	private String timestampPayload(String payload, boolean isTimeStamping, String nowDateTime) {
		if (isTimeStamping) {
			StringBuilder sb = new StringBuilder(nowDateTime);
			sb.append(" ");
			sb.append(payload);
			if (!payload.endsWith("\n") && !payload.endsWith(eol)) sb.append(eol);
			return sb.toString();
		}
		if (!payload.endsWith("\n") && !payload.endsWith(eol)) return payload + eol;
		return payload;
	}

	private File setupFileOutput(String rootDir, String host, SplunkPacket splunkPacket, String[] typeInfo, String now) {
		StringBuilder outputPath = new StringBuilder(rootDir);
		outputPath.append("/").append(host);
		outputPath.append("/splunk/").append(splunkPacket.getOutputPath());
		for (String item : typeInfo) {
			outputPath.append(item);
			outputPath.append("/");
		} 
		File file = new File(outputPath.toString(), now + ".log");
		return file;
	}
	protected String getNowDate() {
		return DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis());
	}
	public TypeMatcher getTypeMatcher(String data) {
		for (TypeMatcher matcher : this.matchers) {
			if (matcher.isMatch(data)) return matcher;
		}
		return null;
	}

	public void setTimeStampingEnabled(boolean b) {
	}

	public void start() {
		List<String> mappingContent = ContentFilteringLoggingHandler.loadMappingContent();
		ContentFilteringLoggingHandler.populate(mappingContent, matchers);
	}

	@Override
	public void stop() {
	}
	public static class SplunkPacket {
		long time;
		String raw;
		String source;
		String sourceType;
		String host;
//		String path;
		byte[] given;
		public SplunkPacket(byte[] data) {
			this.given = data;
			String unixTimeString = extractWord("_time\0\0\0".getBytes(), "\0\0\0".getBytes(), data);
			if (unixTimeString.length() > 2) unixTimeString = unixTimeString.substring(1);
			try {
			time = Long.parseLong(unixTimeString) * 1000;
			} catch (Throwable t) {
				time = -1;
			}
			raw = extractWord("_raw\0\0\0".getBytes(), "\0\0\0".getBytes(), data);
			if (raw.length() > 2) raw = raw.substring(1);
			sourceType = extractWord("sourcetype:".getBytes(),"\0\0\0".getBytes(), data);
			source = extractWord("source:".getBytes(),"\0\0".getBytes(), data);
			host = extractWord("host:".getBytes(),"\0\0".getBytes(), data);
		}
		public long getTime() {
			if (time == -1) return DateTimeUtils.currentTimeMillis();
			return time;
		}
		public String getOutputPath() {
			if (sourceType.equals(source)) {
				String format = String.format("%s_%s",host,sourceType.replaceAll(" ", "-"));
				format = format.replaceAll(":", "-");
				return format;
			}
			String format = String.format("%s_%s_%s",host,sourceType.replaceAll(" ", "-"),source.replaceAll("\\/", "-"));
			format = format.replaceAll(":", "-");
			return format;
		}
		public String extractWord(byte[] from, byte[] to, byte[] data) {
			int fromIndex = Arrays.arrayContains(from, data);
			if (fromIndex == -1) return "";
			fromIndex += from.length;
			int fromIndex2 = Arrays.arrayContains(from, data, fromIndex);
			if (fromIndex2 > fromIndex) fromIndex = fromIndex2 + from.length;
			
			
			int toIndex = Arrays.arrayContains(to, data,  fromIndex);
			if (toIndex == -1) return "";
			byte[] copyOfRange = java.util.Arrays.copyOfRange(data, fromIndex+1, toIndex);
			return new String(copyOfRange);
		}
		public String toString() {
//			return String.format("%s \n\ttype:%s \n\tsource:%s \n\thost:%s \n\tpath:%s \n\traw:%s", getClass().getSimpleName(), sourceType, source, host, path, raw);
			return String.format("%s \n\ttype:%s \n\tsource:%s \n\thost:%s\n\tpath:%s\n\traw:%s", getClass().getSimpleName(), sourceType, source, host, getOutputPath(), raw);
		}
		
	}

}
