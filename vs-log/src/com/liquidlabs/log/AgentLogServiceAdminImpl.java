package com.liquidlabs.log;

import com.liquidlabs.admin.User;
import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.raf.BreakRuleUtil;
import com.liquidlabs.common.monitor.Event;
import com.liquidlabs.common.monitor.EventMonitor;
import com.liquidlabs.common.regex.FileMaskAdapter;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSetAssember;
import com.liquidlabs.log.index.FileItem;
import com.liquidlabs.log.index.Indexer;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.index.LogFileOps;
import com.liquidlabs.log.streamer.FileStreamer;
import com.liquidlabs.log.streamer.LogFileStreamer;
import com.liquidlabs.log.streamer.LogLine;
import com.liquidlabs.log.util.DateTimeExtractor;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.transport.serialization.Convertor;
import com.liquidlabs.vso.agent.metrics.DefaultOSGetter;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AgentLogServiceAdminImpl implements AgentLogServiceAdmin {

	static final Logger LOGGER = Logger.getLogger(AgentLogServiceAdminImpl.class);
	private FileStreamer fileStreamer;
	private static int maxReadSecs = LogProperties.getMaxReadWaitSecs();

	private int fileListLimit = 25;

	private int dirListLimit = 500;
	private int maxItems = Integer.getInteger("max.list",300);

	private Indexer indexer;
    private final EventMonitor eventMonitor;
    private String hostname = NetworkUtils.getHostname();
	private ScheduledExecutorService scheduler;

	public String getUID() {
		return AgentLogServiceAdmin.NAME;
	}
	// we arent exposing/registering any gui stuff for now
	public String getUI() {
		return "</xml>";
	}
	public AgentLogServiceAdminImpl(ProxyFactory proxyFactory, Indexer indexer, EventMonitor eventMonitor) {
		
		this.indexer = indexer;
        this.eventMonitor = eventMonitor;
        LOGGER.info("Starting:" + proxyFactory.getEndPoint());
		
		fileStreamer = new FileStreamerParent(indexer, new LogFileStreamer(indexer));
		makeRemotable(proxyFactory);
		scheduler = proxyFactory.getScheduler();
		
		LOGGER.info("<< Started:" + proxyFactory.getEndPoint());
	}
	private void makeRemotable(ProxyFactory proxyFactory) {
		proxyFactory.registerMethodReceiver(getUID(), this);
	}
	
	public void assign(String filename, String fieldSetId) {
        eventMonitor.raise(new Event("Manual_Assign_DataType").with("fieldSetId", fieldSetId).with("file", filename));
		indexer.assignFieldSetToLogFile(filename, fieldSetId);
	}
	
	public List<FileItem> listDirContents(String currentDir, String fileExcludes, String fileMask, boolean recurse) {
		
		try {
			String[] masks = RegExpUtil.getCommaStringAsRegExp(fileMask, DefaultOSGetter.isA());
			
			String excludesString = FileMaskAdapter.adapt(fileExcludes, DefaultOSGetter.isA());
			String[] fileExcludesT = excludesString.split(",");

			if (currentDir.endsWith(":")) currentDir += File.separator;
			LOGGER.info("List Dir:" + currentDir);
			ArrayList<FileItem> results = new ArrayList<FileItem>();
			File[] files = null;
			if (currentDir == null || currentDir.length() == 0) {
				files = File.listRoots();
			} else {
				files = new File(currentDir).listFiles();
			}
			
			if (files == null) return results;
			
	
			int realFilesCount = 0;
			int realDirsCount = 0;
			List<FieldSet> fieldSets = indexer.getFieldSets(new Indexer.AlwaysFilter());
			long start = System.currentTimeMillis();
			
			results.addAll(readFiles(files, masks, fileExcludesT, fieldSets, start, recurse));

			Collections.sort(results, new Comparator<FileItem>(){
				public int compare(FileItem o1, FileItem o2) {
					return Long.valueOf(o2.lastMod).compareTo(o1.lastMod);
				}
			});
			LOGGER.info(String.format("Return List Dir:%s files:%d results:%d dirs:%d files:%d", currentDir,files.length,results.size(), realDirsCount, realFilesCount));
			if (results.size() > maxItems) results = new ArrayList<FileItem>(results.subList(0, maxItems));
			return results;
		} catch (Throwable t) {
			t.printStackTrace();
			LOGGER.error("Failed to list:" + currentDir, t);
			return new ArrayList<FileItem>();
		}
	}
	private List<FileItem> readFiles(File[] files, final String[] masks, final String[] fileExcludesT, final List<FieldSet> fieldSetsMaster, final long start, final boolean recurse) {
		final AtomicInteger realFilesCount = new AtomicInteger();
		final AtomicInteger realDirsCount = new AtomicInteger();
		final List<FileItem> results = new CopyOnWriteArrayList<FileItem>();
		final List<File> dirs = new CopyOnWriteArrayList<File>();
		
		if (files == null || files.length == 0) return results;
		final DateTimeExtractor extractor = new DateTimeExtractor();
		
		ExecutorService pool = Executors.newFixedThreadPool(Integer.getInteger("list.files.pool", 5), new NamingThreadFactory("agent-file-browse"));
		
		List<Future<FileItem>> listFiles = new ArrayList<Future<FileItem>>();
		for (final File file : files) {
			
			Callable<FileItem> listFile = new Callable<FileItem>(){

				public FileItem call() throws Exception {
                    List<FieldSet> fieldSets = (List<FieldSet>) Convertor.clone(fieldSetsMaster);
					// check time & file count limits
					if (file.isDirectory() && realDirsCount.get() > dirListLimit) return null;
					if (!file.isDirectory()&& realFilesCount.get() > fileListLimit) return null;
					if (!hasPermissions(file)) return null;
					
					if ((System.currentTimeMillis() - start) > maxReadSecs * 1000) return null;
					
					if (file.isDirectory()){
						if (recurse) dirs.add(file);
						results.add(new FileItem(File.separator, file, false, "", false, "dir", "Default", file.lastModified(), ""));
						realDirsCount.incrementAndGet();
						return null;
					}
					
					if (isIgnoringThisExtension(file)) return null;
					String filename = FileUtil.getPath(file);
					
					if (!RegExpUtil.isMatch(filename, masks)) return null;
					
					if (isExcluded(file, fileExcludesT)) return null;
					
					List<String> contents = FileUtil.readLines(filename, 5, BreakRule.Rule.SingleLine.name());
					
					String content = StringUtil.escapeAsText(mergeLines(contents));
					LogFile logFile = null;
                    if (indexer.isIndexed(filename)) {
                        logFile = indexer.openLogFile(filename);
                    }
					String fileTag = logFile != null ? logFile.getTags() : "";
					String fileType = logFile != null ? logFile.getFieldSetId() : "basic";
					if (fileType.equals("basic")) {
						FieldSet foundFieldSet = new FieldSetAssember().determineFieldSet(filename, fieldSets, contents, false, fileTag);
						fileType = foundFieldSet != null ? foundFieldSet.getId() : "basic";
					}
					realFilesCount.incrementAndGet();
                    String timeFormat =  logFile != null ? logFile.getTimeFormat() : "";
                    String newLineRule = BreakRuleUtil.getStandardNewLineRule(contents, "Default", timeFormat);
					results.add(new FileItem(File.separator, file, isTimeKnown(file, contents, new DateTimeExtractor()), content, logFile != null, fileType, newLineRule, extractor.getFileStartTime(file, 20), fileTag));
					return null;
				}
				
			};
			listFiles.add(pool.submit(listFile));
		}
		for (Future<FileItem> future : listFiles) {
			try {
				future.get(1000, TimeUnit.MILLISECONDS);
			} catch (Exception e) {

				LOGGER.warn("TaskFailed:" + e, e);
                future.cancel(false);
			}
		}
		List<Future<List<FileItem>>> tasks = new ArrayList<Future<List<FileItem>>>();
		for (final File dir : dirs) {
			if (results.size() > fileListLimit || realFilesCount.get() > fileListLimit) continue;
			
			Future<List<FileItem>> task = pool.submit(new Callable<List<FileItem>>() {
				public List<FileItem> call() throws Exception {
					return readFiles(dir.listFiles(), masks, fileExcludesT, fieldSetsMaster, start, recurse);
				}
			});
			tasks.add(task);
		}
		for (Future<List<FileItem>> future : tasks) {
			try {
				List<FileItem> list = future.get(500, TimeUnit.MILLISECONDS);
				realFilesCount.addAndGet(list.size());
				results.addAll(list);
			} catch (Exception e) {
			}
		}
		pool.shutdownNow();
		
		return results;
	}
	private boolean hasPermissions(File file) {
		return file.canRead() && !file.getName().startsWith(".");
	}

	public void setFileListLimit(int fileListLimit) {
		this.fileListLimit = fileListLimit-1;
	}
	public void setDirListLimit(int dirListLimit) {
		this.dirListLimit = dirListLimit-1;
	}
	
	private boolean isExcluded(File file, String[] fileExcludesT) {
		for (String excludeCard : fileExcludesT) {
			if (excludeCard.length() > 0 && file.getName().matches(excludeCard)) return true;
		}
		if (isIgnoringThisExtension(file)) return true;
		return false;
	}
	private String mergeLines(List<String> contents) {
		StringBuilder result = new StringBuilder();
		for (String aLine : contents) {
			if (aLine.length() > 1024) aLine = aLine.substring(0, 1023);
			result.append(aLine).append("\n");
		}
		return result.toString();
	}
	private boolean isTimeKnown(File file, List<String> contents, DateTimeExtractor dtExtractor1) {
		String format = dtExtractor1.getFormat(file, contents);
		return format != null;
	}
	
	/**
	 * List the set of forwarded hosts
	 */
	public List<String> listHosts(final User user, Date atTime) {
		LOGGER.info("ListHosts>>" + user.username());
		long start = System.currentTimeMillis();
		long fromms = atTime != null ? atTime.getTime() : start;
		final long from =  new DateTime(fromms).minusDays(LogProperties.getMaxDayHostList()).getMillis();
			
		final Set<String> results = new HashSet<String>();
		indexer.indexedFiles(new LogFileOps.FilterCallback() {
			public boolean accept(LogFile logFile) {
				boolean isMatch = logFile.getStartTime() > from;
				if (!isMatch) return false;
				String hostname = logFile.getFileHost(AgentLogServiceAdminImpl.this.hostname);
				if (user.isFileAllowed(hostname, logFile.getFileName(), logFile.getTags())){
					results.add(hostname);
				}
				return false;
			}
		});
		
		
		long end = System.currentTimeMillis();
		LOGGER.info("ListHosts<< " + user.username() + " elapsedMs:" + (end - start) + " fileCount:" + results.size());
		return new ArrayList<String>(results);
	}
	public List<LogFile> listFiles(String givenFilter, int limit, final Date atTime, String excludeFileFilterString, final User user, String hostSubFilter) {
		
		if (hostSubFilter.length() > 0) {
			try {
				// sometimes people can enter *dev* for DEV machines
				if (this.hostname.toLowerCase().matches(hostSubFilter)) hostSubFilter = "";
			} catch (Throwable t) {
				hostSubFilter = hostSubFilter.replaceAll("\\*", "");
			}
		}
		final String finalHostFilter = hostSubFilter.toUpperCase();
		
		final String[] excludes = excludeFileFilterString == null || excludeFileFilterString.length() == 0 ? new String[0] : excludeFileFilterString.split(",");
		
		
		try {
            // TODO: Reimplement this filter
			String filter = givenFilter.contains("*") ?  FileMaskAdapter.adapt(givenFilter, DefaultOSGetter.isA()) : givenFilter;
			
			LOGGER.info(String.format(">>> listFiles:%s limit:%d time:%s given:%s excl:%s", filter, limit, atTime, givenFilter, excludeFileFilterString));
			final List<LogFile> listing = new ArrayList<LogFile>();
            indexer.indexedFiles(new LogFileOps.FilterCallback() {
                @Override
                public boolean accept(LogFile logFile) {
                    String fileHost = logFile.getFileHost(hostname);
                    if (user.isFileAllowed(fileHost, logFile.getFileName(), logFile.getTags())) {

                        // try and allow for forwarded files to be filtered
                        if (finalHostFilter.length() > 0) {
                            if (!StringUtil.containsIgnoreCase(fileHost, finalHostFilter)) return false;
                        }

                        for (String exclude : excludes) {
                            if (logFile.getFileName().contains(exclude)) return false;
                        }
                        if (atTime != null) {
                            return logFile.isWithinTime(atTime.getTime(), atTime.getTime() + DateUtil.DAY);
                        }
                        return listing.add(logFile);
                    }
                    return false;
                }
            });
            final List<LogFile>result = listFilesInternal(listing);
			
			LOGGER.info(String.format("<<< listFiles:%s count:%d", filter, result.size()));
			return result;
		} catch (Throwable t) {
			LOGGER.warn("listFailed:" + t.toString(), t);
		}
		return new ArrayList<LogFile>();
	}
	
	List<LogFile> listFilesInternal(List<LogFile> indexedFiles ) {
		
		List<LogFile> result = new ArrayList<LogFile>();

		for (LogFile indexedFile : indexedFiles) {
			if (!new File(indexedFile.getFileName()).exists()) continue;
			result.add(indexedFile);
		}
		Collections.sort(result, new Comparator<LogFile>(){
			public int compare(LogFile o1, LogFile o2) {
				return Long.valueOf(o2.getEndTime()).compareTo(o1.getEndTime());
			}
		});

		return result;
	}
	public List<LogLine> getFilePage(String fileName, long time, int pageSize) {
		return fileStreamer.getFilePage(fileName, time, pageSize);
	}
	public List<LogLine> getFilePage(String filename, int startLine, int pageSize) {
		return fileStreamer.getFilePage(filename, startLine, pageSize);
	}
	public long getUpdateTimeStamp(String filename) {
		return fileStreamer.getUpdateTimeStamp(filename);
	}
	public List<LogLine> getFileTail(String filename, int existingStartLine, int pageSize) {
		return fileStreamer.getFileTail(filename, existingStartLine, pageSize);
	}

    public List<LogLine> getFilePage(String fileName, int line, int pageSize, String searchForThis, boolean searchForwards) {
        return fileStreamer.getFilePage(fileName, line, pageSize, searchForThis, searchForwards);
    }

    boolean isIgnoringThisExtension(File file) {
		return !file.isDirectory() && WatchVisitor.isIgnoring(file.getName());
	}
}
