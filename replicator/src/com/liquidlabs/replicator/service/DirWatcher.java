package com.liquidlabs.replicator.service;

import com.liquidlabs.replicator.download.Downloader;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

/**
 * Watches a directory for all files except ".tmp" and those starting with "."
 * 
 * When a file is created or deleted generate an event - {@link DirEventHandler}.created/deleted
 * 
 * Existing files are ignored on startup
 * 
 * To prevent events firing during a download etc the file can use anything starting with "." or ending in ".tmp"
 * and use an atomic file operation (i.e. rename) when completed
 * Used by {@link Downloader}
 */
public class DirWatcher implements Runnable{

	private final File watchDirectory;
	private final String[] fileExtToIgnore = new String[] { ".tmp", ".bak"};
    private final DirWatcher parent;
    private long lastScan;

	Logger LOGGER = Logger.getLogger(DirWatcher.class);
	
	
	Set<File> lastFileScan = new HashSet<File>();
	HashMap<File, DirWatcher> subDirs = new HashMap<File, DirWatcher>();

	private DirEventHandler handler = new DirEventHandler() {

		public void created(File file) {
		}
		public void modified(File file) {
		}

		public void deleted(File file) {
		}

		public void stop() {
		}

	};



    /**
	 * 
	 * @param parent - null when this is the parent
     * @param inDirectory
     */
	public DirWatcher(DirWatcher parent, File inDirectory) {
		this.parent = parent;
		this.watchDirectory = inDirectory;

		if (!inDirectory.exists()) inDirectory.mkdir();
		
		// need to be careful about the quiet period cause if the repeat interval is less, then
		// you can detect a modification 2x
        lastFileScan.addAll(getDirScan(true));
        updateLastScan();

	}

    private void updateLastScan() {
        lastScan = System.currentTimeMillis();
    }

    public void registerEventHandler(DirEventHandler myEventHandler) {
		this.handler = myEventHandler;
	}


	private void scan() {
		
		if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format(">>> scan:%s children[%s]", watchDirectory.getName(), lastFileScan.size()));
		
		/**
		 * Created Files
		 */
		File[] created = getCreatedFiles();
		
		for (File file : created) {
			LOGGER.info(String.format("Created:%s",file.getPath()));
			handler.created(file);
		}

		
		/**
		 * Modified Files
		 */
		File[] modified = getModifiedFiles();
		
		for (File file : modified) {
			LOGGER.info(String.format("Modified:%s", file.getPath()));
			handler.modified(file);
		}
		
		handleDeletedFiles();
		
		handleChildDirectories();
		
		lastFileScan.clear();
		lastFileScan.addAll(getDirScan(true));
        updateLastScan();
		if (LOGGER.isDebugEnabled()) LOGGER.debug(String.format("<<< scan:%s children[%s]", this.watchDirectory, lastFileScan.size()));

	}

	private void handleChildDirectories() {
		Set<File> currentDirList = getDirScan(false);
		
		// handle deleted DirWatchers
		Set<File> deletedDirs = new HashSet<File>(this.subDirs.keySet());
		deletedDirs.removeAll(currentDirList);
		for (File file : deletedDirs) {
			if (!file.isDirectory()) continue;
			subDirs.remove(file);
			LOGGER.debug(getParentDir() + " RemovedChild:" + file.getPath());
		}
		
		// handle new DirWatchers
		Set<File> newFileWatchers = new HashSet<File>(currentDirList);
		newFileWatchers.removeAll(this.subDirs.keySet());
		for (File file : newFileWatchers) {
			if (!file.isDirectory()) continue;
			DirWatcher dirWatcher = new DirWatcher(this, file);
			dirWatcher.registerEventHandler(handler);
			LOGGER.debug(getParentDir() + " NewChild:" + file.getPath());
			subDirs.put(file, dirWatcher);
		}
		
		// ripple scan to children
		for (File file : subDirs.keySet()) {
			subDirs.get(file).scan();
		}
	}

	private String getParentDir() {
		return this.parent != null ? this.parent.watchDirectory.getPath() : "ROOT";
	}

	private void handleDeletedFiles() {
		ArrayList<File> deleted = new ArrayList<File>(lastFileScan);
		deleted.removeAll(getDirScan(true));
		
		for (File file : deleted) {
			LOGGER.info(String.format(" Deleted:%s", file.getPath()));
			handler.deleted(file);
		}
	}


	private Set<File> getDirScan(final boolean filesOnly) {
        File[] listFiles = watchDirectory.listFiles(new FilenameFilter() {
			public boolean accept(File file, String name) {
				boolean isDir = new File(file, name).isDirectory();
				if (isDir && filesOnly) return false;
				return !(name.startsWith(".") || shouldIgnore(name, fileExtToIgnore));
			}
		});
		if (listFiles == null) listFiles = new File[0];

		return new HashSet<File>(Arrays.asList(listFiles));
	}


	private File[] getCreatedFiles() {
		File[] listFiles = watchDirectory.listFiles(new FilenameFilter() {
			public boolean accept(File parent, String name) {
				File childFile = new File(parent, name);
				if (childFile.isDirectory()) return false;
				if (name.startsWith(".") || shouldIgnore(name, fileExtToIgnore)) return false; 
				
				if (isNewToScan(childFile)) return true;
				return false;
			}

			private boolean isNewToScan(File childFile) {
				for (File file : lastFileScan) {
					if (file.getName().equals(childFile.getName())) return false;
				}
				return true;
			}
			
		});
		if (listFiles == null) listFiles = new File[0];
		return listFiles;
	}

	private File[] getModifiedFiles() {
		File[] listFiles = watchDirectory.listFiles(new FilenameFilter() {
			public boolean accept(File parent, String name) {
				File childFile = new File(parent, name);
				if (childFile.isDirectory()) return false;
				if (name.startsWith(".") || shouldIgnore(name, fileExtToIgnore)) return false; 
				
				if (!isNewToScan(childFile)) {
					long lastModified = childFile.lastModified();
					return	lastModified > lastScan;
				}
				return false;
			}
			
			private boolean isNewToScan(File childFile) {
				for (File file : lastFileScan) {
					if (file.getName().equals(childFile.getName())) return false;
				}
				return true;
			}
			
		});
		if (listFiles == null) listFiles = new File[0];
		return listFiles;
	}

	
	protected boolean shouldIgnore(String name, String[] fileExtensions) {
		for (String ext : fileExtensions) {
			if (name.contains(ext)) return true;
		}
		return false;
	}


    public void run() {
        try {
            scan(); 
        } catch(Exception e) {
            LOGGER.warn(format("DirWatcher scan of %s failed with %s", watchDirectory.getAbsolutePath(), e.getMessage()), e);
        }

    }


}

