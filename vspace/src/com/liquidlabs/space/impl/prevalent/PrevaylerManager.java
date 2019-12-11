package com.liquidlabs.space.impl.prevalent;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.prevayler.Prevayler;
import org.prevayler.PrevaylerFactory;

import com.liquidlabs.space.VSpaceProperties;

public class PrevaylerManager {
    private static final Logger LOGGER = Logger.getLogger(PrevaylerManager.class);
    private final ScheduledExecutorService executor;
    private final int snapshotIntervalSecs;
	private File baseDir;


    public PrevaylerManager(int snapshotIntervalSecs, ScheduledExecutorService executor) {
    	this.snapshotIntervalSecs = snapshotIntervalSecs;
		this.executor = executor;
		baseDir = new File(VSpaceProperties.baseSpaceDir());
		baseDir.mkdirs();
    }

    public Prevayler startPrevayler(final String partitionName, Object prevalentSystem) throws IOException, ClassNotFoundException {
    	try {
	    	final File persistentDir = new File(baseDir, partitionName);
			persistentDir.mkdirs();
            final File[] brokenSnapshots = persistentDir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return file.getName().endsWith("snapshot") && file.length() == 0;
                }
            });
            for (File brokenSnapshot : brokenSnapshots) {
                LOGGER.warn("Found broken prevalyer snapshot:" + brokenSnapshot);
                if(!brokenSnapshot.delete()) {
                    LOGGER.warn("Unable to delete broken snapshot: " + brokenSnapshot);
                }
            }
            PrevaylerFactory factory = new PrevaylerFactory();
	    	LOGGER.info("Persisting:" + partitionName + " at:" + persistentDir.getAbsolutePath());
	        factory.configurePrevalenceDirectory(persistentDir.getAbsolutePath());
			factory.configureSnapshotSerializer(new MyXStreamSerializer());
	        factory.configureJournalSerializer(new MyXStreamSerializer());
	        factory.configurePrevalentSystem(prevalentSystem);
	        final Prevayler prevayler = new PrevaylerProxy(factory.create(), factory, executor);
	        executor.scheduleWithFixedDelay(new Runnable() {
	        	public void run() {
	                try {

						LOGGER.info("Taking Snapshot:");

						try {
							prevayler.takeSnapshot();
						} catch (Exception e) {
							e.printStackTrace();
							LOGGER.info("Snapshot failed:" + e, e);
						}
						File [] snapshots = persistentDir.listFiles(new FileFilter() {
							public boolean accept(File pathname) {
								return pathname.getName().endsWith("snapshot");
							}});
	                	
	                	Arrays.sort(snapshots, new Comparator<File>() {
							public int compare(File arg0, File arg1) {
								return Long.valueOf(arg0.lastModified()).compareTo(arg1.lastModified());
							}});
	                	
	                	final File latestSnapshot = snapshots[snapshots.length -1];

						LOGGER.info("Taking Snapshot:" + latestSnapshot);
	                	
	                	File[] journals = persistentDir.listFiles(new FileFilter(){
	                		public boolean accept(File file) {
	                			return (file.getName().endsWith("journal")) && file.lastModified() < new DateTime().minusDays(1).getMillis();
						}});
	                	

                        for (int i =0; i < snapshots.length -1; i++) {
							LOGGER.info("Deleting:" + snapshots[i].getName());
                            snapshots[i].delete();
                        }

                        if(snapshots[snapshots.length-1].length() == 0) {
                            LOGGER.warn("Generated a 0kb snapshot. Will delete and use journal");
                            snapshots[snapshots.length].delete();
                        } else {
							LOGGER.warn("Deleting Journals:" + journals.length);
							for (File journal : journals) {
								journal.delete();
							}
                        }


	                } catch (Exception e) {
	                	e.printStackTrace();
	                    LOGGER.warn(e);
	                }
	
	        }}, snapshotIntervalSecs, snapshotIntervalSecs, TimeUnit.SECONDS);
            return prevayler;
        } catch (Throwable t) {
        	throw new RuntimeException(t);
        }
    }

    interface Task {
    	void run(Prevayler prevayler);
    }

    public Prevayler startWithScheduledTask(String saveDir, Object prevalentSystem, final Task t, int intervalSeconds) throws IOException, ClassNotFoundException {
    	final Prevayler prevayler = startPrevayler(saveDir, prevalentSystem);
        	executor.scheduleWithFixedDelay(new Runnable() {
        		public void run() {
        			t.run(prevayler);
                }}, 0, intervalSeconds, TimeUnit.SECONDS);
         return prevayler;
     }
}

