package com.liquidlabs.services;

import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.vso.VSOProperties;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AppLoader {
	Logger logger = Logger.getLogger(AppLoader.class);
	String srcRoot = VSOProperties.getDeployedBundleDir();
	String dstRoot = LogProperties.getWebAppDir(8080) + "/logscape-apps";
    long lastModCopy = 0;
	
	public AppLoader(ScheduledExecutorService scheduler) {
		
		if (System.getProperty("war.temp.dir","xx").equals(".")) {
			System.err.println(getClass().getName() + " RUNNING IN TEST MODE !!!!!!!!!!!!!!!!!!!!!!!");
			srcRoot = "webapp/logscape-apps";
			System.err.println(new File(".", srcRoot).getAbsolutePath());
		}
		
		String absolutePath = new File(".").getAbsolutePath();
		logger.info("ROOT DIR:" + absolutePath);
		
		try {
			
			FileUtil.copyDir(srcRoot, dstRoot, new FileFilter() {
				public boolean accept(File pathname) {
					return pathname.getAbsolutePath().contains("App-");
				}
			}, lastModCopy);
		} catch (Throwable t ) {
			logger.error("Failed to copyDir:", t);
		}
		
		scheduler.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
                try {
				    FileUtil.copyDir(srcRoot, dstRoot, new FileFilter() {
					public boolean accept(File pathname) {
						return pathname.getAbsolutePath().contains("App-");
					}
				}, lastModCopy);
                    lastModCopy = System.currentTimeMillis() - 30000;
                } catch (Throwable t) {
                    logger.warn("Failed to CopyDir:" + t,t);
                }
			}
		}, 10, 10, TimeUnit.MINUTES);
	}
	
	public String loadAppData(String appName, String type, LogSpace logSpaceAdaptor) {
		try {
			if (type.equals("MenuXML")) return loadAppMenuXML(appName);
			if (type.equals("LogoURL")) return loadAppLogoURL(appName);
			if (type.equals("TitleXML")) return loadAppTitleXML(appName);
			if (type.equals("LinkMenuXML")) return loadAppLinkMenuXML(appName);
			if (type.equals("StartPageData")) return loadAppStartPageData(appName);
			if (type.equals("BGColour")) return loadAppBGColor(appName);
			return "";
		} catch (Throwable t) {
			String msg = "Failed to load App:" + appName + " data:" + type + " ex:" + t.toString();
			logger.warn(msg, t);
			throw new RuntimeException(msg);
		}
	}
	
	/**
	 * Called when the app is deployed
	 */
	public void loadConfigXMLIntoLogScape(String appName, LogSpace logSpaceAdaptor) {
		
		
		File[] configFiles = listConfigFilesForApp(appName);
		if (configFiles == null) {
			logger.info("Didnt find config files in:" + new File(appName).getAbsolutePath());
			return;
		}
		logger.info("Loading ConfigFiles:" + configFiles.length);
		for (File configFile : configFiles) {
			try {
				logger.info("Loading:" + configFile.getAbsolutePath());
				String configData = FileUtil.readAsString(configFile.getAbsolutePath()).trim();
				logSpaceAdaptor.importConfig(configData, true, false);
				logger.info("Loading:" + configFile.getAbsolutePath());
			} catch (Throwable t) {
				logger.warn("Failed to load config:" + configFile.getName(), t);
			}
		}
	}



	private String loadAppBGColor(String appName) {
		return FileUtil.readAsString(srcRoot + "/" + appName + "/bgcolour.txt").trim();
	}
	String listApps(String uid, String userApps, LogSpace logSpaceAdaptor) {
		StringBuilder result = new StringBuilder();
		
		if (userApps == null) userApps = "";
		Set<String> userAppsSet = new HashSet<String>();
		String[] split = userApps.split(",");
		for (String appItem : split) {
			appItem = appItem.trim();
			if (appItem.length() > 0) userAppsSet.add(appItem);
		}
		
		File dir = new File(srcRoot);
		File[] listDeployedFiles = dir.listFiles(new FileFilter() {
			public boolean accept(File pathname) {
				if (pathname.getAbsolutePath().contains("App-")) {
					return (new File(pathname.getAbsolutePath(), "DEPLOYED")).exists();
				}
				return false;
			}
		});
		List<String> deployedAppsList = new ArrayList<String>();
		if (listDeployedFiles != null) {
			for (File deployedDir : listDeployedFiles) {
				if (deployedDir.isDirectory()) {
					deployedAppsList.add(deployedDir.getName());
				}
			}
		}
		
		// only keep deployed apps which the user has access to and are deployed
		boolean isAccessToLogScape = StringUtil.containsIgnoreCase(userApps, "logscape");
		if (!userApps.equals("*")) deployedAppsList.retainAll(userAppsSet);
		
		Collections.sort(deployedAppsList);
		
		if (userApps.equals("*") || isAccessToLogScape) deployedAppsList.add(0, "Logscape");
		//if (uid.contains("admin") || isAccessToLogScape) deployedAppsList.add("LogScape-NEW");
		
		// return a , string
		for (String deployedApp : deployedAppsList) {
				result.append(deployedApp).append(",");
		}
		
		
		return result.toString();		
	}
	

	public String loadAppMenuXML(String appName) {
		try {
		return FileUtil.readAsString(srcRoot + "/" + appName + "/menu.xml").trim();
		} catch (Throwable t) {
			throw new RuntimeException("App:" + appName + " not deployed");
		}
	}

	public String loadAppLogoURL(String appName) {
		return "/logscape-apps/" + appName + "/logo.png";
	}

	public String loadAppTitleXML(String appName) {
		return FileUtil.readAsString(srcRoot + "/" + appName + "/title.xml").trim();
	}

	public String loadAppLinkMenuXML(String appName) {
		return FileUtil.readAsString(srcRoot + "/" + appName + "/linkMenu.xml").trim();
	}
	public String loadAppStartPageData(String appName) {
		return FileUtil.readAsString(srcRoot + "/" + appName + "/startPage.txt").trim();
	}

	public void unloadApp(String appName, LogSpace logSpaceAdaptor) {
		logger.info("Undeploy App:" + appName);
		File[] configFiles = listConfigFilesForApp(appName);
		
		if (configFiles == null) {
			logger.info("Didnt find config files in:" + new File(appName).getAbsolutePath());
			return;
		}
		for (File configFile : configFiles) {
			try {
				List<File> configFilesSameName = findConfigFilesWithSameName(srcRoot, configFile.getName());
				if (configFilesSameName.size() > 1) {
					logger.info("Cannot unload ConfigFile:" + configFile.getName() + " As there is more that one instance");
					continue;
				}
				logger.info("UN-Loading:" + configFile.getAbsolutePath());
				String configData = FileUtil.readAsString(configFile.getAbsolutePath()).trim();
				logSpaceAdaptor.removeConfig(configData);
			} catch (Throwable t) {
				logger.warn("Failed to load config:" + configFile.getName(), t);
			}
		}
		File dir = new File(this.dstRoot + "/" + appName);
		int deleteDir = FileUtil.deleteDir(dir);
		logger.info("Deleted FileCount:" + deleteDir + " from:" + dir.getAbsolutePath());

	}

	List<File> findConfigFilesWithSameName(String srcRoot, final String name) {
		File[] appDirs = new File(srcRoot).listFiles(new FileFilter(){
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().contains("App-") && pathname.isDirectory();
			}
		});
		List<File> results = new ArrayList<File>();
		for (File file : appDirs) {
			String[] list = file.list(new FilenameFilter() {
				public boolean accept(File dir, String thisname) {
					return thisname.equals(name);
				}
			});
			for (String found : list) {
				results.add(new File(found));
			}
		}
		return results;
	}
	File[] listConfigFilesForApp(String appName) {
		String pathname = srcRoot + "/" + appName;
		logger.info("Loading AppConfig XML for App:" + new File(pathname).getAbsolutePath());
		
		File[] configFiles = new File(pathname).listFiles(new FileFilter() {

			public boolean accept(File pathname) {
				return pathname.getName().endsWith(".config");
			}
		});
		return configFiles;
	}
	

}
