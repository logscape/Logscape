package com.liquidlabs.log;

import java.util.Date;
import java.util.List;

import com.liquidlabs.admin.User;
import com.liquidlabs.log.index.FileItem;
import com.liquidlabs.log.index.LogFile;
import com.liquidlabs.log.streamer.FileStreamer;
import com.liquidlabs.vso.work.InvokableUI;

public interface AgentLogServiceAdmin extends InvokableUI, FileStreamer {
	public static String NAME = AgentLogServiceAdmin.class.getSimpleName();

	/**
	 * List those files being Tailed
	 * @param filter
	 * @param atTime TODO
	 * @param excludeFileFilter TODO
	 * @param user TODO
	 * @param hostSubFilter TODO
	 * @return
	 */
	List<LogFile> listFiles(String filter, int limit, Date atTime, String excludeFileFilter, User user, String hostSubFilter);

	List<FileItem> listDirContents(String currentDir, String excludes, String fileMask, boolean recurse);
	
	void assign(String filename, String fieldSetId);

	List<String> listHosts(User user, Date atTime);

}
