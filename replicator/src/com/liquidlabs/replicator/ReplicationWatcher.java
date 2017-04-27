package com.liquidlabs.replicator;


/**
 * Watches a given set of directories for file creation, file deletion and directory creation/deletion.
 * When an event is detected these changes are translated into an RFS event so the update propogates
 *
 */
public class ReplicationWatcher {
	
	// listRootFile
	// visit(directory)
	// isFileNew(Filename, size, updated, isDirectory)
	// isFileMissing(filename, isDirectory);

}
