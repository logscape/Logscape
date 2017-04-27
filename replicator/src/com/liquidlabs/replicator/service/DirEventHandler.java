package com.liquidlabs.replicator.service;

import java.io.File;


public interface DirEventHandler {

	void created(File file);
	
	void modified(File file);
	
	void deleted(File file);
	
	void stop();


}