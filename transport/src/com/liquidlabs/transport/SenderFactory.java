package com.liquidlabs.transport;

import java.io.IOException;
import com.liquidlabs.common.net.URI;

import com.liquidlabs.common.LifeCycle;

public interface SenderFactory extends LifeCycle {

	Sender getSender(URI uri, boolean logIt, boolean remoteOnly, String context) throws IOException, InterruptedException;

	void returnSender(URI uri, Sender sender, boolean discardSender);

	int currentLiveConnectionCount();

	String dumpStats();

}
