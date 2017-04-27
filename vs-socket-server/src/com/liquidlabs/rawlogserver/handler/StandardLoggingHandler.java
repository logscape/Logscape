package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.rawlogserver.handler.fileQueue.DefaultFileQueuer;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import java.io.File;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * Outputs and timestamps the data. Filename is basd upon the incoming hostname
 *
 */
public class StandardLoggingHandler extends BaseLoggingHandler implements StreamHandler  {
	private final static Logger LOGGER = Logger.getLogger(StandardLoggingHandler.class);

	DefaultFileQueuer queuer = null;
	int schedulePeriod = Integer.getInteger("raw.server.fq.delay.secs", 3);

	private final ScheduledExecutorService sheduler;
	
	public StandardLoggingHandler(ScheduledExecutorService sheduler) {
		this.sheduler = sheduler;
		sheduler.scheduleAtFixedRate(new Runnable() {
			public void run() {
				if (queuer != null) queuer.flush();
			}
		}, schedulePeriod, schedulePeriod,  TimeUnit.SECONDS);
	}
	@Override
	public void handled(byte[] payload, String remoteAddress, String remoteHostname, String rootDir) {
		
		if (queuer == null) {
			try {
			String now = getNowTimeSting();
			String remoteRootDir = String.format(FORMAT_SS, rootDir, getHostPath(remoteHostname, remoteAddress));
			FileUtil.mkdir(remoteRootDir);
			
			File outFile = new File(String.format(FORMAT_SS_LOG, remoteRootDir, tag, now));
			queuer = new DefaultFileQueuer(outFile.getAbsolutePath(), false);
			} catch (Throwable t) {
				LOGGER.error("Failed to start:" + t.toString());
			}
		}

		try {
			if (isTimeStampingEnabled) {
				queuer.append(DateUtil.shortDateTimeFormat3.print(DateTimeUtils.currentTimeMillis()));
				queuer.append(SPACE);
				queuer.append(new String(payload).replaceAll("\n", "\n ").trim() + eol);
			} else {
				queuer.append(new String(payload));
			}
		} catch (Exception e) {
			LOGGER.warn("WriteFailed:" + remoteHostname + " e:" + e.toString(), e);
		} finally {
			try {
			} catch (Throwable t) {
			}
		}
	}
	public StreamHandler copy() {
		StandardLoggingHandler handler = new StandardLoggingHandler(sheduler);
		handler.setTimeStampingEnabled(this.isTimeStampingEnabled);
		return handler;
	}
	public void start() {
	}
	public void stop() {
	}
}
