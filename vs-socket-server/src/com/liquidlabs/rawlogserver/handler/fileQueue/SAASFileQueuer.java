package com.liquidlabs.rawlogserver.handler.fileQueue;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.rawlogserver.handler.ContentFilteringLoggingHandler;
import com.logscape.meter.Meter;
import com.logscape.meter.MeterService;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/10/2014
 * Time: 10:20
 * To change this template use File | Settings | File Templates.
 */
public class SAASFileQueuer implements FileQueuer {
    private final static Logger LOGGER = Logger.getLogger(SAASFileQueuer.class);
    private final MeterService meterService;
    private String sourceHost;
    private final String destinationFile;
    private final boolean mLine;
    private long lastDisableTime;

    public SAASFileQueuer(MeterService meterService, String sourceHost, String destinationFile, boolean mLine) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("Created:" + sourceHost + "/" + destinationFile);

        this.meterService = meterService;
        this.sourceHost = sourceHost;
        this.destinationFile = destinationFile;
        this.mLine = mLine;
    }

    private String token = "";
    private String tag = "";
    public void setTokenAndTag(String sourcehost, String token, String tag) {
        this.token = token;
        this.tag = tag;
        this.sourceHost = sourcehost;
    }


    static boolean isMetering = Boolean.getBoolean("metering.enabled");

    @Override
    public void append(String lineData) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("append");
        String destFile = destinationFile;
        String destFileSource = destinationFile;
        if (tag.length() > 0) {
//            destFile= new File(destinationFile).getParent();
            String tagPath = "";
            String[] tags = tag.split(",");
            for (String tagg : tags) {
                String newTag = "/_" + tagg + "_/";
                tagPath += newTag;
                if (destFileSource.contains(newTag)) destFileSource = destFileSource.replace(newTag,"");
            }
            // insert the tag path at the start of the tags
            int firstTag = destFileSource.indexOf("/_");
            if (firstTag != -1) {
                destFile = destFileSource.substring(0, firstTag);
                destFile += tagPath;
                destFile += destFileSource.substring(firstTag);

            } else {
                destFile = tagPath + destFileSource;
            }
            String now = getDateTime();

            destFile = destFile.replace(now,"");
            destFile = destFile.replace("//","/");
            destFile = now + "/" + destFile;

        }
        lineData = Meter.removeTagAndToken(lineData);

        // still disabled
        if (isDisabled()) {
            return;

        }

        // if not token then grab one and try and proceed
        if (isMetering && (token == null || token.length() == 0)) {
            token = meterService.getToken(sourceHost, destFile);
            // should probably disable it here
            if (token != null && token.length() == 0) {
                lastDisableTime = System.currentTimeMillis();
                return;
            }
        }

        try {
            meterService.handle(token, sourceHost, destFile, lineData);
        } catch (Throwable t) {
            LOGGER.warn("QUEUE-Error/Stalling:" + t.toString() ,t);
            try {
                Thread.sleep(20 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    protected String getDateTime() {
        return DateUtil.shortDateFormat.print(DateTimeUtils.currentTimeMillis());
    }
    public boolean isDisabled() {
        return lastDisableTime > System.currentTimeMillis() - DateUtil.SECOND * 30;
    }

    @Override
    public void flushMLine(ContentFilteringLoggingHandler.TypeMatcher lastType) {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("flush");
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void flush() {
        if (LOGGER.isDebugEnabled()) LOGGER.debug("flush");
    }

    @Override
    public void run() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
