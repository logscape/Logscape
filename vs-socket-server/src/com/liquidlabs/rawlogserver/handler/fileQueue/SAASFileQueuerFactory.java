package com.liquidlabs.rawlogserver.handler.fileQueue;

import com.logscape.meter.MeterService;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/10/2014
 * Time: 10:19
 * To change this template use File | Settings | File Templates.
 */
public class SAASFileQueuerFactory implements FileQueuerFactory {

    private MeterService meterService;
    protected static final String FORMAT_SS_LOG = "%s/%s.log";
    Map<String, FileQueuer> queuers = new ConcurrentHashMap<String, FileQueuer>();

    public  SAASFileQueuerFactory(MeterService meterService) {
        this.meterService = meterService;
    }
    @Override
    public FileQueuer create(String sourceHost, String file, String timestamp) {
        String hostAndfile = sourceHost + file;

        FileQueuer fileQueuer = queuers.get(hostAndfile);
        if (fileQueuer != null) return fileQueuer;
        fileQueuer = new SAASFileQueuer(meterService, sourceHost, file, false);
        queuers.put(hostAndfile, fileQueuer);
        return fileQueuer;

    }
    @Override
    public FileQueuer create(String sourceHost, String destinationFile, boolean isMLine, String[] tags, String timestamp) {
        String file = timestamp;

        for (String tag : tags) {
            file += "/" + tag + "/";
        }
        file += "/" + destinationFile;


//        String file = String.format(FORMAT_SS_LOG, timestamp, tags);
        String hostAndfile = sourceHost + file.replace("//","/");

        FileQueuer fileQueuer = queuers.get(hostAndfile);
        if (fileQueuer != null) return fileQueuer;
        fileQueuer = new SAASFileQueuer(meterService, sourceHost, file, isMLine);
        queuers.put(hostAndfile, fileQueuer);
        return fileQueuer;
    }
}
