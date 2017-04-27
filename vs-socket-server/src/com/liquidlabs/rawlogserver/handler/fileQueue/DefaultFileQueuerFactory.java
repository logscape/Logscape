package com.liquidlabs.rawlogserver.handler.fileQueue;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/10/2014
 * Time: 10:18
 * To change this template use File | Settings | File Templates.
 */
public class DefaultFileQueuerFactory implements FileQueuerFactory {
    @Override
    public FileQueuer create(String sourceHost, String destinationFile, boolean isMLine, String[] tags, String timestamp) {
        return new DefaultFileQueuer(destinationFile, isMLine);
    }

    @Override
    public FileQueuer create(String sourceHost, String destinationFile, String timestamp) {
        return new DefaultFileQueuer(destinationFile, false);
    }
}
