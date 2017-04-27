package com.liquidlabs.rawlogserver.handler.fileQueue;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/10/2014
 * Time: 10:17
 * To change this template use File | Settings | File Templates.
 */
public interface FileQueuerFactory {

    FileQueuer create(String sourceHost, String destinationFile, String timestamp);
    FileQueuer create(String sourceHost, String destinationFile, boolean isMLine, String[] tags, String timestamp);
}
