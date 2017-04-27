package com.liquidlabs.rawlogserver.handler.fileQueue;

import com.liquidlabs.rawlogserver.handler.ContentFilteringLoggingHandler;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 24/10/2014
 * Time: 10:11
 * To change this template use File | Settings | File Templates.
 */
public interface FileQueuer extends Runnable {

    // CTOR (String destinationFile, boolean isMLine);
    void append(String lineData);

    void flushMLine(ContentFilteringLoggingHandler.TypeMatcher lastType);

    void flush();

    void setTokenAndTag(String sourcehost, String token, String tag);
}
