package com.liquidlabs.log.util.notifier;

import java.io.File;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 21/10/2015
 * Time: 08:46
 * To change this template use File | Settings | File Templates.
 */
public interface PathNotifier {
    Set<File> getDirs();


    void setCacheTimeSensiSeconds(int seconds);
}
