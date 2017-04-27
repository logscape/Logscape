package com.liquidlabs.logserver;

import com.liquidlabs.common.file.FileUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TailerIntegrationForWindowsFilenamesTest {



    @Test
    public void shouldWriteFlushAndWriteResult() throws Exception {

        File serverDir = new File("build/W_F_W/LogServer");
        FileUtil.deleteDir(serverDir);

        LogServerImpl logServer = new LogServerImpl();
        logServer.setRootDir(serverDir.getPath());
        LogMessage logMessage = new LogMessage("neil-vm", WIN_TEST_LOG, 0,0, 0);
        logMessage.addMessage(System.currentTimeMillis(), "line 1", 10);
        logMessage.addMessage(System.currentTimeMillis(), "line 2", 20);
        logMessage.addMessage(System.currentTimeMillis(), "line 3", 30);
        logMessage.flush(logServer, true, 100);
        logMessage.addMessage(System.currentTimeMillis(), "line 4", 10);
        logMessage.addMessage(System.currentTimeMillis(), "line 5", 20);
        logMessage.addMessage(System.currentTimeMillis(), "line 6", 30);
        logMessage.flush(logServer, true, 100);



        int startLine = logServer.getStartLine("neil-vm"+WIN_TEST_LOG, "neil-vm",WIN_TEST_LOG);
        System.out.println("Start:" + startLine);

        Assert.assertEquals(6, startLine);
    }

    private static final String WIN_TEST_LOG = "C:\\test\\winInt.log";

    @Test
    public void shouldForwardForwardAndResult() throws Exception {

        File serverDir = new File("build/winIntTest/LogServer");
        FileUtil.deleteDir(serverDir);

        System.setProperty("log4j.debug", "true");


        LogServerImpl logServer = new LogServerImpl();
        logServer.setRootDir(serverDir.getPath());
        LogMessage logMessage = new LogMessage("neil-vm", WIN_TEST_LOG, 0,0, 0);
        logMessage.addMessage(System.currentTimeMillis(), "line 1", 10);
        logMessage.addMessage(System.currentTimeMillis(), "line 2", 20);
        logMessage.addMessage(System.currentTimeMillis(), "line 3", 30);
        logMessage.flush(logServer, true, 0);


        int startLine = logServer.getStartLine("neil-vm"+WIN_TEST_LOG, "neil-vm",WIN_TEST_LOG);
        System.out.println("Start:" + startLine);

        Assert.assertEquals(3, startLine);
    }
}
