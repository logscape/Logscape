package com.liquidlabs.logserver;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.raf.BreakRule;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import junit.framework.Assert;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;

public class LogServerMiniIntegrationTest {


    @Test
    public void shouldFireUpServerAndBeAbleToStoreLogs() throws Exception {


        System.setProperty("allow.coloc", "true");

        int lineLimit = 100;

        //"C:\\work\\logs\\centris\\centris2\\logscape_data\\TEST_1.log"
        File logserverFileSrc = new File("build/integ/someFile.log");
        logserverFileSrc.delete();
        new File("build/integ").mkdirs();
        logserverFileSrc.createNewFile();
        String eol = System.getProperty("line.separator");
        PrintWriter writer = new PrintWriter(logserverFileSrc);
        for (int i = 0; i < lineLimit; i++) {
            writer.print(new String(("line:" + i) + "" + eol));
        }
        writer.close();

        File logserverFileDest = new File(String.format("./build/_SERVER_/HOST/%s", logserverFileSrc.getCanonicalPath().replaceAll("\\:\\\\", "/")));
        logserverFileDest.delete();

        System.out.println("SrcFile:" + logserverFileSrc.getAbsolutePath());
        System.out.println("DesFile:" + logserverFileDest.getAbsolutePath());

        ProxyFactoryImpl proxyFactory = new ProxyFactoryImpl(NetworkUtils.determinePort(6168), ExecutorService.newDynamicThreadPool("test","logserver"), "miniTest");
        proxyFactory.start();

        LogServerImpl logServer = new LogServerImpl();
        logServer.rootDirectory = "./build/_SERVER_";
        FileUtil.deleteDir(new File("./build/_SERVER_"));
        proxyFactory.registerMethodReceiver(LogServer.NAME, logServer);

        LogServer remoteLogService =  proxyFactory.getRemoteService(LogServer.NAME, LogServer.class, proxyFactory.getAddress().toString());

        LogMessage msg = new LogMessage("HOST", logserverFileSrc.getAbsolutePath(), DateTimeUtils.currentTimeMillis(), 100, 0);
        RAF raf = RafFactory.getRaf(logserverFileSrc.getAbsolutePath(), BreakRule.Rule.SingleLine.name());
        int lines = 0;
        String line = "";
        while ( (line = raf.readLine()) != null) {
            msg.addMessage(System.currentTimeMillis(), line, raf.getFilePointer());
            msg.flush(remoteLogService, false, raf.getFilePointer());
            System.out.println(line);
            lines++;
        }
        msg.flush(remoteLogService, true, raf.getFilePointer());

        System.out.println("Lines:" + lines);


//		msg.addMessage("line2");
//		msg.addMessage("line3");

//		msg.addMessage("line4");
//		msg.addMessage("line5");
//		msg.addMessage("line6");
//		msg.flush(remoteLogService, true);
        Thread.sleep(100);

        System.out.println("DestFile:" + logserverFileDest.getAbsoluteFile());
        Assert.assertTrue("File Doesnt exist:" + logserverFileDest.getAbsolutePath(), logserverFileDest.exists());

        Assert.assertEquals(lineLimit, FileUtil.countLines(logserverFileDest)[1]);

        Assert.assertTrue(new File("build/_SERVER_/HOST/datasource.properties").exists());


        int lineCount = msg.getLineCount(logServer);

        System.out.println("LineCount:" + lineCount);
        Assert.assertEquals(lineLimit, lineCount);

    }
}
