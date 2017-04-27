package com.liquidlabs.logserver;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class LogMessageTest {

    @Test
    public void testShouldWriteDataInSequence() throws Exception {
        LogMessage logMessage = new LogMessage("build", "/some/test/file.log", 1, 1, 0);
        DateTime dateTime = new DateTime().minusHours(1);

        logMessage.addMessage(dateTime.getMillis(), "hello3", 10);
        logMessage.addMessage(dateTime.minusMinutes(1).getMillis(), "hello2", 20);
        logMessage.addMessage(dateTime.minusMinutes(2).getMillis(), "hello1", 30);
        final StringBuilder sb = new StringBuilder();
        LogServerImpl logServerImpl = new LogServerImpl() {
            @Override
            public int handle(String canonicalFileWithSrcHost, LogMessage msg) {
                sb.append(msg.message.toString());
                return 1;
            }
        };
        logMessage.flush(logServerImpl, true, 0);
        System.err.println("");
        Assert.assertTrue(sb.toString().indexOf("hello1") < sb.toString().indexOf("hello2") );
    }

    @Test
    public void testShouldWriteFile() throws Exception {
        LogMessage logMessage = new LogMessage("build", "/some/test/file.log", 1, 1, 0);
        logMessage.addMessage(System.currentTimeMillis(), "hello1", 10);
        LogServerImpl logServerImpl = new LogServerImpl();
        logServerImpl.handle("", logMessage);

        File file = new File("build/some/test/file.log");
        Assert.assertTrue(file.exists());
    }
}
