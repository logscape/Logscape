package com.liquidlabs.log.jreport;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 04/07/2013
 * Time: 14:45
 * To change this template use File | Settings | File Templates.
 */
public class PrintJobTest {



    //executePrintJob
    @Test
    public void shouldGetATempFileWithStuff() {
        JReportRunner ws = new JReportRunner(null   , null, 10, "", "Home", "admin");
        try {
            File home = ws.executePrintJob("Workspace","Home", "admin");
            System.out.println("File:" + home);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }
}
