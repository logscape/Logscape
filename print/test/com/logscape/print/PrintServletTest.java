package com.logscape.print;

import com.logscape.PrintServlet;
import static junit.framework.Assert.*;

import org.apache.commons.httpclient.methods.GetMethod;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 03/07/2013
 * Time: 12:41
 * To change this template use File | Settings | File Templates.
 */
public class PrintServletTest {

    @Test
    public void shouldGetPhantomLinux64() throws Exception {
        //new GetMethod("http://10.28.0.193:11054/C/Logscape 2.0/logscape/work/agent.log.html?from=1&proxied=true");
        //new GetMethod("http://10.28.0.193:11054/opt%20/agent.log.html?from=1&proxied=true");

        System.setProperty("os.name","Linux");
        System.setProperty("os.arch","amd64");
        assertTrue(PrintServlet.isLinux64());
        assertFalse(PrintServlet.isLinux32());
        assertFalse(PrintServlet.isOSX());
        assertFalse(PrintServlet.isWindows());

    }
    @Test
    public void shouldGetPhantomWindows() throws Exception {

        System.setProperty("os.name","Windows 7");
        System.setProperty("os.arch","amd64");
        assertFalse(PrintServlet.isLinux64());
        assertFalse(PrintServlet.isLinux32());
        assertFalse(PrintServlet.isOSX());
        assertTrue(PrintServlet.isWindows());
    }

}
