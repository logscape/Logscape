/**
 * 
 */
package com.liquidlabs.vso.agent.process;

import java.io.InputStream;
import java.io.OutputStream;

public interface AProcess {
   OutputStream getOutputStream();
   InputStream getInputStream();
   InputStream getErrorStream();
   int waitFor() throws InterruptedException;
   int exitValue();
   void destroy();
   void quit();
   int pid();
}