package com.logscape.play.deployment;

import com.liquidlabs.replicator.service.Uploader;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import com.liquidlabs.vso.VSOProperties;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.List;


public class UploadServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	Logger LOGGER = Logger.getLogger(UploadServlet.class);



    private File deployDir;
	private String agentAddress = System.getProperty("vs.agent.address");
	private ProxyFactoryImpl proxyFactory;
	
	public UploadServlet() throws URISyntaxException, IOException, NoSuchAlgorithmException {
        // IDE TEST MODE - VSOProperties.setRootDir("/Volumes/Media/LOGSCAPE/TRUNK/LogScape/master/build.run/logscape/");
        deployDir = new File(VSOProperties.getDownloadsDir());
        LOGGER.info("Created");
	}

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

    }

	
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}
	
	protected void doPost(HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		try {
			getProxyFactory();

			ServletInputStream inputStream = req.getInputStream();
            List<FileItem> items = new ServletFileUpload(new DiskFileItemFactory()).parseRequest(req);

			for (FileItem file : items) {
				
				try {

                    String filename = new File(file.getName()).getName();
                    File theFile = new File(deployDir + "/" +filename);
                    LOGGER.info("About to publish: " + file.toString() + " Artifact:" + theFile.getAbsolutePath());
                    theFile.mkdirs();
                    if (theFile.exists()) theFile.delete();
                    theFile.createNewFile();
                    file.write(theFile);
                    LOGGER.info("Pushing:" + theFile.getPath());
                    findUploader().deployFile(theFile.getPath(), true);
				} catch (Throwable e) {
					LOGGER.error("Error with File:" + e.getMessage() + " file:" + deployDir + "/" + file.getName(), e);
				}
			}
				writeResponse(resp, HttpServletResponse.SC_OK);
		} catch (Throwable t) {
            LOGGER.error("Upload failed:" + t);
            t.printStackTrace();
			writeResponse(resp, HttpServletResponse.SC_EXPECTATION_FAILED);
		}
	}

	private void getProxyFactory() {
		if (proxyFactory == null) {
            proxyFactory = (ProxyFactoryImpl) getServletContext().getAttribute("ProxyFactory");
        }
	}


	private Uploader findUploader() {
		return proxyFactory.getRemoteService(Uploader.NAME, Uploader.class, agentAddress);
	}
	
	private void writeResponse(HttpServletResponse resp, int status) {
		resp.setContentType("text/html");
		try {
			OutputStreamWriter outputStream = new OutputStreamWriter(resp.getOutputStream());
			resp.setStatus(status);
			outputStream.close();
		}catch(IOException e){}
	}
}
