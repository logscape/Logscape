package com.liquidlabs.dashboard.server;

import com.liquidlabs.common.FileHelper;
import com.liquidlabs.common.Unpacker;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.replicator.service.Uploader;
import com.liquidlabs.transport.proxy.ProxyFactoryImpl;
import org.apache.log4j.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;



public class UploadServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	Logger LOGGER = Logger.getLogger(UploadServlet.class);
	
	private File deployDir = new File(System.getProperty("vscape.upload.dir", "downloads"));
	private String agentAddress = System.getProperty("vs.agent.address");
	private ProxyFactoryImpl proxyFactory;
	
	public UploadServlet() throws URISyntaxException, IOException, NoSuchAlgorithmException {
	}

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        proxyFactory = (ProxyFactoryImpl) getServletContext().getAttribute("ProxyFactory");
    }

	
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doPost(req, resp);
	}
	
	protected void doPost(HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
		try {
			String[] filenames = req.getParameter("Filenames").split(",");
			if (filenames == null) return;
			boolean error = false;
			for (String filename : filenames) {
				
				if (filename.length() == 0) continue;
				File file = (File) req.getAttribute(filename);
				if (file == null) continue;
				
				try {
					
					if (req.getParameter("userZips") != null) {
						LOGGER.info("Received UPLOAD:" + filename);
						String path = req.getParameter("uploadPath");
						handleUserUpload(path, file, filename);
					}
					
					else if (req.getParameter("deployFile") != null && req.getParameter("deployFile").equals("true")) {
						LOGGER.info("Received Upload/DEPLOY:" + filename);
						LOGGER.info("Unpacking and Replicating:" + filename);
						handleBundleZipUpload(resp, filename, file);
					} else {
						LOGGER.info("Received File/Replicating:" + filename);
						String path = req.getParameter("path");
						File theFile = getFileForPath(path, file, filename);
						System.out.println("About to publish: " + theFile.getPath() + " file exists?" + theFile.exists() + " size = " + theFile.length());
						findUploader().deployFile(theFile.getPath(), true);
					}
				} catch (Throwable e) {
					error = true;
					LOGGER.error("Error with File:" + e.getMessage(), e);
				}
			}
			if (error) 
				writeResponse(resp, HttpServletResponse.SC_EXPECTATION_FAILED);
			else 
				writeResponse(resp, HttpServletResponse.SC_OK);
		} catch (Throwable t) {
			writeResponse(resp, HttpServletResponse.SC_EXPECTATION_FAILED);
		}
	}

	private void handleUserUpload(String path, File file, String filename) {
		try {
			FileUtil.mkdir(path);
			if (filename.endsWith(".zip")) {
				// upzip to this dir
				new Unpacker().unpack(new File(path), file);
			} else if (filename.endsWith(".tar.gz") || filename.endsWith(".tgz")) {
				FileUtil.extractTGZ(path, file);
			} else if (filename.endsWith(".gz")) {
				FileUtil.extractGzip(path, file, filename);
			} else if (filename.endsWith(".bz2")) {
				FileUtil.extractBZ2(path, file, filename);
			} else {
				FileUtil.copyFile(file, new File(path, filename));
			}
		} catch (Throwable t) {
			LOGGER.warn("Failed to unzip:" + file.getAbsolutePath(), t);
		}
		
	}

	private void handleBundleZipUpload(final HttpServletResponse resp, String filename, File file) throws Exception {
		if (!Unpacker.isValidZip(file)) {
			LOGGER.error("Zip is invalid:" + filename + " file:" + file.getPath());
			throw new RuntimeException("ZIP is invalid:" + filename);
		}
		File theFile = getFileForPath(deployDir.getName(), file, filename);
		findUploader().deployBundle(theFile.getPath());
	}

	private Uploader findUploader() {
		return proxyFactory.getRemoteService(Uploader.NAME, Uploader.class, agentAddress);
	}
	
	
	private File getFileForPath(String path, File file, String fileName) {
		if (path == null) path = "./";
		FileHelper.mkAndList(path);
		
		File theFile = new File(path, fileName);
		if (theFile.exists())
			theFile.delete();
        try {
            FileUtil.renameTo(file, theFile);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return theFile;
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
