// ========================================================================
// Copyright 1996-2005 Mort Bay Consulting Pty. Ltd.
// ------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at 
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ========================================================================
package org.mortbay.servlet;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;
import org.eclipse.jetty.util.MultiMap;
import org.eclipse.jetty.util.log.Log;

import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.concurrent.NamingThreadFactory;

public class LLabsMultiPartFilter implements Filter {
	Logger logger = Logger.getLogger(LLabsMultiPartFilter.class);
	private FileItemFactory fileItemfactory;
	private ServletFileUpload uploadHandler;
	private ScheduledExecutorService scheduler;

	/*
	 * --------------------------------------------------------------------------
	 * -----
	 */
	/**
	 * @see javax.servlet.Filter#init(javax.servlet.FilterConfig)
	 */
	public void init(FilterConfig filterConfig) throws ServletException {
		logger.info("Initialised");
		System.out.println("Using MultiPartFilter:" + getClass().getName());
		fileItemfactory = new DiskFileItemFactory(10240, new File("."));
		uploadHandler = new ServletFileUpload(fileItemfactory);
		scheduler = ExecutorService.newScheduledThreadPool(1,new NamingThreadFactory("LLPartFilter"));
		
	}

	/*
	 * --------------------------------------------------------------------------
	 * -----
	 */
	/**
	 * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest,
	 *      javax.servlet.ServletResponse, javax.servlet.FilterChain)
	 */
	@SuppressWarnings("unchecked")
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

		logger.info("Received uploadRequest:" + request);
		if (Log.isDebugEnabled())
			Log.debug(getClass().getName() + ">>> Received Request");

		HttpServletRequest srequest = (HttpServletRequest) request;
		if (srequest.getContentType() == null || !srequest.getContentType().startsWith("multipart/form-data")) {
			chain.doFilter(request, response);
			return;
		}
		if (ServletFileUpload.isMultipartContent(srequest)){
			// Parse the request
			
			List<File> tempFiles = new ArrayList<File>();
			try {
				
				MultiMap params = new MultiMap();
				
				logger.info("Parsing Request");
				
				List<DiskFileItem> fileItems = uploadHandler.parseRequest(srequest);
				
				logger.info("Done Parsing Request, got FileItems:" + fileItems);
				
				
				StringBuilder filenames = new StringBuilder();
				
				for (final DiskFileItem diskFileItem : fileItems) {
					if (diskFileItem.getName() == null && diskFileItem.getFieldName() != null && diskFileItem.getFieldName().length() > 0) {
						params.put(diskFileItem.getFieldName(), diskFileItem.getString());
						continue;
					}
					if (diskFileItem.getName() == null) continue;
					final File tempFile = File.createTempFile("upload"+ diskFileItem.getName(), ".tmp");
					tempFiles.add(tempFile);
					
					diskFileItem.write(tempFile);
					request.setAttribute(diskFileItem.getName(), tempFile);
					filenames.append(diskFileItem.getName()).append(",");
				}
				params.put("Filenames", filenames.toString());
			    logger.info("passing on filter");
				chain.doFilter(new Wrapper(srequest, params), response);
				
				for (final DiskFileItem diskFileItem : fileItems) {
					diskFileItem.delete();
				}
			} catch (FileUploadException e) {
				e.printStackTrace();
				Log.warn(getClass().getName() + "MPartRequest, ERROR:" + e.getMessage());
				logger.error("FileUpload Failed", e);
				writeResponse((HttpServletResponse) response, HttpServletResponse.SC_EXPECTATION_FAILED);
			} catch (Throwable e) {
				logger.error(e.getMessage(), e);
				Log.warn(getClass().getName() + "MPartRequest, ERROR:" + e.getMessage());
				e.printStackTrace();
				writeResponse((HttpServletResponse) response, HttpServletResponse.SC_EXPECTATION_FAILED);
			} finally {
				for (File tempFile : tempFiles) {
					try {
						tempFile.delete();
					} catch (Throwable t){}
				}
			}
		}
		
	}


	/**
	 * @see javax.servlet.Filter#destroy()
	 */
	public void destroy() {
	}

	private static class Wrapper extends HttpServletRequestWrapper {
		String encoding = "UTF-8";
		MultiMap map;

		/**
		 * Constructor.
		 * 
		 * @param request
		 */
		public Wrapper(HttpServletRequest request, MultiMap map) {
			super(request);
			this.map = map;
		}

		/**
		 * @see javax.servlet.ServletRequest#getContentLength()
		 */
		public int getContentLength() {
			return 0;
		}

		/**
		 * @see javax.servlet.ServletRequest#getParameter(java.lang.String)
		 */
		public String getParameter(String name) {
			Object o = map.get(name);
			if (o instanceof byte[]) {
				try {
					String s = new String((byte[]) o, encoding);
					return s;
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (o instanceof String)
				return (String) o;
			return null;
		}

		/**
		 * @see javax.servlet.ServletRequest#getParameterMap()
		 */
		@SuppressWarnings("unchecked")
		public Map getParameterMap() {
			return map;
		}

		/**
		 * @see javax.servlet.ServletRequest#getParameterNames()
		 */
		@SuppressWarnings("unchecked")
		public Enumeration getParameterNames() {
			return Collections.enumeration(map.keySet());
		}

		/**
		 * @see javax.servlet.ServletRequest#getParameterValues(java.lang.String)
		 */
		@SuppressWarnings("unchecked")
		public String[] getParameterValues(String name) {
			List l = map.getValues(name);
			if (l == null || l.size() == 0)
				return new String[0];
			String[] v = new String[l.size()];
			for (int i = 0; i < l.size(); i++) {
				Object o = l.get(i);
				if (o instanceof byte[]) {
					try {
						v[i] = new String((byte[]) o, encoding);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if (o instanceof String)
					v[i] = (String) o;
			}
			return v;
		}
		/**
		 * @see javax.servlet.ServletRequest#setCharacterEncoding(java.lang.String)
		 */
		public void setCharacterEncoding(String enc) throws UnsupportedEncodingException {
			encoding = enc;
		}
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
