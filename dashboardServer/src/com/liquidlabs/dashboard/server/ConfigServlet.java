package com.liquidlabs.dashboard.server;

import com.liquidlabs.log.space.LogSpace;
import org.apache.log4j.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import static java.lang.String.format;


public class ConfigServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(ConfigServlet.class);
    private LogSpace logSpace;
    private HashMap<String, ConfigAction> actionMap;

    public ConfigServlet() throws URISyntaxException, IOException {

    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        logSpace = (LogSpace) getServletContext().getAttribute(LogSpace.NAME);
        initActionMap();
    }

    private void initActionMap() {
        actionMap = new HashMap<String, ConfigAction>();
        actionMap.put(null, new AllConfigAction());
        actionMap.put("", new AllConfigAction());
        
        put(actionMap, new AllConfigAction());
        put(actionMap, new AllMConfigAction());
    }


    private void put(HashMap<String, ConfigAction> map, ConfigAction action) {
		map.put(action.name(), action);
		
	}

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ConfigAction action = determineAction(request);
        String filter = determineFilter(request);
        if (action == null) {
            doError(response);
        }

        if (action != null) {
            export(action.exportData(filter), action.name(),response);
        }
    }

    protected void doPost(HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
    	try {
	        ConfigAction configAction = determineAction(req);
	        if (configAction == null) {
	            doError(resp);
	        }
	
	        if (configAction != null) {
	            importConfiguration(req, resp, configAction);
	        }
	        
    	} catch (Throwable t) {
    		LOGGER.error("ERROR:" + t.toString(), t);
    	}

    }

    private void importConfiguration(HttpServletRequest req, HttpServletResponse resp, ConfigAction configAction) {

        String[] filenames = req.getParameter("Filenames").split(",");
        if (filenames != null) {
            String fileName = filenames[0];
            LOGGER.info("Importing: " + fileName);
            File file = (File) req.getAttribute(fileName);
            if (file != null) {
                try {
                    configAction.importData(dataToImport(file));
                } catch (Exception e) {
                    handleBadImport(e.getMessage(), resp);
                }
                file.delete();
            }
        }
    }

    private void handleBadImport(String message, HttpServletResponse resp) {
        try {
            resp.getOutputStream().write(format("Import failed. Possibly due to bad data: %s",message).getBytes());
            resp.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
            resp.getOutputStream().close();
        } catch (IOException e) {

        }
    }


    private void doError(HttpServletResponse response) {
        LOGGER.error("DoError!");
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    }

   private ConfigAction determineAction(HttpServletRequest request) {
        String pathInfo = request.getPathInfo();
        if (pathInfo != null) {
            String[] split = pathInfo.split("/");
            if (split.length == 2) {
            	return actionMap.get(split[split.length - 1].toLowerCase());
            }
            return actionMap.get(split[split.length - 2].toLowerCase());
        }
        return actionMap.get(null);
    }
   private String determineFilter(HttpServletRequest request) {
	   String pathInfo = request.getPathInfo();
	   if (pathInfo != null) {
		   String[] split = pathInfo.split("/");
		   if (split.length == 2) {
			   return "";
		   } else {
			   return split[split.length - 1];
		   }
	   }
	   return "";
   }

    private void export(String data, String name, HttpServletResponse response) throws IOException {
        response.setHeader("Content-disposition",
                format("attachment; filename=logscape-%s.config", name));
        response.setContentType("application/logscape");
        ServletOutputStream outputStream = response.getOutputStream();
        outputStream.write(data.getBytes());
        response.setStatus(HttpServletResponse.SC_OK);
        outputStream.close();

    }


    private String dataToImport(File file) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(file);
        byte[] bytes = new byte[fileInputStream.available()];
        fileInputStream.read(bytes);
        fileInputStream.close();
        return new String(bytes);
    }


    interface ConfigAction {
        void importData(String data) throws Exception;

		String exportData(String filter);

        String name();
    }

    class AllConfigAction implements ConfigAction {
        public void importData(String data) throws Exception {
            logSpace.importConfig(data, false, true);
        }

        public String exportData(String filter) {
            return logSpace.exportConfig(filter);
        }

        public String name() {
            return "all";
        }
    }
    class AllMConfigAction implements ConfigAction {
    	public void importData(String data) throws Exception {
    		logSpace.importConfig(data, true, true);
    	}
    	
    	public String exportData(String filter) {
    		return logSpace.exportConfig(filter);
    	}
    	
    	public String name() {
    		return "all-merge";
    	}
    }
}
