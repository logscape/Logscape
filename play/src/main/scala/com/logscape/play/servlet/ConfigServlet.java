package com.logscape.play.servlet;

import com.liquidlabs.admin.AdminSpace;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.services.ServicesLookup;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.resource.ResourceSpace;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.log4j.Logger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

import static java.lang.String.format;


public class ConfigServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(ConfigServlet.class);
    private LogSpace logSpace = null;
    private AdminSpace adminSpace;
    private ResourceSpace resourceSpace;
    private HashMap<String, ConfigAction> actionMap;

    public ConfigServlet() throws URISyntaxException, IOException {
        LOGGER.info("Created");

    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
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
    private void initLS() {
        try {
            if (logSpace == null) logSpace = ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD).getLogSpace();
            if (resourceSpace == null) resourceSpace = ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD).getResourceSpace();
            if (adminSpace == null) adminSpace = ServicesLookup.getInstance(VSOProperties.ports.DASHBOARD).getAdminSpace();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        initLS();
        ConfigAction action = determineAction(request);
        String filter = determineFilter(request);
        if (action == null) {

            writeResponse(response, HttpServletResponse.SC_EXPECTATION_FAILED);
        }

        if (action != null) {
            export(action.exportData(filter), action.name(),response);
        }
    }

    protected void doPost(HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
    	try {
            initLS();
	        ConfigAction configAction = determineAction(req);
	        if (configAction == null) {
                writeResponse(resp, HttpServletResponse.SC_EXPECTATION_FAILED);
                return;
	        }
	
	        if (configAction != null) {
                List<FileItem> items = new ServletFileUpload(new DiskFileItemFactory()).parseRequest(req);
                for (FileItem item : items) {
                    configAction.importData(new String(item.get()));
                    item.delete();
                }
	        }

            writeResponse(resp, HttpServletResponse.SC_OK);
    	} catch (Throwable t) {
    		LOGGER.error("ERROR:" + t.toString(), t);
            writeResponse(resp, HttpServletResponse.SC_EXPECTATION_FAILED);
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

    interface ConfigAction {
        void importData(String data) throws Exception;

		String exportData(String filter);

        String name();
    }

    class AllConfigAction implements ConfigAction {
        public void importData(String data) throws Exception {
            logSpace.importConfig(data, false, true);
            resourceSpace.importConfig(data);
            adminSpace.importConfig(data);
        }

        public String exportData(String filter) {
            return logSpace.exportConfig(filter) + "\n" + resourceSpace.exportConfig(filter) + "\n" + adminSpace.exportConfig(filter);
        }

        public String name() {
            return "all";
        }
    }
    class AllMConfigAction implements ConfigAction {
    	public void importData(String data) throws Exception {
    		logSpace.importConfig(data, true, true);
            resourceSpace.importConfig(data);
            adminSpace.importConfig(data);
        }
    	
    	public String exportData(String filter) {
    		return logSpace.exportConfig(filter) + "\n" + resourceSpace.exportConfig(filter) + "\n" + adminSpace.exportConfig(filter);
    	}
    	
    	public String name() {
    		return "all-merge";
    	}
    }
}
