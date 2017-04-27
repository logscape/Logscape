package com.liquidlabs.transport.netty;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.transport.TransportProperties;


/**
 * Scans for a boot.properties with client.port.retrict.property or if the system property is set - just returns true.
 * Intention is that any child/forked processes can pick up the property as well (i.e. aggspace)
 * @author neil
 *
 */
public class ClientPortRestrictedDetector {
	static final Logger LOGGER = Logger.getLogger(ClientPortRestrictedDetector.class);
	
	public Boolean isRestricted =  null;
	
	
	public boolean isBootPropertiesSetValueToTrue(){
		if (isRestricted == null) {
			isRestricted = isPropertyExisting(TransportProperties.VSO_CLIENT_PORT_RESTRICT, ".;..;../..;../../..", "boot.properties");
			
			if (isRestricted) {
				// force the networking to use OIO sockets
//				System.setProperty("tcp.use.oio.server", "true");
//				System.setProperty("tcp.use.oio.client", "true");
			}
		}
		return isRestricted;
	}
	public boolean isValueFound(String lookingFor, List<String> listOfData) {
		for (String string2 : listOfData) {
			if (string2.contains(lookingFor)) return true;
		}
		return false;
	}
	public List<File> findListOfExistingFiles(String path, String filename) {
		ArrayList<File> results = new ArrayList<File>();
		String[] pathParts = path.split(";");
		for (String pathBit : pathParts) {
			File file = new File(pathBit, filename);
			if (file.exists()) results.add(file);
		}
		
		return results;
	}
	public boolean isPropertyExisting(String property, String path, String filename) {
		String property2 = System.getProperty(property);
		if (property2 != null){
			if (property2.equals("true")) return true;
			else return false;
			
		}
		List<File> files = findListOfExistingFiles(path, filename);
		ArrayList<String> lines = new ArrayList<String>();
		for (File file : files) {
			try {
				List<String> readLines = FileUtil.readLines(file.getCanonicalPath(), 20);
				lines.addAll(readLines);
			} catch (Exception e) {
			}
			
		}
		return isValueFound(property, lines);
	}

}
