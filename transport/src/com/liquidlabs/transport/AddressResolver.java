package com.liquidlabs.transport;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.liquidlabs.common.net.URI;

public class AddressResolver {
	Map<String, String> ips = new HashMap<String, String>();
	
	public AddressResolver(String hostsFile) {
		try {
			BufferedReader input =  new BufferedReader(new FileReader(hostsFile));
			
			try {
		        String line = null; 
		        while (( line = input.readLine()) != null){
		        	line = line.replaceAll("   ", " ");
		        	line = line.replaceAll("  ", " ");
		        	String[] items = line.split(" ");
		        	if (items.length == 2) {
		        		ips.put(items[0], items[1]);
		        	}
		        }
		      }
		      finally {
		        input.close();
		      }
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}
	}

	public URI resolve(URI uri) {
		if (ips.isEmpty()) return uri;
		if (!ips.containsKey(uri.getHost())) {
			return uri;
		}
		try {
			return new URI(uri.getScheme(), uri.getUserInfo(), ips.get(uri.getHost()), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment());
		} catch (URISyntaxException e) {
			return uri;
		}
	}
}
