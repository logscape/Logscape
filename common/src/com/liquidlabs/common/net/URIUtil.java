package com.liquidlabs.common.net;

import java.util.HashMap;
import java.util.Map;


public class URIUtil {

	public static String getParam(String paramKey, URI uri) {
		String query = uri.getQuery();
		try {
			String[] params = query.split("&");
			Map<String, String> map = new HashMap<String, String>();
			for (String param : params) {
				String[] split = param.split("=");
				if (split.length == 2) map.put(split[0], split[1]);
			}
			
			return map.get(paramKey);
		} catch (Throwable t) {
			return null;
		}
	}

}
