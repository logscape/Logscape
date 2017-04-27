package com.liquidlabs.log;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.log.space.LogSpace;

import java.util.LinkedHashMap;
import java.util.Map;

public class PrintUrlBuilder {
    private static int port = Integer.getInteger("web.app.port", 8080);
    private final LogSpace logSpace;
    private final Map<String, Object> params = new LinkedHashMap<String, Object>();
    private String name;
    private String mailLinkProtocol = System.getProperty("mailLinkProtocol", "http");

    public PrintUrlBuilder(LogSpace logSpace){
        this.logSpace = logSpace;
    }

    public PrintUrlBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public PrintUrlBuilder withParam(String key, Object value) {
        params.put(key, value);
        return this;
    }

    public String build() {
        StringBuilder url;
        if(mailLinkProtocol.equals("https")){
            url = new StringBuilder("https://");
        } else {
            url = new StringBuilder("http://");
        }
        url.append(NetworkUtils.getHostname()).append(":").append(port).append("/play/?");
        url.append(determineReportType()).append("=").append(name);
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            url.append('&').append(entry.getKey()).append('=').append(entry.getValue());
        }
        return url.toString();
    }


    private String determineReportType() {
        return logSpace.getSearch(name, null) != null ? "Search" : "Workspace";
    }
}
