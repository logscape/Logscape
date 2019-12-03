package com.liquidlabs.dashboard.server;


import java.util.Random;

public class DashboardProperties {

    public static final String HTTPS_PORT = "com.logscape.dashboard.ssl.port";
    public static final String HTTP_PORT = "com.logscape.dashboard.port";
    private static final String HTTPS_URL = "com.logscape.dashboard.https.url";
    private static final String HTTP_URL = "com.logscape.dashboard.http.url";

    public static final String PROXY_TAILERS = "proxy.tailers";
    private static final String START_TIME = "com.logscape.build.id";
    private static final int randomBuildId = new Random().nextInt(10000000);
    private static final String buildId = System.getProperty("version.id","unknown");
    private static final String versionId = System.getProperty("version.id","unknown");

    public static String getBuildId() {
        return buildId;
    }
    public static String getVersionId() {
        return versionId;
    }


	public static Integer getSessionTimeoutDays() {
		return Integer.getInteger("logscape.session.timeout.days", 7);
	}
	public static Integer getReplayLineLimit() {
		return Integer.getInteger("dashboard.replay.maxLogLineWidth", 3 * 1024);
	}


	public static Integer getMaxLiveEvents() {
		return Integer.getInteger("log.max.live.events", 5 * 1024);
	}

    public static void setHttpsPort(String sslPort) {
        System.setProperty("web.ssl.port", sslPort);
        System.setProperty(HTTPS_PORT, sslPort);
    }

    public static void setHttpPort(String httpPort) {
        System.setProperty(HTTP_PORT, httpPort);
    }

    public static String getHttpsPort(){
        return System.getProperty(HTTPS_PORT,"8081");
    }

    public static String getHttpPort() {
        return System.getProperty(HTTP_PORT,"8080");
    }

    public static String getHttpsUrl() {
        return System.getProperty(HTTPS_URL);
    }

    public static String getHttpUrl() {
            return System.getProperty(HTTP_URL);
    }

    public static void setHttpsUrl(String httpsUrl) {
        System.setProperty(HTTPS_URL, httpsUrl);
    }

    public static void setHttpUrl(String httpUrl) {
        System.setProperty(HTTP_URL, httpUrl);
    }

    public static boolean isProxyTailers() {
        return Boolean.getBoolean(PROXY_TAILERS);
    }

    public static void setStartTime(long startTime) {
        System.setProperty(START_TIME, Long.toString(startTime));
    }

    public static String getStartTime(){
        return System.getProperty(START_TIME, Integer.toString(randomBuildId));
    }
}
