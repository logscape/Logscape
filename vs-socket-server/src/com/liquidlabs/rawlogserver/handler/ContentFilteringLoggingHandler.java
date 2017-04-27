package com.liquidlabs.rawlogserver.handler;

import com.liquidlabs.common.DateUtil;
import com.liquidlabs.common.StringUtil;
import com.liquidlabs.common.concurrent.NamingThreadFactory;
import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.common.regex.SimpleQueryConvertor;
import com.liquidlabs.rawlogserver.handler.fileQueue.FileQueuer;
import com.liquidlabs.rawlogserver.handler.fileQueue.FileQueuerFactory;
import com.liquidlabs.vso.deployment.ServiceTestHarness;
import com.logscape.meter.Meter;
import javolution.util.FastMap;
import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;


/**
 * Breaks the streamdown to output to a respective file mapping
 *
 */
public class ContentFilteringLoggingHandler extends BaseLoggingHandler implements StreamHandler {
	
	final public static Logger LOGGER = Logger.getLogger(ContentFilteringLoggingHandler.class);

	private static final String RIGHT_BR = ")";
	private static final String LEFT_BR = "(";


	static String mappingFile = System.getProperty("mapping.rules.filename", "mapping.csv");

	List<TypeMatcher> matchers = new CopyOnWriteArrayList<TypeMatcher>();
	
	ExecutorService executor = Executors.newFixedThreadPool(3, new NamingThreadFactory("SockFilterSPI"));
	ScheduledExecutorService scheduler = com.liquidlabs.common.concurrent.ExecutorService.newScheduledThreadPool(1, new NamingThreadFactory("CFSched"));
	
	Map<String, ClientContentHandler> clientHandlers = new FastMap<String, ClientContentHandler>().shared();
    private FileQueuerFactory fqFactory;

    public ContentFilteringLoggingHandler(FileQueuerFactory fqFactory) {
        this.fqFactory = fqFactory;
    }
	
	public void start() {
		List<String> mappingContent = loadMappingContent();
		populate(mappingContent, matchers);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (rootFileHandle != null && rootFileHandle.lastModified() != loadedAt) {
                        loadedAt = rootFileHandle.lastModified();
                        LOGGER.info("Reloading Mapping:" + rootFileHandle);
                        List<String> mappingContent = loadMappingContent();
                        matchers.clear();
                        Set<String> strings = clientHandlers.keySet();
                        for (String string : strings) {
                            ClientContentHandler removed = clientHandlers.remove(string);
                            removed.stop();
                        }

                        populate(mappingContent, matchers);
                    }
                } catch (Throwable t) {
                    LOGGER.warn("Reload Failed:" + t.toString());
                }
            }
        }, 1, 3, TimeUnit.SECONDS);
	};
	public void stop() {
		Collection<ClientContentHandler> values = clientHandlers.values();
		for (ClientContentHandler clientContentHandler : values) {
			clientContentHandler.stop();
		}
	}

    static File rootFileHandle = null;
    static long loadedAt = 0;
	
	static List<String> loadMappingContent(){
		
		LOGGER.info("WorkingDir:" + new File(".").getAbsolutePath());
		InputStream is = null;
		File rootFile = new File("../../downloads/" + mappingFile);
		if (rootFile.exists()) {
			try {
				is = new FileInputStream(rootFile);
                rootFileHandle = rootFile;
				LOGGER.info("Loading MappingFile from Root:" + rootFile.getAbsolutePath());
			} catch (FileNotFoundException e) {
			}
		}
		if (is == null) {
			is = ClassLoader.getSystemResourceAsStream(mappingFile);
			if (is != null) {
                LOGGER.info("Loading MappingFile from ClassPath:" + System.getProperty("java.class.path"));
            }
		}
		if (is == null)
			try {
				is = new FileInputStream(mappingFile);
				if (is != null) LOGGER.info("Loading MappingFile from Dir:" + new File(mappingFile).getAbsolutePath());
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			}
		if (is == null) {
			LOGGER.warn("Failed to load mapping file:" + mappingFile);
			return null;
		}
		byte[] content = null;
		try {
			content = new byte[is.available()];
			is.read(content);
			is.close();
		} catch (IOException e) {
			LOGGER.warn("Failed to load mapping:" + mappingFile + " ex:" + e.toString());
			return null;
		}
		String string = new String(content);
		string = string.replaceAll("\r", "");
		return Arrays.asList(string.split("\n"));
	}


	@Override
	public void handled(byte[] payloadBytes, String remoteAddress, String remoteHostname, String rootDir) {

        if (LOGGER.isDebugEnabled()) LOGGER.debug(">>handled remoteAddress:" + remoteAddress);
                try {
		String hostPath = getHostPath(remoteHostname, remoteAddress);
        String payloadLine = new String(payloadBytes);
        if (payloadLine.contains(ServiceTestHarness.RUNNER_KEY)) {
            try {
                JSONObject jsobj = new JSONObject(payloadLine);
                String destFile = jsobj.getString("destFile");
                FileQueuer fileQueuer = fqFactory.create(remoteAddress, destFile, DateUtil.shortDateFormat.print(System.currentTimeMillis()));
                String host = jsobj.getString("host");
                String ipaddress = jsobj.getString("ipAddress");
                String token = Meter.extractToken(jsobj.getString("token"));
                String tag = Meter.extractTag( jsobj.getString("tag"));
                fileQueuer.setTokenAndTag(ipaddress + "-" +host, token, tag);
                fileQueuer.append(jsobj.getString("message"));
            } catch (JSONException e) {
                LOGGER.warn("From:" + remoteAddress + " " + e.toString() + " JSON:" + payloadLine);
            }

        } else {


            ClientContentHandler handler = this.clientHandlers.get(hostPath);
            if (handler == null) {
                handler = new ClientContentHandler(hostPath, this.matchers, this.scheduler, fqFactory);
                this.clientHandlers.put(hostPath, handler);
            }
            synchronized (handler) {

                handler.handleContent(remoteAddress, payloadLine, this, rootDir);
            }
        }
        } finally {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("<<handled() remoteAddress:" + remoteAddress);
        }

	}


	public TypeMatcher getTypeMatcher(String data) {
		for (TypeMatcher matcher : this.matchers) {
			if (matcher.isMatch(data)) return matcher;
		}
		return null;
	}
    public static Set<String> badLines = new HashSet<String>();
	public static List<TypeMatcher> populate(List<String> lineData, List<TypeMatcher> matchers) {
		for (String line : lineData) {
			if (line.startsWith("#")) continue;
            if (badLines.contains(line)) continue;
            try {
                TypeMatcher typeMatcher = new TypeMatcher(line);
                matchers.add(typeMatcher);
            } catch (Throwable t) {
                badLines.add(line);
                LOGGER.error("Failed to Load Mapping:" + line, t);
            }
		}
		return matchers;
	}

	public StreamHandler copy() {
		return new ContentFilteringLoggingHandler(fqFactory);
	}
	public static class TypeMatcher {
		private Pattern pattern;
		private boolean timeStamp = false;
		private boolean mline = false;
		private String recordDelimiter = "";
		String type = "unknown";
		String expression = "";
		String regexp;
		String postProcess = "";
		public String[] parts = new String[0];
		public FileQueuer queuer;
		private Pattern postProcessPattern;
        int serverGroup=-1;
        enum keys  { Type,Timestamp,Mline,AppendNl,PostProcess,ServerGroup,Expression};
		
		public TypeMatcher(String line) {

			String[] parts = StringUtil.splitFast(line, ',',keys.values().length, false );

			this.type = parts[keys.Type.ordinal()];
			this.postProcess = parts[keys.PostProcess.ordinal()];
			this.timeStamp = parts[keys.Timestamp.ordinal()].contains("timestamp=true");
            this.serverGroup = Integer.parseInt(parts[keys.ServerGroup.ordinal()].substring(parts[keys.ServerGroup.ordinal()].indexOf("=")+1));
			this.mline = parts[keys.Mline.ordinal()].contains("mline=true");
            String appendNl = parts[keys.AppendNl.ordinal()];
			if ( appendNl.contains("appendNL=true")) this.recordDelimiter = "\n";
			else {
				if (!appendNl.contains("appendNL=false")) {
					recordDelimiter = appendNl.substring("appendNL=".length(), appendNl.length()).replaceAll("\\\\n", "\n");
				}
			}
			
			this.expression =  parts[keys.Expression.ordinal()];
			if (expression.contains("AND") || (expression.contains(LEFT_BR) && expression.contains(RIGHT_BR))) {
				this.regexp = SimpleQueryConvertor.convertSimpleToRegExp(expression);
				pattern = getPattern(regexp);
			} else if (expression.contains("[")) {
				try {
					pattern = getPattern(expression);
				} catch (Throwable t) {
					
				}
			}
			

		}
		public TypeMatcher() {
		}
		public boolean isMatch(String data) {
			this.parts = new String[0];
			if (pattern != null) {
				// regexp
				Matcher matcher = pattern.matcher(data);
				MatchResult mr = new MatchResult(matcher, RegExpUtil.isMultiline(data));
				if (mr.isMatch()) {
                    String[] groups = mr.getGroups();
                    ArrayList<String> strings = new ArrayList<String>();
                    int index = 0;
                    for (String group : groups) {
                        //strings.add("_" +group.trim()+ "_");
                        if (this.serverGroup != -1 && index == serverGroup) {
                            strings.add("_SERVER_/" + group.trim());
                        } else {
//                            strings.add(group.trim());
							strings.add("_" +group.trim()+ "_");
                        }
                        index++;
                    }


                    this.parts = strings.toArray(new String[0]);
                    // auto tag it
					this.parts[0] = "_" + this.type + "_";
					return true;
				}
				
			} else {
				// substring
				if (data.contains(expression)) {
					this.parts = new String[] { this.type };
					return true;
				}
			}
			return false;
				
		}
		private Pattern getPattern(String regexp) {
			return new jregex.Pattern(regexp, REFlags.MULTILINE | REFlags.DOTALL);
		}
		public boolean isMLine() {
			return this.mline;
		}
		public boolean isTimeStamp() {
			return timeStamp;
		}
		public String getRecordDelimiter() {
			return recordDelimiter;
		}
		public String toString() {
			return String.format("%s type:%s expr:%s", getClass().getSimpleName(), this.type, this.expression);
		}
		public String postProcess(LinkedBlockingQueue<String> buffer) {
			if (this.postProcess == null || this.postProcess.length() == 0) return "";
			if (postProcessPattern == null) {
				if (postProcess.startsWith("postProcessFileName=")){
					postProcess = postProcess.substring("postProcessFileName=".length());
					postProcessPattern = getPattern(postProcess);
				}
			}
			
			ArrayList<String> arrayList = new ArrayList<String>(buffer);
			StringBuilder string = new StringBuilder();
			for (String item : arrayList) {
				string.append(item);
			}
			String string2 = string.toString();
			Matcher matcher = postProcessPattern.matcher(string2);
			MatchResult mr = new MatchResult(matcher, RegExpUtil.isMultiline(string2));
			String result = "";
			if (mr.isMatch()) {
				String[] groups = mr.getGroups();
				for (int i = 1; i < groups.length; i++) {
					result += groups[i] + "/";
				}
			}
			return result;
		}
	}
}
