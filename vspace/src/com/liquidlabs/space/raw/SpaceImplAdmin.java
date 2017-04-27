package com.liquidlabs.space.raw;

import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.ThreadUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.space.Space;
import com.liquidlabs.transport.serialization.ObjectTranslator;
import com.thoughtworks.xstream.XStream;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class SpaceImplAdmin implements SpaceImplAdminMBean {
	private final static Logger LOGGER = Logger.getLogger(SpaceImplAdmin.class);
	
	private final Space space;
	private int maxResults = 1024;
	private int maxSpaceSize;
	private String cwd = "";
	List<URI> peers = new CopyOnWriteArrayList<URI>();
	
	public SpaceImplAdmin(Space space, String partitionName, int spaceSize) {
		this.cwd = FileUtil.getPath(new File("."));
		this.space = space;
		this.maxSpaceSize = spaceSize;
		try {
			MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
			ObjectName objectName = new ObjectName("com.liquidlabs.vspace.Space:id="+partitionName);
			if (mbeanServer.isRegistered(objectName)) return;
			mbeanServer.registerMBean(this, objectName);
		} catch (Exception e) {
			LOGGER.error(e);
		}
	}
	public int getMaxResultValue(){
		return this.maxResults;
	}
	public int getCurrentSize(){
		return  space.size();
	}
	public int getMaxSpaceSize(){
		return maxSpaceSize;
	}
	public String getCWD() {
		return this.cwd;
	}

	public String setMaxResultValue(int maxResults){
		this.maxResults = maxResults;
		return "Will now return multiple items up to size:" + this.maxResults;
	}
	public String displayIndexSize(){
		String[] keys = space.getKeys(new String[] { "all:" }, -1);
		return "Number of keys:" + keys.length;
	}
	public String displayIndexMaxLimited(){
		String[] keys = space.getKeys(new String[] { "all:" }, this.maxResults);
		StringBuilder stringBuilder = new StringBuilder();
		
		int i = 0;
		for (String key : keys) {
			stringBuilder.append("<b>(").append(i++).append(")</b>  ").append(key).append("<br>\n");
		}
		
		return "Keys:\n" + stringBuilder.toString();
	}
	public String displayKeyValuesMaxLimited(){
		String[] keys = space.getKeys(new String[] { "all:" }, this.maxResults);
		StringBuilder stringBuilder = new StringBuilder();
		int i = 0;
		XStream stream = new XStream();
		for (String key : keys) {
			String value = space.read(key);
			i = appendValue(stringBuilder, i, stream, key, value);
		}
		return "KeyValuePairs:\n" + stringBuilder.toString();
	}
	private int appendValue(StringBuilder stringBuilder, int i, XStream stream, String key, String value) {
		
		try {
			String[] values = value.split(Space.DELIM);
			Class<?> classType = Class.forName(values[0]);
			Object obj = new ObjectTranslator().getObjectFromFormat(classType, value);
			String xml = stream.toXML(obj);
			String escapeHtml = StringEscapeUtils.escapeHtml4(xml);
			escapeHtml = escapeHtml.replaceAll("\n", "\n<br>");
			stringBuilder.append("<br><b>(").append(i++).append(")</b>  ").append(key).append("=<br>\n\t").append(escapeHtml).append("\n<br>");
			
			
		} catch (Throwable t) {
			t.printStackTrace();
			String replaceAll = value.replaceAll(Space.DELIM, "<br>\n\t\t");
			stringBuilder.append("\n\n<br><b>(").append(i++).append(")</b>  ").append(key).append("=<br>\n\t").append(replaceAll).append("\n<br>");
		}
		return i;
	}
	
	public String displayIndexSizeForQuery(String query){
		String[] keys = space.getKeys(new String[] { query }, -1);
		return "Number of keys:" + keys.length;
	}
	public String displayIndexForQueryMaxLimited(String query){
		String[] keys = space.getKeys(new String[] { query }, this.maxResults);
		StringBuilder stringBuilder = new StringBuilder();
		int i = 0;
		for (String key : keys) {
			stringBuilder.append("<b>(").append(i++).append(")</b>  ").append(key).append("<br>\n");
		}
		return "Keys:" + stringBuilder.toString();
	}
	public String displayKeyValuesForQueryMaxLimited(String query){
		
		LOGGER.info("Handling Query:" + query);
		
		String[] keys = space.getKeys(new String[] { "all:" }, this.maxResults);
		String values = space.read(keys[0]);
		try {
			Class<?> classType = Class.forName(values.split(Space.DELIM)[0]);
			LOGGER.info("Using Object Type:" + classType);
			ObjectTranslator ot = new ObjectTranslator();
			String[] spaceBasedQuery = ot.getQueryStringTemplate(classType, query);
			LOGGER.info("Q:" + Arrays.toString(spaceBasedQuery));
//			keys = space.getKeys(spaceBasedQuery, this.maxResults);
//			LOGGER.info("Results:" + keys.length);
		} catch (Exception e) {
//			e.printStackTrace();
			LOGGER.warn("Failed to handle query:", e);
		}
		
		StringBuilder stringBuilder = new StringBuilder();
		int i = 0;
		XStream stream = new XStream();
		for (String key : keys) {
			if (query.trim().length() == 0 || key.contains(query)) {
				String value = space.read(key);
				i = appendValue(stringBuilder, i, stream, key, value);
			}
//			stringBuilder.append("<b>(").append(i++).append(")</b>  ").append(key).append("=").append(space.read(key)).append("<br>\n");
		}
		return "KeyValuePairs:\n" + stringBuilder.toString();
	}
	
	public String takeItemWithKey(String key){
		String itemsRemoved = space.take(key, -1, 1);
		return "Removed [" + itemsRemoved +"] with key [" +key+"]";
	}
	
	public String takeItemsUsingTemplate(String template){
		
		try {
			String[] itemsRemoved = space.takeMultiple(new String[] { template }, -1, -1, -1);
			return "Removed [\n" + itemsRemoved.length +"]\n with template [" +template+"]";
		} catch (Throwable t){
			return ExceptionUtil.stringFromStack(t, -1);
			
		}
	}
	public String getReplicationPeers() {
		return this.peers.toString();
	}
	public boolean addPeer(URI uri) {
		String lookForThis = uri.getHost() + ":" + uri.getPort();
		boolean isAdding = true;
		for (URI peer : peers) {
			if (peer.toString().contains(lookForThis)) isAdding = false;
		}
		if (isAdding) this.peers.add(uri);
		return isAdding;
	}
	public void removePeer(URI uri) {
		this.peers.remove(uri);
		
	}
    public String dumpThreads(String filter) {
        String threadDump = ThreadUtil.threadDump("", filter);
        return threadDump.replaceAll("\n", "\n<br>");
    }

}
