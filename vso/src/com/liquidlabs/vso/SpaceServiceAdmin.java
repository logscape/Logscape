package com.liquidlabs.vso;

import java.util.Date;

import com.liquidlabs.common.Log4JLevelChanger;
import com.liquidlabs.common.ThreadUtil;
import com.liquidlabs.common.UID;
import com.liquidlabs.space.Space;
import com.liquidlabs.space.raw.SpaceImplAdmin;
import com.liquidlabs.transport.proxy.ProxyFactory;
import com.liquidlabs.vso.work.InvokableImpl;
import com.liquidlabs.vso.work.InvokableUI;

public class SpaceServiceAdmin implements InvokableUI {
	
	private final String serviceName;
	private final SpaceService spaceService;
	String id;
	private SpaceImplAdmin spaceAdmin;

	public SpaceServiceAdmin(String serviceName, SpaceService spaceService, ProxyFactory proxyFactory) {
		this.serviceName = serviceName;
		this.spaceService = spaceService;
		id = serviceName + "_Admin" + UID.getSimpleUID(serviceName);
		makeRemotable(proxyFactory);
		
	}

	private SpaceImplAdmin getSpaceAdmin(String serviceName, SpaceService spaceService) {
		if (spaceAdmin != null) return spaceAdmin;
		Space space = spaceService.getSpace();
		spaceAdmin = new SpaceImplAdmin(space, serviceName, 5 * 1024);
		return spaceAdmin;
	}
	
	private void makeRemotable(ProxyFactory proxyFactory) {
		if (proxyFactory != null) {
			InvokableImpl invokable = new InvokableImpl(this);
			proxyFactory.registerMethodReceiver(getUID(), invokable);
		}
	}
	public String getServiceName() {
		return serviceName;
	}
	
	public String getUID() {
		return id;
	}
	public String getUI() {
		return "<root>" +
		"<panel>" +
		"	<title label='" + serviceName + "'/>"+
		"	<label label='space\t:Management'         padding='10'/>"+
		"	<label label=' --------------------' padding='30'/>"+
		"  <row2 spaceOneWidth='10' label='Space Keys' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='spaceKeys' inputLabel='' taText='             ' outputHeight='200' />\n" +
		"  <row2 spaceOneWidth='10' label='Space KeyValue' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='spaceKeyValue' inputLabel=' ' taText='             ' outputHeight='200' />\n" +
		"  <row2 spaceOneWidth='10' label='Dump Threads' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='threadDump' inputLabel='filename' taText='             ' outputHeight='200' />\n" +
		"  <row2 spaceOneWidth='10' label='Set LogLevel' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='setLogLevel' inputLabel='className, [INFO,WARN,ERROR]' taText='             ' outputHeight='40' />\n" +
		"  <row2 spaceOneWidth='10' label='Dump Heap' labelWidth='200' buttonLabel='>>' buttonWidth='200' method='heapDump' inputLabel='filename' taText='             ' outputHeight='200' />\n" +
		" </panel>" + 
		"</root>";
	}
	
	/**
	 *  UI methods
	 */
	public String spaceKeys(String xxx) {
		return getSpaceAdmin(serviceName, spaceService).displayIndexMaxLimited().replaceAll("<b>", "").replaceAll("<br>", "");
	}
	public String  spaceKeyValue(String filter) {
		return getSpaceAdmin(serviceName, spaceService).displayKeyValuesForQueryMaxLimited(filter).replaceAll("&gt;", ">").replaceAll("&lt;", "<").replaceAll("<br>", "").replaceAll("&amp;", "&").replaceAll("quot;","\"");
	}
	public String setLogLevel(String arg0){
		try {
			String[] split = arg0.split(",");
			if (split.length != 2) return "Provide 2 args, loggerName and logLevel";
			Log4JLevelChanger.setLogLevel(split[0].trim(), split[1].trim());
			
			return "LogLevel set:" + split[0].trim() + " to " + split[1].trim();
		} catch (Throwable t){
			return "Error:" + t;
		}
	}
	public String threadDump(String filename){
		return ThreadUtil.threadDump(null,"");		
	}
	public String heapDump(String filename){
		System.out.println(new Date() +  " Dumping HEAP");
		try {
			return "Created Heap:" + com.liquidlabs.common.util.HeapDumper.dumpHeapWithPid(true);
		} catch (Throwable t) {
			t.printStackTrace();
			return t.getMessage();
		}

	}
}
