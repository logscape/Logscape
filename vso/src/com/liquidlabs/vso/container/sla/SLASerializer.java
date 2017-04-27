package com.liquidlabs.vso.container.sla;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import com.thoughtworks.xstream.XStream;

public class SLASerializer {

	private static final Logger LOGGER = Logger.getLogger(SLASerializer.class);
	
	private final XStream xstream = new XStream();
	
	public SLASerializer() {
		xstream.alias("sla", GroovySLA.class);
		
		xstream.useAttributeFor(GroovySLA.class, "consumerClass");
		
		xstream.alias("variable", Variable.class);
		xstream.useAttributeFor(Variable.class, "name");
		xstream.useAttributeFor(Variable.class, "value");
		xstream.addImplicitCollection(GroovySLA.class, "variables", Variable.class);
		
		xstream.alias("timePeriod", TimePeriod.class);
		xstream.addImplicitCollection(GroovySLA.class, "timePeriods", TimePeriod.class);
		
		xstream.addImplicitCollection(TimePeriod.class, "rules");
		xstream.alias("rule", Rule.class);

		xstream.alias("resourceGroup", String.class);
		xstream.useAttributeFor(Rule.class, "maxResources");
		xstream.useAttributeFor(Rule.class, "priority");
		xstream.addImplicitCollection(Rule.class, "resourceGroups", String.class);
		
		xstream.useAttributeFor(TimePeriod.class, "start");
		xstream.useAttributeFor(TimePeriod.class, "end");
		xstream.useAttributeFor(TimePeriod.class, "label");
		xstream.useAttributeFor(TimePeriod.class, "isOneOff");
		xstream.aliasField("evaluator", Rule.class, "script");
	}
	

	public SLA deSerializeFile(String pathToCheck, String filename) {
		
		if (filename == null || filename.length() == 0) throw new IllegalArgumentException("Cannot open SLA.xml file with name:" + filename);
		
		SLA fromXML = null;
		
		File overrider = null;
		
		LOGGER.info(String.format("Loading from Path:%s exists:%s ", pathToCheck, new File(pathToCheck, filename).exists()));
		
		if (pathToCheck != null && pathToCheck.length() > 0 && new File(pathToCheck, filename).exists()) {
			LOGGER.info("Found at specified location");
			overrider = new File(pathToCheck, filename);
			try {
				FileInputStream fis = new FileInputStream(overrider);
				fromXML = (SLA) xstream.fromXML(fis);
				fis.close();
				return fromXML;
			} catch (Exception e) {
				LOGGER.info("NOT Found at specified location, loading from classpath");
			}			
		} 
		InputStream stream = getClass().getClassLoader().getResourceAsStream(filename);
		if (fromXML == null && stream != null) {
			LOGGER.info("LOADING from classpath successful");
			fromXML = (SLA) xstream.fromXML(stream);
			try {
				stream.close();
			} catch (IOException e) {
			}
		} else {
			try {
				if (!new File(filename).exists()) throw new RuntimeException(String.format("Failed to open SLA.xml[%s] in dir[%s] or locate on Classpath", filename, new File(".").getAbsolutePath()));
				FileReader fileReader = new FileReader(filename);
				fromXML = (SLA) xstream.fromXML(fileReader);
				fileReader.close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (fromXML.getConsumerClass() == null || fromXML.getConsumerClass().length() == 0) throw new RuntimeException("SLA.consumerClass has not been specified, cannot load this SLA.xml");
		return fromXML;
	}
	public SLA deSerialize(String pathToCheck, String string) {
		if (string.endsWith(".xml")) return this.deSerializeFile(pathToCheck, string);
		
		SLA fromXML = (SLA) xstream.fromXML(string);
		if (fromXML.getConsumerClass() == null || fromXML.getConsumerClass().length() == 0) throw new RuntimeException("SLA.consumerClass has not been specified, cannot load this SLA.xml");
		return fromXML;
	}
	public String serialize(SLA sla){
		String slaXml = xstream.toXML(sla);
		slaXml = slaXml.replaceAll("\t", "  ");
		slaXml = slaXml.replaceAll("&amp;", "&");
		slaXml = slaXml.replaceAll("&apos;", "'");
		slaXml = slaXml.replaceAll("&lt;", "<");
		slaXml = slaXml.replaceAll("&gt;", ">");
		slaXml = slaXml.replaceAll("&quot;", "\"");
		slaXml = slaXml.replaceAll("<evaluator>", "<evaluator><![CDATA[");
		slaXml = slaXml.replaceAll("</evaluator>", "]]></evaluator>");
		return slaXml;
	}
}
