package com.liquidlabs.vso.container.sla;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.liquidlabs.vso.container.Consumer;
import com.liquidlabs.vso.container.Metric;
import com.thoughtworks.xstream.converters.ConversionException;

public class SLAValidator {
	Logger logger = Logger.getLogger(SLAValidator.class);

	public String validate(String newSLA,  Metric[] metrics, Consumer consumer) {
		String result = isWellFormed(newSLA);
		if (result.length() > 0) return result;
		
		result = isDeserializable(newSLA);
		if (result.length() > 0) return result;
		
		result = isRulesValid(newSLA, metrics, consumer);
		if (result.length() > 0) return result;
	
		return "SLA is valid";
	}

	
	public String isWellFormed(String string) {
		try {
			ByteArrayInputStream is = new ByteArrayInputStream(string.getBytes());
			DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			documentBuilder.parse(is);
		} catch (ParserConfigurationException e) {
			return "ERROR: " + e.getMessage();
		} catch (SAXException e) {
			return "ERROR: Document has errors:" + e.getMessage();
		} catch (IOException e) {
			logger.error(e);
		}
		return "";
	}

	public String isDeserializable(String sla_xml) {
		
		try {
			SLASerializer serializer = new SLASerializer();
			SLA sla = serializer.deSerialize(null, sla_xml);
		} catch (ConversionException ex) {
			return "ERROR: Deserialization failed, ConvertionException:" + ex.getMessage();
		} catch (Exception ex) {
			return "ERROR: Deserialiation failed," + ex.getMessage();
		}

		return "";
	}

	public String isRulesValid(String sla_xml, Metric[] metrics, Consumer consumer) {
		StringBuilder result = new StringBuilder();
		SLASerializer serializer = new SLASerializer();
		SLA sla = serializer.deSerialize(null, sla_xml);
		List<TimePeriod> timePeriods = sla.getTimePeriods();
		for (TimePeriod timePeriod : timePeriods) {
			List<Rule> rules = timePeriod.getRules();
			for (Rule rule : rules) {
				try {
					rule.setScriptLogger(logger);
					rule.evaluate(consumer, metrics);
				} catch (Throwable t){
					result.append("SLA Rule Label:").append(timePeriod.getLabel());
					result.append("Metrics:").append(Arrays.toString(metrics)).append("\n");
					result.append(" ERROR:").append(t.getMessage()).append("\n");
				}
			}
		}
		

		return result.toString();
	}


}
