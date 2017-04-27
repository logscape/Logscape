package com.liquidlabs.vso.agent;

import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.joda.time.format.DateTimeFormatter;

import com.liquidlabs.common.DateUtil;

/**
 * Takes in stuff like;
 * 
 * and runs over the tree
 * @author Neiil
 *
 */
public class JmxScriptRunner {
	private final OutputStream os;
	private final boolean verbose;
	DateTimeFormatter formatter = DateUtil.shortDateTimeFormat3;

	public JmxScriptRunner(OutputStream os, boolean verbose) {
		this.os = os;
		this.verbose = verbose;
		
	}
	public Throwable t = null;
	public StringBuilder processed = new StringBuilder();
	public void runJmxQueries(String[] queries, String jmxUrl, Map<String, ?> properties) throws MalformedURLException, IOException {
		if (jmxUrl == null || jmxUrl.length() == 0) jmxUrl = "service:jmx:rmi:///jndi/rmi://localhost:8989/jmxrmi";
		JMXServiceURL url = new JMXServiceURL(jmxUrl);
		JMXConnector jmxc = JMXConnectorFactory.connect(url, properties);
		try {
			MBeanServerConnection server = jmxc.getMBeanServerConnection();
			
			int count = 0;
			for (String query: queries) {
				try {
					query = query.replaceAll("\t", "");
					query = query.trim();
					String[] attrs = getSpecifiedAttrs(query);
					query =  removeAttrs(query);
					ObjectName rootbean = new ObjectName(query);
					Set<ObjectName> queryNames = server.queryNames(rootbean, null);
					for (ObjectName objectName2 : queryNames) {
						count++;
						MBeanInfo beanInfo = server.getMBeanInfo(objectName2);
						processed.append(objectName2.getCanonicalName()).append(",");
						
						// Attributed were not specified - so grab everything
						if (attrs.length == 0) {
						
							MBeanAttributeInfo[] attributes3 = beanInfo.getAttributes();
							attrs = new String[attributes3.length];
							int i = 0;
							for (MBeanAttributeInfo beanAttributeInfo : attributes3) {
								attrs[i++] = beanAttributeInfo.getName();
							}
						}
						AttributeList attributes2 = server.getAttributes(objectName2, attrs);
						printAttribute(objectName2.getCanonicalName(), attributes2);							
						count++;
					}
				} catch (Throwable t) {
					this.t  = t;
				}
			}
			processed.append(" CountedMBeans:" + count);
		} catch (Throwable t) {
			this.t = t;
		} finally {
			jmxc.close();
		}
	}

	private String[] getSpecifiedAttrs(String query) {
		if (query.contains("@")) {
			String params = query.substring(query.indexOf("@"), query.length());
			return params.split("@");
		} else return new String[0];
	}
	/**
	 * query - up until the first @attribute
	 * @param query
	 * @return
	 */
	private String removeAttrs(String query) {
		if (!query.contains("@")) return query;
		return query.substring(0, query.indexOf("@"));
	}
	private void printAttribute(String bean, AttributeList attributes2) {
		String now = formatter.print(System.currentTimeMillis());
		bean = bean.replaceAll("\\s+", "");
		for (Object object : attributes2) {
			try {
				if (object instanceof Attribute) {
					Attribute attr = (Attribute) object;
					Object value = attr.getValue();
					if (value instanceof CompositeDataSupport) {
						CompositeDataSupport cds = (CompositeDataSupport) value;
						
						printValues(false, new AtomicBoolean(false), bean, now, attr, cds, "");
						os.write("\n".getBytes());
					} else if (value instanceof String[]) {
						printValues(bean, now, attr, value);
					} else {
						printValuesObject(bean, now, attr, value);
					}
				}
				//os.write("\n".getBytes());
			} catch (Throwable t){
				this.t = t;
			}
		}
	}
	private void printValues(Boolean printedRoot, AtomicBoolean printedPath, String bean, String now, Attribute attr, CompositeDataSupport cds, String path) throws IOException {
		
		CompositeType compositeType = cds.getCompositeType();
		Set<String> keys = compositeType.keySet();
		
		for (Object key : keys) {
			try {
				String keyString = key.toString();
				Object value = cds.get(key.toString());
				
				if (value instanceof TabularDataSupport) {
					TabularDataSupport mappp = (TabularDataSupport) value;
					Set<Entry<Object, Object>> entrySet = mappp.entrySet();
					for (Entry<Object, Object> entry : entrySet) {
						try {
							String ttkey = entry.getKey().toString();
							Object vv = entry.getValue();
							
							if (vv instanceof CompositeDataSupport) {
								CompositeDataSupport cdsvv = (CompositeDataSupport) vv;
								printValues(printedRoot, new AtomicBoolean(false), bean, now, attr, cdsvv, path + key + "." + ttkey);
							} else {
								if (!printedRoot) {
									printedRoot = printRoot(bean, now, attr, path);
								}
								String msg = keyString.replaceAll("\\s+", "") + "=" + value.toString() + "< ";
								os.write(msg.getBytes());
							}
							os.write("\n".getBytes());
						} catch (Throwable t) {
							this.t = t;
						}
					}
					
				} else if (value instanceof CompositeDataSupport) {
					printValues(printedRoot, printedPath, bean, now, attr, (CompositeDataSupport) value, path + "." + keyString);
				} else if (value instanceof Long[]) {
					if (!printedRoot) {
						printedRoot = printRoot(bean, now, attr, path);
					}
					value = com.liquidlabs.common.collection.Arrays.toString((Long[]) value);
					String msg = keyString.replaceAll("\\s+", "") + ":" + value.toString() + " ";
					if (!printedPath.get()) {
						printedPath.set(true);
						msg  = path + "." + msg;
					}
					os.write(msg.getBytes());
				} else {
					if (!printedRoot) {
						printedRoot = printRoot(bean, now, attr, path);
					}
					String msg = keyString.replaceAll("\\s+", "") + ":" + value.toString() + " ";
					if (!printedPath.get()) {
						printedPath.set(true);
						msg  = path + " " + msg ;
					}
					os.write(msg.getBytes());
				}
			} catch (Throwable t) {
				this.t = t;
			}
		}
	
	}
	private boolean printRoot(String bean, String now, Attribute attr, String path) throws IOException {
		boolean printedRoot;
		printedRoot = true;
		String msg = now + "\t" + bean + " " + attr.getName().replaceAll("\\s+", "");
		if (path.length() > 0) msg += path;
		msg += " ";
		os.write(msg.getBytes());
		return printedRoot;
	}
	private void printValuesObject(String bean, String now, Attribute attr, Object value) throws IOException {
		if (value == null) return;
		String vString = value.toString();
		if (value instanceof Double) {
			vString = new DecimalFormat("#.####").format(value);
		}
		String generalMsg = now + "\t" + bean + "." + attr.getName().replaceAll("\\s+", "") + ":\t" + vString + "\n";
		os.write(generalMsg.getBytes());
	}
	private void printValues(String bean, String now, Attribute attr, Object value) throws IOException {
		String[] vs = (String[]) value;
		String strArrayMsg = now + "\t" + bean + "." + attr.getName().replaceAll("\\s+", "") + ":\t" + Arrays.toString(vs) + "\n";
		os.write(strArrayMsg.getBytes());
	}

}
