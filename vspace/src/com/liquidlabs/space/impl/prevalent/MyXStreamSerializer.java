package com.liquidlabs.space.impl.prevalent;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Set;

import org.prevayler.foundation.serialization.XStreamSerializer;

import com.liquidlabs.common.collection.LLMapConvertor;
import com.liquidlabs.space.map.MapImpl;
import com.liquidlabs.space.map.MapIsFullException;
import com.thoughtworks.xstream.XStream;

/**
 * Writes and reads objects using XML. This serializer can be used for
 * snapshots, journals or both.
 * 
 * <p>
 * This implementation requires the <a
 * href="http://xstream.codehaus.org/">XStream</a> Java and XML language binding
 * framework which provides for Java object XML serialization.
 * </p>
 * 
 * <p>
 * Note that XStream has some dependencies of its own. It requires the standard
 * XML API's (xml-apis.jar from the <a
 * href="http://xml.apache.org/xerces2-j/">Apache Xerces2-j</a> project or
 * j2sdk1.4+) and an XML implementation (again, provided by Xerces2 or
 * j2sdk1.4+).
 * </p>
 * 
 * <p>
 * To make XStream up to 10x faster, add <a
 * href="http://www.extreme.indiana.edu/xgws/xsoap/xpp/mxp1/">XPP3</a> to the
 * classpath. XStream has the concept of a <a href="http://xstream.codehaus.org/javadoc/com/thoughtworks/xstream/io/HierarchicalStreamDriver.html"
 * >HierarchicalStreamDriver</a> and the default implementation for XStream is
 * the highly performant XppDriver. However, XStream will fall back to the
 * DomDriver if XPP3 is not found in the classpath making the XPP3 library
 * entirely optional... well, not quite. See <a
 * href="http://jira.codehaus.org/browse/XSTR-71">XSTR-71</a>. The current
 * decision in that issue forces XPP3 to be a required runtime dependency when
 * using XStream unless one specifically configures another driver, such as the
 * DomDriver.
 * </p>
 */
public class MyXStreamSerializer extends XStreamSerializer {

	private ThreadLocal _xstreams = new ThreadLocal() {
		protected Object initialValue() {
			return createXStream();
		}
	};

	private String _encoding;

	/**
	 * Use the default character encoding for XML serialization.
	 */
	public MyXStreamSerializer() {
		_encoding = null;
	}

	/**
	 * Use the specified character encoding for XML serialization.
	 */
	public MyXStreamSerializer(String encoding) {
		_encoding = encoding;
	}

	private XStream getXStream() {
		return (XStream) _xstreams.get();
	}

	public void writeObject(OutputStream stream, Object object)
			throws IOException {
		OutputStreamWriter writer = _encoding == null ? new OutputStreamWriter(
				stream) : new OutputStreamWriter(stream, _encoding);
		getXStream().toXML(object, writer);
		writer.flush();
	}

	public Object readObject(InputStream stream) throws IOException,
			ClassNotFoundException {
		Object fromXML = getXStream().fromXML(
						_encoding == null ? new InputStreamReader(stream)
								: new InputStreamReader(stream, _encoding));
		if (fromXML instanceof MapImpl) {
			MapImpl map = (MapImpl) fromXML;
			Set<String> keySet = map.keySet();
			for (String key : keySet) {
				if (key.contains("com.liquidlabs.log.report.Report")) {
					String newKey = key.replaceAll("com.liquidlabs.log.report.Report", "com.liquidlabs.log.space.Search");
					String remove = map.remove(key);
					String newValue = remove.replaceAll("com.liquidlabs.log.report.Report", "com.liquidlabs.log.space.Search");
					map.remove(key);
					try {
						map.put(newKey, newValue);
					} catch (MapIsFullException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			
		}
		return fromXML;
	}

	/**
	 * Create a new XStream instance. This must be a new instance because
	 * XStream instances are not threadsafe.
	 */
	protected XStream createXStream() {
		XStream stream = new XStream();
		stream.registerConverter(new LLMapConvertor(stream.getMapper()));
		return stream;
	}

}