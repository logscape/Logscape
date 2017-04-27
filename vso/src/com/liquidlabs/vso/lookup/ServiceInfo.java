package com.liquidlabs.vso.lookup;

import com.liquidlabs.common.net.URI;
import com.liquidlabs.orm.Id;

import java.net.URISyntaxException;

import org.joda.time.DateTimeUtils;

public class ServiceInfo {
	
	enum SCHEMA { fullBundleName, id, insertTime, insertTimeMs, interfaceName, invokableTag, jmxURL, location, locationURI, name, replicationAddress, meta, zone, agentType  };
	
	@Id
	String id;
	
	String locationURI;
	
	String name;
	
	String interfaceName="";
	
	String insertTime = "";
	
	String jmxURL = "";
	
	private String fullBundleName = "";

	private String invokableTag;
    private String replicationAddress;
    
    long insertTimeMs = DateTimeUtils.currentTimeMillis();
    
    public String meta;
    public String agentType;

    /**
     * Physical location of the resource - use in resource lookup
     */
    String zone;
    @Deprecated // (renamed to zone)
	String location = "LOC";

    public ServiceInfo(){
	}
	public ServiceInfo(String name, String locationURI, String jmxURL, String zone, String agentType){

		this.id = zone + "/" + agentType + "/" + name + "/" + getBaseURL(locationURI);
		if (zone == null || zone.length() == 0 && name.contains(".")) {
			zone = name.substring(0, name.lastIndexOf("."));
			name = name.substring(name.lastIndexOf(".")+1);
		}
		this.name = name;
		this.jmxURL = jmxURL;
		if (zone == null) throw new IllegalArgumentException("Cannot provide null 'zone'");
		this.zone = zone;
        pickupDotNotation(name, zone);
        this.agentType = agentType;
		
		// remove any trailing path crap and make sure its lowercase hostnames only
		try {
			URI uri = new URI(locationURI);
			this.locationURI = "tcp://" + uri.getHost() + ":" + uri.getPort();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}

    private void pickupDotNotation(String name, String zone) {
        if ((zone == null || zone.length() == 0) && name.contains(".")) {
            this.zone = name.substring(0, name.lastIndexOf("."));
            this.name = name.substring(name.lastIndexOf("."));
        }
    }

    public ServiceInfo(String name, String locationURI, String interfaceName, String jmxURL, String fullBundleName, String invokableTag, String zone, String agentType) {
		this.invokableTag = invokableTag;
		this.id = zone + "/" + agentType + "/" + name + "/" + getBaseURL(locationURI);
		this.name = name;
		this.locationURI = locationURI;
		this.interfaceName = interfaceName;
		this.jmxURL = jmxURL;
		this.zone = zone;
        pickupDotNotation(name, zone);
		this.setFullBundleName(fullBundleName);
		this.agentType = agentType;
	}

	private String getBaseURL(String locationURI) {
		String baseURL = "";
		try {
			URI uri = new URI(locationURI);
			baseURL = uri.getHost() + "/" + uri.getPath() + ":" + uri.getPort();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return baseURL;
	}

	@Override
	public String toString() {
		return String.format("%s name:%s zone:%s uri[%s] iface[%s] added[%s] rep[%s] agent:%s", getClass().getSimpleName(), name, zone, locationURI, interfaceName,insertTime, this.replicationAddress, this.agentType);
	}
	public String getLocationURI() {
		return locationURI;
	}
	public String getZone() {
		return zone;
	}
	public String getName() {
		return name;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ServiceInfo other = (ServiceInfo) obj;
		if (locationURI == null) {
			if (other.locationURI != null)
				return false;
		} else if (!locationURI.equals(other.locationURI))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((locationURI == null) ? 0 : locationURI.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
	
	
	public String getInterfaceName() {
		return interfaceName;
	}
	public void setInterfaceName(String interfaceName) {
		this.interfaceName = interfaceName;
	}
	public String getInsertTime() {
		return insertTime;
	}
	public void setInsertTime(String insertTime) {
		this.insertTime = insertTime;
	}
	public String getJmxURL() {
		return jmxURL;
	}
	public void setFullBundleName(String fullBundleName) {
		this.fullBundleName = fullBundleName;
	}
	public String getFullBundleName() {
		return fullBundleName;
	}
	public String getInvokableTag() {
		return invokableTag;
	}

    public void setReplicationAddress(String replicationAddress) {
        this.replicationAddress = replicationAddress;
    }

    public String getReplicationAddress() {
    	if (replicationAddress == null) return "";
    	String sTime;
		try {
			sTime = com.liquidlabs.common.net.URIUtil.getParam("_startTime", new URI(getLocationURI()));
			if (sTime != null) {
				return replicationAddress += "&_startTime=" + sTime;
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

        return replicationAddress;
    }
	public String getHost() {
		try {
			return new URI(getLocationURI()).getHost();
		} catch (URISyntaxException e) {
			return "unknown";
		}
	}
	public String getMeta() {
		return meta;
	}
	public String getAgentType() {
		return this.agentType;
	}
}
