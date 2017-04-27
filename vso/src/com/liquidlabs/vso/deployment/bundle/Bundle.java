package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.common.regex.MatchResult;
import com.liquidlabs.common.regex.RegExpUtil;
import com.liquidlabs.orm.Id;

import java.util.ArrayList;
import java.util.List;

public class Bundle {
	public enum Status { UNINSTALLED, INSTALLED, RESOLVED, STARTING, STOPPING, ACTIVE }
	
	@Id
	String id;
	
	String name = "App";
	String version  = "1.10";
	
	String releaseDate  = "25/10/2008";
	String installDate  = "25/10/2008";
	String buildId  = "1.0";
	
	String classification = "DEV";
	String location = "LONDON_DC, FFT";
	String businessArea = "Credit";
	String businessClassification = "FO, MO";
	String owner = "user@logscape.com";
	String otherProperties = "downStreamSystems=Tdw,Venture, Feeds=MarkIT";
	String workingDirectory;
	boolean system;
	boolean autoStart;
	
	String status = Status.UNINSTALLED.name();
	List<Service> services = new ArrayList<Service>();
	
	public Bundle(){
	}
	public Bundle(String name, String version){
		this.name = name;
		this.version = version;
		setId();
	}
	public void setId(){
		this.id = getFullBundleName(name, version);		
	}
	
	static public String getFullBundleName(String name, String version) {
		return name + "-" + version;
	}
	
	public String getId() {
		if (id == null) setId();
	    return id;
	}
	
	public List<Service> getServices() {
		if (services == null) services = new ArrayList<Service>();
		for (Service service : services) {
			service.setBundleId(getId());
		}
		return services;
	}

	public String getName() {
		return name;
	}

    public String getBundleName() {
        return getFullBundleName(name, version);

    }
	public void setStatus(Status status) {
		this.status = status.name();
	}
	public void addService(Service service) {
		service.setBundleId(id);
		this.services.add(service);
	}
	public Status getStatus() {
		return Status.valueOf(this.status);
	}
	public String getVersion() {
		return version;
	}
	@Override
	public String toString() {
		return String.format("%s id: %s %s status:%s", getClass().getSimpleName(), id, name, version, status);
	}
	public String getReleaseDate() {
		return releaseDate;
	}
	public void setReleaseDate(String releaseDate) {
		this.releaseDate = releaseDate;
	}
	public String getClassification() {
		return classification;
	}
	public void setClassification(String classification) {
		this.classification = classification;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getBusinessArea() {
		return businessArea;
	}
	public void setBusinessArea(String businessArea) {
		this.businessArea = businessArea;
	}
	public String getBusinessClassification() {
		return businessClassification;
	}
	public void setBusinessClassification(String businessClassification) {
		this.businessClassification = businessClassification;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getOtherProperties() {
		return otherProperties;
	}
	public void setOtherProperties(String otherProperties) {
		this.otherProperties = otherProperties;
	}
	public void setName(String name) {
		this.name = name;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public void setServices(List<Service> services) {
		this.services = services;
	}
	public void setInstallDate(String installDate) {
		this.installDate = installDate;
	}
	public String getInstallDate() {
		return this.installDate;
	}

	public void setWorkingDirectory(String wd) {
		this.workingDirectory = wd;
	}
	
	public String getWorkingDirectory() {
		return workingDirectory;
	}
	
	public boolean isSystem() {
		return system;
	}
	
	public void setSystem(boolean value) {
		this.system = value;
	}
	public boolean isAutoStart() {
		return autoStart;
	}
	public void setAutoStart(boolean autoStart) {
		this.autoStart = autoStart;
	}
	public Service getService(String serviceName) {
		for (Service service : this.services) {
			if (service.getName().equals(serviceName)) return service;
		}
		return null;
	}
	public String getBundleIdForFilename(String filename) {
		String pattern = "(.*)-(\\d+\\.\\d+)";
		MatchResult matches = RegExpUtil.matches(pattern, filename);
		if (matches.isMatch()) {
			String name = matches.getGroup(1);
			String version = matches.getGroup(2);
			this.name = name;
			this.version = version;
			this.id = String.format("%s-%s", name, version);
			return this.id;
		} else {
			return this.id;
		}
	}
	public void setBundleIdFromZip(String zipName) {
		if (zipName.indexOf(".zip") == -1) return;
		getBundleIdForFilename(zipName.substring(0, zipName.indexOf(".zip")));	
	}
	public void setId(String bundleId) {
		if (bundleId != null) {
			this.id = bundleId;
			if (this.id.contains("-")) {
				this.name = this.id.substring(0, this.id.lastIndexOf("-"));
			}
		}
		
	}
}
