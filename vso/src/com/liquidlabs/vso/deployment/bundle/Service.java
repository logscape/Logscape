package com.liquidlabs.vso.deployment.bundle;

import com.liquidlabs.orm.Id;

import java.util.Properties;

import static java.lang.String.format;

public class Service {

    public static final String BUNDLE_SERVICE_S_RESOURCE = "bundle.service.%s.resourceSelection";
    public static final String BUNDLE_SERVICE_S_PAUSE = "bundle.service.%s.pauseSeconds";
    public static final String BUNDLE_SERVICE_S_INSTANCE_COUNT = "bundle.service.%s.instanceCount";

    @Id
    private String id;
    @SuppressWarnings("unused")
    @Deprecated
    private String bundleServiceName;
    private String bundleId;
    private String name;
    private String resourceSelection = "";
    private String script;
    private String property;
    private int priority = 5;
    private String lookupVariable = "name:value";
    private String slaFilename;
    private String overridesService;
    private String instanceCount = "1";
    private int pauseSeconds = 0;
    private boolean background = false;
    private boolean fork = true;
    private String dependencies;
    private double costPerUnit;
    private boolean system;
    private int dependencyWaitCount = 1000;


    public Service(){
    }
    public Service(String bundleId, String serviceName, String script, String instanceCount){
        this.script = script;
        this.instanceCount = instanceCount;
        this.bundleId = bundleId;
        this.name = serviceName;
        setId();
    }
    private void setId(){
        this.id = getId(bundleId, name);
    }
    public static String getId(String bundleId, String serviceName) {
        return bundleId  + ":" + serviceName;
    }
    public String getFullBundleName(){
        return this.bundleId;
    }
    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
        setId();
    }

    public String getName(){
        return name;
    }
    public String getBundleId() {
        return bundleId;
    }
    public String getScript() {
        return script;
    }
    public String getResourceSelection() {
        if (resourceSelection == null) resourceSelection = "";
        return resourceSelection.replaceAll("\n", "");
    }
    public void setResourceSelection(String resourceSelection) {
        this.resourceSelection = resourceSelection;
    }
    public String getInstanceCount() {
        return instanceCount;
    }
    public void setInstanceCount(String instanceCount) {
        this.instanceCount = instanceCount;
    }
    public String getLookupVariable() {
        return lookupVariable;
    }
    public void setLookupVariable(String lookupVariable) {
        this.lookupVariable = lookupVariable;
    }
    public String getProperty() {
        return property;
    }
    public boolean isFork() {
        return fork;
    }
    public void setFork(boolean fork) {
        this.fork = fork;
    }
    public void setProperty(String property) {
        this.property = property;
    }
    public void setBackground(boolean background) {
        this.background = background;
    }
    public boolean isBackground() {
        return background;
    }
    public int getPauseSeconds() {
        return pauseSeconds;
    }
    public void setPauseSeconds(int pauseSeconds) {
        this.pauseSeconds = pauseSeconds;
    }
    public int getPriority() {
        return priority;
    }
    public void setPriority(int priority) {
        this.priority = priority;
    }
    public String getId() {
        if (id == null) setId();
        return id;
    }
    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + getId();
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getSlaFilename() {
        return slaFilename;
    }
    public void setSlaFilename(String slaFilename) {
        this.slaFilename = slaFilename;
    }

    public void addDependency(String service) {
        if (dependencies == null) {
            dependencies = service;
        } else {
            dependencies += "," + service;
        }
    }
    public boolean hasDependencies() {
        return dependencies != null;
    }

    public String [] getDependencies() {
        if (dependencies == null || dependencies.trim().length() == 0) {
            return new String[0];
        }
        return dependencies.split(",");
    }
    public void setSystem(boolean system) {
        this.system = system;
    }
    public String getOverridesService() {
        return overridesService;
    }
    public boolean	 isSystem() {
        return system;
    }
    public int getInstanceCountAsInteger() {
        try {
            if (isSimpleCount()) return Integer.parseInt(this.instanceCount);
        } catch (NumberFormatException e) {
        }
        return 1;
    }
    public boolean isSimpleCount() {
        if (this.instanceCount == null) instanceCount = "1";
        return this.instanceCount.indexOf("%") == -1;
    }
    public void setCostPerUnit(double costPerUnit) {
        this.costPerUnit = costPerUnit;
    }
    public double getCostPerUnit() {
        return costPerUnit;
    }
    public int getDependencyWaitCount() {
        return dependencyWaitCount ;
    }
    public void setDependencyWaitCount(int waitCount) {
        this.dependencyWaitCount = waitCount;
    }

    public void overrideWith(Properties bundleOverrides) {
        setResourceSelection(resourceOverride(bundleOverrides));
        setPauseSeconds(pausSecondsOverride(bundleOverrides));
        setInstanceCount(instanceCountOverride(bundleOverrides));
    }

    private String instanceCountOverride(Properties bundleOverrides) {
        return bundleOverrides.getProperty(format(BUNDLE_SERVICE_S_INSTANCE_COUNT, name), bundleOverrides.getProperty("bundle.defaults.instanceCount", instanceCount));
    }

    private int pausSecondsOverride(Properties bundleOverrides) {
        return currentValueOrFromProperty(bundleOverrides.getProperty(format(BUNDLE_SERVICE_S_PAUSE, name), bundleOverrides.getProperty("bundle.defaults.pauseSeconds")));
    }

    private String resourceOverride(Properties bundleOverrides) {
        return bundleOverrides.getProperty(format(BUNDLE_SERVICE_S_RESOURCE, name), bundleOverrides.getProperty("bundle.defaults.resourceSelection",this.resourceSelection));
    }

    private int currentValueOrFromProperty(String pauseSeconds) {
        try {
            return pauseSeconds == null ? this.pauseSeconds : Integer.valueOf(pauseSeconds.trim());
        } catch (NumberFormatException e) {
            return this.pauseSeconds;
        }
    }
}
