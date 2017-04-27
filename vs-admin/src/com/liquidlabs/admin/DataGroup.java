package com.liquidlabs.admin;

import com.liquidlabs.orm.Id;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 11/04/2013
 * Time: 13:36
 * To change this template use File | Settings | File Templates.
 */
public class DataGroup {
    @Id
    public String name;
    public String include;
    public String exclude;
    public boolean enabled;
    public String children;
    public String resourceGroup = "";


    enum SCHEMA { children, enabled, exclude, include, name, resourceGroup  };

    public DataGroup(){};
    public DataGroup(String name, String include, String exclude, String children, boolean enabled, String resourceGroup) {
        this.name = name;
        this.include = include;
        this.exclude = exclude;
        this.children = children;
        this.enabled = enabled;
        this.resourceGroup = resourceGroup;
    }

    public String getName() {
        return name;
    }

    public String getInclude() {
        if (include == null) return "";
        return include;
    }
    public void setResourceGroup(String group) {
        this.resourceGroup = group;
    }

    public String getExclude() {
        if (exclude == null) return "";
        return exclude;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getChildren() {
        if (!isEnabled()) return "";
        return children;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setInclude(String include) {
        this.include = include;
    }

    public void setExclude(String exclude) {
        this.exclude = exclude;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setChildren(String children) {
        this.children = children;
    }

    public void merge(UserSpace userSpace, DataGroup dataGroup) {
        if (!this.enabled) {
            this.include = "";
        }
        // NOTE we only go to a depth of 3 - we want to prevent recursive loops etc
        List<DataGroup> d1 = userSpace.getDataGroups(children);
        for (DataGroup group : d1) {
            this.copyFlags(group);
            List<DataGroup> d2 = userSpace.getDataGroups(group.getChildren());
            for (DataGroup d22 : d2) {
                this.copyFlags(d22);
                List<DataGroup> d3 = userSpace.getDataGroups(group.getChildren());
                for (DataGroup d33 : d3) {
                    this.copyFlags(d33);
                }
            }
        }
    }
    public String getResourceGroup() {
        if (resourceGroup == null) return "";
        return this.resourceGroup;
    }

    private void copyFlags(DataGroup dataGroup1) {
        if (dataGroup1.enabled) {
            try {
                if (dataGroup1.getInclude().length() > 0) {
                    if (!this.getInclude().contains(dataGroup1.getInclude())) {
                        if (this.include.length() > 0) this.include += ",";
                        this.include += dataGroup1.getInclude();
                    }
                }
                if (dataGroup1.getExclude().length() > 0) {
                    if (!this.getExclude().contains(dataGroup1.getExclude())) {
                        if (this.exclude.length() > 0) this.exclude += ",";
                        this.exclude += "," + dataGroup1.getExclude();
                    }
                }
                if (dataGroup1.getResourceGroup().length() > 0) {
                    if (!this.getResourceGroup().contains(dataGroup1.getResourceGroup())) {
                        this.resourceGroup += "," + dataGroup1.getResourceGroup();
                    }
                }
            } catch (Throwable t) {
                System.err.println("Bad DataGroup copyFlags This:" + this.name + " MergeTarget:" + dataGroup1.getName());
                t.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return "DataGroup{" +
                "name='" + name + '\'' +
                ", include='" + include + '\'' +
                ", exclude='" + exclude + '\'' +
                ", enabled=" + enabled +
                ", children='" + children + '\'' +
                '}';
    }

    public void addHost(String ip) {
        if (this.resourceGroup == null || this.resourceGroup.length() == 0) {
            this.resourceGroup = "hosts:" + ip;
        } else {
            this.resourceGroup += ", hosts:" + ip;
            String[] split = this.resourceGroup.split(",");
            Set<String> gggs = new HashSet<String>();
            for (String s : split) {
                gggs.add(s.trim());
            }
            this.resourceGroup = gggs.toString().replace("[","").replace("]","");
        }
    }

}
