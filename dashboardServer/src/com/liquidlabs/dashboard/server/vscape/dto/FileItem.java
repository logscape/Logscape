package com.liquidlabs.dashboard.server.vscape.dto;

import com.liquidlabs.common.DateUtil;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 05/04/2013
 * Time: 15:31
 * To change this template use File | Settings | File Templates.
 */
public class FileItem {
    public String name;
    public String size;
    public String time;
    public long timeMs;
    public String status;
    public boolean isSystem;
    public int downloaded;
    public int totalAgents;
    public String view;
    transient public final File file;
    public FileItem(File file, String sysStatus, String deployedFlag, String bundleXML, int downloaded, int totalAgents, String view) {
        this.file = file;
        this.name = file.getName();
        this.size = String.format("%dkb",file.length() / 1024);
        this.timeMs = file.lastModified();
        this.time =  DateUtil.shortDateTimeFormat2.print(file.lastModified());
        if (sysStatus.equals("ACTIVE")) status = "DEPLOYED";
        else status = sysStatus + ":" + deployedFlag;

        if (!this.name.endsWith(".zip")) {
            status = "-";
        }
        isSystem = file.getName().equals("boot.zip") || bundleXML.contains("system=\"true\"");
        this.downloaded = downloaded;
        this.totalAgents = totalAgents;
        this.view = view;
    }
    public void incrementDownloaed() {
        this.downloaded++;
    }
    @Override
    public String toString() {
        return super.toString() + " Name:" + name + " sys:" + this.isSystem + " size:" + this.size;
    }
}