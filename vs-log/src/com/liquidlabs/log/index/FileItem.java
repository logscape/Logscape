package com.liquidlabs.log.index;

import org.apache.commons.lang3.StringEscapeUtils;

import java.io.File;
import java.io.IOException;

public class FileItem {
	public String label = "";
    public String path = "";
	public String type = "";
	public String dataType = "";
	public String sep = "";
	public String size = "";
	public long startTime;
	public boolean time;
	public boolean isIndexed = false;
	public String content = "";
	public String newLineRule = "Default";
	public long lastMod = 0;
	public String fileTag = "";
	
	public FileItem() {
	}
	public FileItem(String separator, File file, boolean isTimeKnown, String content, boolean isIndexed, String fileType, String newLineRule, long startTime, String fileTag) {
		this.label = file.getName();
		if (this.label.length() == 0) this.label = file.getPath();
		if (this.label.endsWith(File.separator) && this.label.length() > 1) this.label = this.label.substring(0, this.label.length()-1);
		if (file.isDirectory()) type = "dir";
		else type = "file";
        try {
            this.path = file.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        this.sep = separator;
		this.time = isTimeKnown;
		this.isIndexed = isIndexed;
		// content is not currently used can may cause mem overhead issues
		this.content = StringEscapeUtils.escapeXml(content);
		this.size = String.format("%.2f",(file.length() / (1024.0 * 1024.0)));
		this.dataType = fileType;
		this.lastMod = file.lastModified();
		this.newLineRule = newLineRule;
		this.startTime = startTime;
		this.fileTag = fileTag;
	}
	
	public String toXML() {
//		return String.format("<node label='%s' type='%s' sep='%s' time='%b' content='%s' />", this.label, this.type, this.sep, this.time, this.content);
		return String.format("<node label='%s' type='%s' sep='%s' time='%b' newLineRule='%s' isIndexed='%b' content='%s' dataType='%s' startTime='%d' lastMod='%d' size='%s' />", 
				this.label, this.type, this.sep, this.time, this.newLineRule, this.isIndexed, this.content, this.dataType, this.startTime, this.lastMod, this.size);
	}
	public String toString() {
		return "FileItem:" + toXML();
	}
    public String getType() {
        return type;
    }

}
