package com.liquidlabs.log.streamer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.liquidlabs.common.StringUtil;


public class LogLine implements Externalizable {
	
	public String text;
	public int number;
	public long timeMs;
	public String time;
	public int lines;
	
	public LogLine() {
	}
	
	public LogLine(long timeMs, String time, int number, String line) {
		this.timeMs = timeMs;
		this.time = time;
		this.number = number;
		this.text = StringUtil.escapeAsText(line);
	}


	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("[LogLine:");
		buffer.append(" text: ");
		buffer.append(text);
		buffer.append(" number: ");
		buffer.append(number);
		buffer.append(" timeMs: ");
		buffer.append(timeMs);
		buffer.append(" time: ");
		buffer.append(time);
		buffer.append("]");
		return buffer.toString();
	}

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.text = in.readUTF();
		this.number = in.readInt();
		this.timeMs = in.readLong();
		this.time = in.readUTF();
		this.lines = in.readInt();
	}

	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(this.text);
		out.writeInt(this.number);
		out.writeLong(timeMs);
		out.writeUTF(time);
		out.writeInt(this.lines);
	}
}
