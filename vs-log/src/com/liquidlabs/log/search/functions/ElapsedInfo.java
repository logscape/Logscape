package com.liquidlabs.log.search.functions;

import static java.lang.String.format;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.liquidlabs.common.DateUtil;

public class ElapsedInfo implements Externalizable {
    private long startTime;
    private long endTime;
    private double divisor;
	private String labelValue;

    public ElapsedInfo() {
    }

    public ElapsedInfo(long startTime, long endTime, int divisor, String labelValue) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.divisor = divisor;
		this.labelValue = labelValue;
    }


    public String getLabel() {
    	return labelValue != null ? labelValue : "";
    }

    public long getStartTime() {
        return startTime;
    }

    public double getDuration() {
        return Math.abs((double)(endTime - startTime) /divisor);
    }

    @Override
    public String toString() {
        return format("ElapsedInfo [%s] startTime:%s, endTime:%s\n", this.labelValue, DateUtil.shortDateTimeFormat3.print(startTime), DateUtil.shortDateTimeFormat3.print(endTime));
    }

    @Override
    public int hashCode() {
        int result = (int) (startTime ^ (startTime >>> 32));
        result = 31 * result + (int) (endTime ^ (endTime >>> 32));
        result = 31 * result + (labelValue != null ? labelValue.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ElapsedInfo) {
            ElapsedInfo other = (ElapsedInfo) o;
                    return 
                    other.startTime == this.startTime &&
                    other.endTime == this.endTime &&
                    other.labelValue.equals(this.labelValue);
        }
        return false;
    }

    public long getEndTime() {
        return endTime;
    }

	public boolean isRunning(long start, long end) {
		return !(end < this.startTime || start > this.endTime);
	}
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.labelValue = in.readUTF();
		this.startTime = in.readLong();
		this.endTime = in.readLong();
		this.divisor = in.readDouble();
	}
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(this.labelValue);
		out.writeLong(this.startTime);
		out.writeLong(this.endTime);
		out.writeDouble(this.divisor);
	}

	public void setEndTime(long time) {
		this.endTime = time;
		
	}
}
