/**
 * 
 */
package com.liquidlabs.log.search.functions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

class Averager implements Externalizable {
	private int entries;
	private double total;
	
	void add(double val) {
		total += val;
		entries++;
	}
	
	double average() {
		if (total == 0 || entries == 0) return 0;
		return total / ((double) entries);
	}
	
	double sum() {
		return total;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + " e:" + entries + " t:" + total;
	}

    public void add(Averager other) {
        this.entries += other.entries;
        this.total += other.total;

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeDouble(this.total);
        out.writeInt(this.entries);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.total = in.readDouble();
        this.entries = in.readInt();
    }
}