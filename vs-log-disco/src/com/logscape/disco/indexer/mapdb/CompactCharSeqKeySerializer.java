package com.logscape.disco.indexer.mapdb;

import com.liquidlabs.common.collection.CompactCharSequence;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 06/01/15
 * Time: 21:21
 * To change this template use File | Settings | File Templates.
 */
public class CompactCharSeqKeySerializer extends BTreeKeySerializer<CompactCharSequence> {
    @Override
    public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
        byte[] previous = null;
        for (int i = start; i < end; i++) {
            CompactCharSequence key = (CompactCharSequence) keys[i];
            byte[] b = key.data();
            leadingValuePackWrite(out, b, previous, 0);
            previous = b;
        }
    }

    @Override
    public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
        Object[] ret = new Object[size];
        byte[] previous = null;
        for (int i = start; i < end; i++) {
            byte[] b = leadingValuePackRead(in, previous, 0);
            if (b == null) continue;
            ret[i] = new CompactCharSequence(b);
            previous = b;
        }
        return ret;
    }

    @Override
    public Comparator<CompactCharSequence> getComparator() {
        return BTreeMap.COMPARABLE_COMPARATOR;
    }
}
