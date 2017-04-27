package com.logscape.disco.indexer.mapdb;

import com.liquidlabs.common.collection.CompactCharSequence;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 07/11/2013
 * Time: 13:54
 * To change this template use File | Settings | File Templates.
 */
public class CompactCharSequenceSerialiser implements Serializer<CompactCharSequence>, Serializable {
    @Override
    public void serialize(DataOutput out, CompactCharSequence value) throws IOException {
        byte[] data = value.data();
        out.writeInt(data.length);
        out.write(data);
    }

    @Override
    public CompactCharSequence deserialize(DataInput in, int available) throws IOException {
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data);
        return new CompactCharSequence(data);
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
