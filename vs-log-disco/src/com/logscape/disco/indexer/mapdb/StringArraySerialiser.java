package com.logscape.disco.indexer.mapdb;

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
public class StringArraySerialiser implements Serializer<String[]>, Serializable {
    @Override
    public void serialize(DataOutput out, String[] value) throws IOException {
        out.writeInt(value.length);
        for (String s : value) {
            out.writeUTF(s);
        }

    }

    @Override
    public String[] deserialize(DataInput in, int available) throws IOException {
        int length = in.readInt();
        String[] results = new String[length];
        for (int i = 0; i < length; i++) {
             results[i] = in.readUTF();
        }
        return results;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
