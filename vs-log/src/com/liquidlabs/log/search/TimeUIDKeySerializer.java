package com.liquidlabs.log.search;

import org.mapdb.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by Neiil on 10/28/2015.
 * Seems to be a bug in mapDB - cant use this - it confuses it with the Tuple2 serliazer and it all goes boom!
 */
public final class TimeUIDKeySerializer extends BTreeKeySerializer<TimeUID>  implements Serializable {

    transient private Serializer<Long> aSerializer = Serializer.LONG;
    transient private Comparator<Long> aComparator = new Comparator<Long>() {
        public int compare(Long o1, Long o2) {
            return o1.compareTo(o2);
        }
    };
    transient private Serializer<String> bSerializer = Serializer.STRING;

    @Override
    public void serialize(DataOutput out, int start, int end, Object[] keys) throws IOException {
        int acount=0;
        for(int i=start;i<end;i++){
            TimeUID t = (TimeUID) keys[i];
            if(acount==0){
                //write new A
                aSerializer.serialize(out,t.timeMs);
                //count how many A are following
                acount=1;
                while(i+acount<end && aComparator.compare(t.timeMs, ((TimeUID) keys[i+acount]).timeMs)==0){
                    acount++;
                }
                DataOutput2.packInt(out, acount);
            }
            bSerializer.serialize(out,t.uid);

            acount--;
        }
    }

    @Override
    public Object[] deserialize(DataInput in, int start, int end, int size) throws IOException {
        Object[] ret = new Object[size];
        Long a = null;
        int acount = 0;

        for(int i=start;i<end;i++){
            if(acount==0){
                //read new A
                a = aSerializer.deserialize(in,-1);
                acount = DataInput2.unpackInt(in);
            }
            String b = bSerializer.deserialize(in,-1);
            ret[i]= Fun.t2(a, b);
            acount--;
        }
        assert(acount==0);

        return ret;
    }

    @Override
    public Comparator getComparator() {
        return BTreeMap.COMPARABLE_COMPARATOR;
    }

}
