package com.logscape.disco.indexer;

import java.io.Serializable;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 16/01/2014
 * Time: 10:23
 * To change this template use File | Settings | File Templates.
 */
public final class TupleIntInt implements Comparable, Serializable {


    final public int a;
    final public int b;

    public TupleIntInt(int a, int b) {
        this.a = a;
        this.b = b;
    }



    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TupleIntInt tuple2 = (TupleIntInt) o;

        if (a != tuple2.a ) return false;
        if (b != tuple2.b ) return false;

        return true;
    }

    @Override public int hashCode() {
        int result = a;
        result = 31 * a + b;
        return result;
    }


    @Override public String toString() {
        return "Tuple2[" + a +", "+b+"]";
    }

    @Override public int compareTo(Object o) {
        TupleIntInt other = (TupleIntInt) o;
        if (other.a != a) {
            return compareTo(this.a, other.a);

        } else {
            return compareTo(this.b, other.b);
        }
    }
    public int compareTo(int thisValue, int anotherValue) {
        int thisVal = thisValue;
        int anotherVal = anotherValue;
        return (thisVal<anotherVal ? -1 : (thisVal==anotherVal ? 0 : 1));
    }
}